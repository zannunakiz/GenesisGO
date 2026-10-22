// Package orchestrator connects persisted run requests to executable scenarios.
//
// It owns the run lifecycle: validate a scenario, create a run and a bounded
// unit of work (run job) transactionally, enqueue and claim work with leases
// and heartbeats, execute the scenario plan, record progress, evaluate
// deterministic invariants, and persist exactly one terminal status per run.
//
// Scenario steps are executed by a pluggable ScenarioExecutor. The default
// executor wires the NATS JetStream broker, the delivery layer, and the sample
// consumer; tests can inject a fake executor so orchestration logic is
// verified without live infrastructure.
package orchestrator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/zannunakiz/GenesisGO/internal/broker"
	"github.com/zannunakiz/GenesisGO/internal/delivery"
	"github.com/zannunakiz/GenesisGO/internal/events"
	"github.com/zannunakiz/GenesisGO/internal/invariants"
	"github.com/zannunakiz/GenesisGO/internal/sampleconsumer"
	"github.com/zannunakiz/GenesisGO/internal/scenarios"
	"github.com/zannunakiz/GenesisGO/internal/store"
)

// Sentinel errors returned by the orchestrator.
var (
	// ErrScenarioNotFound is returned when the requested scenario key has no
	// fixture or store row.
	ErrScenarioNotFound = errors.New("scenario not found")
	// ErrRunNotFound is returned when the target run does not exist.
	ErrRunNotFound = errors.New("run not found")
	// ErrRunAlreadyTerminal is returned when an operation targets a run that
	// already reached a terminal status.
	ErrRunAlreadyTerminal = errors.New("run already terminal")
	// ErrOrchestratorStopped is returned when an operation is attempted after
	// the orchestrator has been stopped.
	ErrOrchestratorStopped = errors.New("orchestrator stopped")
)

// jobPayload is the JSON payload stored on a run job. It identifies which
// scenario the job executes.
type jobPayload struct {
	ScenarioKey string `json:"scenario_key"`
}

// RunStore is the subset of store.Store the orchestrator depends on. Keeping
// it an interface lets tests verify orchestration with in-memory fakes while
// *store.Store satisfies it in production.
type RunStore interface {
	CreateScenario(ctx context.Context, key, name, description string) (*store.Scenario, error)
	GetScenarioByKey(ctx context.Context, key string) (*store.Scenario, error)
	CreateRun(ctx context.Context, scenarioID, status string) (*store.Run, error)
	CreateRunJob(ctx context.Context, runID, jobType, status string, payload []byte) (*store.RunJob, error)
	GetRun(ctx context.Context, id string) (*store.Run, error)
	TransitionRun(ctx context.Context, input store.TransitionInput) (*store.Run, error)
	WithTx(ctx context.Context, fn func(pgx.Tx) error) error
	ClaimRunJob(ctx context.Context, owner string, ttlSeconds int64) (*store.RunJob, error)
	UpdateRunJobStatus(ctx context.Context, jobID, status string) error
	HeartbeatRunJob(ctx context.Context, jobID string, ttlSeconds int64, owner string) error
	RecoverExpiredLeases(ctx context.Context) (int64, error)
	CreateInvariantResult(ctx context.Context, runID, invariantKey, verdict string, details []byte) (*store.InvariantResult, error)
	ListSideEffectRecordsByRun(ctx context.Context, runID string) ([]*store.SideEffectRecord, error)
	CreateEventAttempt(ctx context.Context, runID, eventID string, attemptNumber int32, deliveredAt time.Time, acknowledgedAt *time.Time, faultInjected *string) (*store.EventAttempt, error)
	ListRunJobsByRun(ctx context.Context, runID string) ([]*store.RunJob, error)
	ListInvariantResultsByRun(ctx context.Context, runID string) ([]*store.InvariantResult, error)
}

var _ RunStore = (*store.Store)(nil)

// ScenarioExecutor runs the steps of a scenario for a single run. The
// orchestrator owns the run lifecycle and terminal status; the executor owns
// transport and consumer interaction for the duration of one job.
type ScenarioExecutor interface {
	// Execute runs every step of scenario for runID and returns nil when the
	// plan completed successfully, or an error describing the first failing
	// step. It must respect ctx cancellation and timeout.
	Execute(ctx context.Context, runID string, scenario *scenarios.Scenario) error
}

// Config holds the orchestrator's dependencies and bounds.
type Config struct {
	// Store persists runs, jobs, attempts, effects, and invariant results.
	Store RunStore
	// Broker transports run-scoped events (default executor only).
	Broker broker.Broker
	// ScenarioLoader reads built-in scenario fixtures.
	ScenarioLoader *scenarios.Loader
	// Invariants evaluates run evidence against business rules.
	Invariants *invariants.Engine
	// Executor, when set, runs scenario steps. When nil, a default executor
	// wiring the broker, delivery layer, and sample consumer is used.
	Executor ScenarioExecutor
	// SampleConsumer processes events against the side-effect ledger (default
	// executor only).
	SampleConsumer *sampleconsumer.Consumer
	// WorkerConcurrency bounds how many run jobs execute concurrently.
	WorkerConcurrency int
	// RunTimeout caps how long a single run's execution may take.
	RunTimeout time.Duration
	// LeaseTTL is how long a claimed job's lease lasts before another worker
	// may recover it. Heartbeats renew the lease while a worker runs.
	LeaseTTL time.Duration
	// Logger is optional; slog.Default() is used when nil.
	Logger *slog.Logger
}

// Orchestrator schedules and executes run jobs. It is safe for concurrent use.
type Orchestrator struct {
	cfg   Config
	log   *slog.Logger
	owner string // unique owner prefix for leases claimed by StartWorkers

	mu        sync.Mutex
	executing map[string]*atomic.Bool // runID -> job in progress (defense in depth)
	workers   sync.WaitGroup
	stop      atomic.Bool
}

// pgxErrNoRows is aliased so the package can distinguish a missing row from
// other errors without exposing pgx in its public interface.
var pgxErrNoRows = pgx.ErrNoRows

// New builds an Orchestrator from Config. Zero values are replaced with safe
// defaults: concurrency 1, a 2 minute run timeout, and a 30 second lease TTL.
func New(cfg Config) (*Orchestrator, error) {
	if cfg.Store == nil {
		return nil, errors.New("orchestrator store is required")
	}
	if cfg.ScenarioLoader == nil {
		return nil, errors.New("orchestrator scenario loader is required")
	}
	if cfg.Invariants == nil {
		return nil, errors.New("orchestrator invariants engine is required")
	}
	conc := cfg.WorkerConcurrency
	if conc <= 0 {
		conc = 1
	}
	timeout := cfg.RunTimeout
	if timeout <= 0 {
		timeout = 2 * time.Minute
	}
	lease := cfg.LeaseTTL
	if lease <= 0 {
		lease = 30 * time.Second
	}
	log := cfg.Logger
	if log == nil {
		log = slog.Default()
	}
	return &Orchestrator{
		cfg: Config{
			Store:             cfg.Store,
			Broker:            cfg.Broker,
			ScenarioLoader:    cfg.ScenarioLoader,
			Invariants:        cfg.Invariants,
			Executor:          cfg.Executor,
			SampleConsumer:    cfg.SampleConsumer,
			WorkerConcurrency: conc,
			RunTimeout:        timeout,
			LeaseTTL:          lease,
			Logger:            log,
		},
		log:       log,
		owner:     fmt.Sprintf("orchestrator-%d", time.Now().UnixNano()),
		executing: make(map[string]*atomic.Bool),
	}, nil
}

// EnqueueScenario validates a scenario, ensures it has a store row, then
// creates a run (pending) and a run job (queued) in a single transaction. It
// returns the created run. The run is not executed until a worker claims its
// job via StartWorkers or ExecuteRun.
func (o *Orchestrator) EnqueueScenario(ctx context.Context, scenarioKey string) (*store.Run, error) {
	fixtures, err := o.cfg.ScenarioLoader.Load()
	if err != nil {
		return nil, fmt.Errorf("load scenarios: %w", err)
	}
	s, ok := fixtures[scenarioKey]
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrScenarioNotFound, scenarioKey)
	}
	if err := scenarios.Validate(*s); err != nil {
		return nil, fmt.Errorf("validate scenario: %w", err)
	}

	var run *store.Run
	err = o.cfg.Store.WithTx(ctx, func(tx pgx.Tx) error {
		scenario, err := o.cfg.Store.GetScenarioByKey(ctx, scenarioKey)
		if err != nil {
			if !errors.Is(err, pgxErrNoRows) {
				return err
			}
			scenario, err = o.cfg.Store.CreateScenario(ctx, s.Key, s.Name, s.Description)
			if err != nil {
				return fmt.Errorf("create scenario: %w", err)
			}
		}
		run, err = o.cfg.Store.CreateRun(ctx, scenario.ID, string(store.RunStatePending))
		if err != nil {
			return fmt.Errorf("create run: %w", err)
		}
		payload, err := json.Marshal(jobPayload{ScenarioKey: scenarioKey})
		if err != nil {
			return fmt.Errorf("marshal job payload: %w", err)
		}
		if _, err := o.cfg.Store.CreateRunJob(ctx, run.ID, "execute_scenario", "queued", payload); err != nil {
			return fmt.Errorf("create run job: %w", err)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return run, nil
}

// ExecuteRun runs a single run synchronously: it transitions the run to
// running, executes the scenario plan, evaluates invariants, and persists the
// terminal status. It is safe to call concurrently for different runs; the
// same run is never executed twice.
func (o *Orchestrator) ExecuteRun(ctx context.Context, runID string) (*store.Run, error) {
	if o.stop.Load() {
		return nil, ErrOrchestratorStopped
	}
	run, err := o.cfg.Store.GetRun(ctx, runID)
	if err != nil {
		if errors.Is(err, pgxErrNoRows) {
			return nil, fmt.Errorf("%w: %s", ErrRunNotFound, runID)
		}
		return nil, err
	}
	if store.IsTerminal(store.RunState(run.Status)) {
		return nil, fmt.Errorf("%w: %s (status %q)", ErrRunAlreadyTerminal, runID, run.Status)
	}

	inProgress := o.getOrCreateRunGuard(runID)
	if !inProgress.CompareAndSwap(false, true) {
		return nil, fmt.Errorf("run %s is already being executed", runID)
	}
	defer inProgress.Store(false)

	return o.executeJob(ctx, run)
}

// StartWorkers spawns n workers that continuously claim queued run jobs and
// execute them until ctx is cancelled or Stop is called. Claims use FOR UPDATE
// SKIP LOCKED plus leases, so concurrent workers never execute the same job
// and jobs whose workers are interrupted are recovered via expired leases.
func (o *Orchestrator) StartWorkers(ctx context.Context, n int) {
	if n <= 0 {
		n = 1
	}
	for i := 0; i < n; i++ {
		o.workers.Add(1)
		go o.workerLoop(ctx, fmt.Sprintf("%s-worker-%d", o.owner, i))
	}
}

// Stop halts worker loops and waits for in-flight jobs to finish. It is
// idempotent.
func (o *Orchestrator) Stop() {
	o.stop.Store(true)
	o.workers.Wait()
}

// workerLoop claims and executes jobs until the context is cancelled.
func (o *Orchestrator) workerLoop(ctx context.Context, owner string) {
	defer o.workers.Done()
	ttlSeconds := int64(o.cfg.LeaseTTL / time.Second)
	if ttlSeconds <= 0 {
		ttlSeconds = 1
	}
	retryDelay := 250 * time.Millisecond

	for {
		if o.stop.Load() {
			return
		}
		job, err := o.cfg.Store.ClaimRunJob(ctx, owner, ttlSeconds)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return
			}
			// No queued job or transient error; back off and retry.
			select {
			case <-ctx.Done():
				return
			case <-time.After(retryDelay):
			}
			continue
		}

		if o.stop.Load() {
			// Release the claim so it can be recovered by a later worker.
			_ = o.cfg.Store.UpdateRunJobStatus(ctx, job.ID, "open")
			return
		}

		run, err := o.cfg.Store.GetRun(ctx, job.RunID)
		if err != nil {
			o.failJob(ctx, job.ID, fmt.Sprintf("load run: %v", err))
			continue
		}

		// Heartbeat while executing so an in-progress job is not recovered.
		hctx, hcancel := context.WithCancel(ctx)
		defer hcancel()
		o.workers.Add(1)
		go o.heartbeatLoop(hctx, job.ID, owner, ttlSeconds)

		o.executeJob(ctx, run)

		hcancel()
	}
}

// heartbeatLoop renews the job's lease every LeaseTTL/2 until cancelled.
func (o *Orchestrator) heartbeatLoop(ctx context.Context, jobID, owner string, ttlSeconds int64) {
	defer o.workers.Done()
	interval := o.cfg.LeaseTTL / 2
	if interval <= 0 {
		interval = time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if o.stop.Load() {
				return
			}
			if err := o.cfg.Store.HeartbeatRunJob(context.Background(), jobID, ttlSeconds, owner); err != nil {
				o.log.Warn("heartbeat failed", slog.String("job_id", jobID), slog.String("error", err.Error()))
			}
		}
	}
}

// executeJob runs one run to a terminal status. The caller must hold the run's
// in-progress guard. It transitions pending -> running, executes the scenario
// plan, records invariant verdicts, and persists completed only when every
// invariant passed; otherwise it persists failed. Terminal transitions use
// optimistic locking, so a run can never be completed twice or leave a
// terminal state (no fake pass).
func (o *Orchestrator) executeJob(ctx context.Context, run *store.Run) (*store.Run, error) {
	scenarioKey, err := o.scenarioKeyFromRun(ctx, run)
	if err != nil {
		return o.failRun(ctx, run.ID, run.Version, fmt.Errorf("resolve scenario: %w", err))
	}

	scenarios, err := o.cfg.ScenarioLoader.Load()
	if err != nil {
		return o.failRun(ctx, run.ID, run.Version, fmt.Errorf("load scenarios: %w", err))
	}
	s, ok := scenarios[scenarioKey]
	if !ok {
		return o.failRun(ctx, run.ID, run.Version, fmt.Errorf("%w: %q", ErrScenarioNotFound, scenarioKey))
	}

	run, err = o.cfg.Store.TransitionRun(ctx, store.TransitionInput{
		RunID:           run.ID,
		From:            store.RunStatePending,
		To:              store.RunStateRunning,
		Reason:          "worker started execution",
		ExpectedVersion: run.Version,
	})
	if err != nil {
		// Another worker or transition already moved this run; do not execute.
		return nil, err
	}

	execCtx, cancel := context.WithTimeout(ctx, o.cfg.RunTimeout)
	defer cancel()

	executor := o.executorFor(run.ID)
	execErr := executor.Execute(execCtx, run.ID, s)

	// Record invariant evidence regardless of outcome so a verdict exists.
	if evalErr := o.evaluateAndRecord(ctx, run.ID, s); evalErr != nil {
		o.log.Warn("evaluate invariants failed", slog.String("run_id", run.ID), slog.String("error", evalErr.Error()))
	}

	if execErr != nil {
		return o.failRun(ctx, run.ID, run.Version, fmt.Errorf("execute scenario: %w", execErr))
	}

	allPass, verdictErr := o.allInvariantsPass(ctx, run.ID)
	if verdictErr != nil {
		return o.failRun(ctx, run.ID, run.Version, fmt.Errorf("read invariant results: %w", verdictErr))
	}
	if !allPass {
		return o.failRun(ctx, run.ID, run.Version, errors.New("one or more invariants failed"))
	}

	run, err = o.cfg.Store.TransitionRun(ctx, store.TransitionInput{
		RunID:           run.ID,
		From:            store.RunStateRunning,
		To:              store.RunStateCompleted,
		Reason:          "execution finished; all invariants passed",
		ExpectedVersion: run.Version,
	})
	if err != nil {
		return nil, err
	}
	if job, jerr := o.currentJob(ctx, run.ID); jerr == nil {
		if err := o.cfg.Store.UpdateRunJobStatus(context.Background(), job.ID, "completed"); err != nil {
			o.log.Warn("mark job completed failed", slog.String("job_id", job.ID), slog.String("error", err.Error()))
		}
	}
	return run, nil
}

// failRun transitions a run from running to failed and marks its job failed.
func (o *Orchestrator) failRun(ctx context.Context, runID string, version int32, reason error) (*store.Run, error) {
	run, err := o.cfg.Store.TransitionRun(ctx, store.TransitionInput{
		RunID:           runID,
		From:            store.RunStateRunning,
		To:              store.RunStateFailed,
		Reason:          reason.Error(),
		ExpectedVersion: version,
	})
	if err != nil {
		return nil, err
	}
	if job, jerr := o.currentJob(ctx, runID); jerr == nil {
		_ = o.cfg.Store.UpdateRunJobStatus(context.Background(), job.ID, "failed")
	}
	return run, nil
}

// failJob marks a run job failed without touching the run status (used when the
// run itself cannot be loaded).
func (o *Orchestrator) failJob(ctx context.Context, jobID, reason string) {
	o.log.Warn("job failed", slog.String("job_id", jobID), slog.String("reason", reason))
	if err := o.cfg.Store.UpdateRunJobStatus(context.Background(), jobID, "failed"); err != nil {
		o.log.Warn("mark job failed failed", slog.String("job_id", jobID), slog.String("error", err.Error()))
	}
}

// getOrCreateRunGuard returns the in-progress guard for a run, creating it on
// first use. This is defense in depth on top of lease-based job claiming.
func (o *Orchestrator) getOrCreateRunGuard(runID string) *atomic.Bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	g, ok := o.executing[runID]
	if !ok {
		g = &atomic.Bool{}
		o.executing[runID] = g
	}
	return g
}

// scenarioKeyFromRun reads the scenario key from the run's first job payload.
func (o *Orchestrator) scenarioKeyFromRun(ctx context.Context, run *store.Run) (string, error) {
	jobs, err := o.cfg.Store.ListRunJobsByRun(ctx, run.ID)
	if err != nil {
		return "", err
	}
	if len(jobs) == 0 {
		return "", fmt.Errorf("no job for run %s", run.ID)
	}
	var p jobPayload
	if err := json.Unmarshal(jobs[0].Payload, &p); err != nil {
		return "", fmt.Errorf("parse job payload: %w", err)
	}
	if p.ScenarioKey == "" {
		return "", fmt.Errorf("job payload missing scenario key")
	}
	return p.ScenarioKey, nil
}

// currentJob returns the run's most relevant job (claimed, then queued, then
// the first job) for status updates.
func (o *Orchestrator) currentJob(ctx context.Context, runID string) (*store.RunJob, error) {
	jobs, err := o.cfg.Store.ListRunJobsByRun(ctx, runID)
	if err != nil {
		return nil, err
	}
	for _, j := range jobs {
		if j.Status == "claimed" || j.Status == "queued" {
			return j, nil
		}
	}
	if len(jobs) > 0 {
		return jobs[0], nil
	}
	return nil, fmt.Errorf("no job for run %s", runID)
}

// executorFor returns the scenario executor: the injected one when configured,
// otherwise the default executor wired to the broker, store, and sample
// consumer. When no broker is configured, a no-op executor is returned.
func (o *Orchestrator) executorFor(_ string) ScenarioExecutor {
	if o.cfg.Executor != nil {
		return o.cfg.Executor
	}
	if o.cfg.Broker == nil {
		return noopExecutor{}
	}
	return &defaultExecutor{
		broker: o.cfg.Broker,
		store:  o.cfg.Store,
		sample: o.cfg.SampleConsumer,
		log:    o.log,
	}
}

// evaluateAndRecord builds run evidence from persisted side-effect records,
// evaluates every invariant, and records one invariant result per rule.
func (o *Orchestrator) evaluateAndRecord(ctx context.Context, runID string, scenario *scenarios.Scenario) error {
	effects, err := o.cfg.Store.ListSideEffectRecordsByRun(ctx, runID)
	if err != nil {
		return err
	}
	ev := make([]invariants.EffectRecord, 0, len(effects))
	for _, e := range effects {
		ev = append(ev, invariants.EffectRecord{
			ID:             e.ID,
			RunID:          e.RunID,
			EffectType:     e.EffectType,
			AggregateID:    e.AggregateID,
			IdempotencyKey: e.IdempotencyKey,
			AmountCents:    e.AmountCents,
			Currency:       e.Currency,
			RecordedAt:     e.RecordedAt,
		})
	}
	results := o.cfg.Invariants.EvaluateAll(ctx, invariants.Evidence{RunID: runID, Effects: ev})
	for _, r := range results {
		details, merr := json.Marshal(map[string]any{
			"rule":     r.Rule,
			"expected": r.Expected,
			"actual":   r.Actual,
			"evidence": r.Evidence,
		})
		if merr != nil {
			return merr
		}
		if _, err := o.cfg.Store.CreateInvariantResult(ctx, runID, r.InvariantKey, string(r.Status), details); err != nil {
			return err
		}
	}
	_ = scenario
	return nil
}

// allInvariantsPass reports whether every recorded invariant result passed.
// A run with no recorded results does not pass.
func (o *Orchestrator) allInvariantsPass(ctx context.Context, runID string) (bool, error) {
	results, err := o.cfg.Store.ListInvariantResultsByRun(ctx, runID)
	if err != nil {
		return false, err
	}
	if len(results) == 0 {
		return false, nil
	}
	for _, r := range results {
		if r.Verdict != "pass" {
			return false, nil
		}
	}
	return true, nil
}

// noopExecutor is a placeholder executor used when no broker is configured. It
// completes the plan immediately with no effects.
type noopExecutor struct{}

func (noopExecutor) Execute(context.Context, string, *scenarios.Scenario) error { return nil }

// defaultExecutor runs scenario steps against the NATS JetStream broker, the
// delivery layer, and the sample consumer. It is used when no custom executor
// is injected.
type defaultExecutor struct {
	broker broker.Broker
	store  RunStore
	sample *sampleconsumer.Consumer
	log    *slog.Logger
	mu     sync.Mutex
	fault  *sampleconsumer.Fault // one-shot fault applied to the next matching delivery
}

// Execute runs every step of scenario for runID.
func (e *defaultExecutor) Execute(ctx context.Context, runID string, scenario *scenarios.Scenario) error {
	// Ensure a delivery consumer is subscribed to this run's subject so
	// published events are delivered to the sample consumer.
	if err := e.subscribe(ctx, runID); err != nil {
		return fmt.Errorf("subscribe: %w", err)
	}

	for _, step := range scenario.Steps {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := e.runStep(ctx, runID, step); err != nil {
			return fmt.Errorf("step %q (%s): %w", step.ID, step.Type, err)
		}
	}

	// Drain: wait for delivered events to be processed into side effects, up
	// to the context deadline. A short quiet period confirms no further
	// effects are arriving (e.g. duplicate deliveries that were deduped).
	if err := e.waitForEffects(ctx, runID); err != nil {
		return fmt.Errorf("drain effects: %w", err)
	}
	return nil
}

func (e *defaultExecutor) runStep(ctx context.Context, runID string, step scenarios.Step) error {
	switch step.Type {
	case "publish":
		payload, merr := json.Marshal(step.Payload)
		if merr != nil {
			return merr
		}
		pub, err := delivery.NewPublisher(delivery.Config{
			Broker:   e.broker,
			Recorder: e.store,
			RunID:    runID,
			Logger:   e.log,
		})
		if err != nil {
			return fmt.Errorf("new publisher: %w", err)
		}
		if err := pub.Publish(ctx, step.EventID, payload); err != nil {
			return fmt.Errorf("publish: %w", err)
		}
	case "delay":
		if step.DelayMs > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Duration(step.DelayMs) * time.Millisecond):
			}
		}
	case "crash":
		// Inject a one-shot failure for the next matching delivery, simulating
		// a consumer worker crashing while processing an event.
		e.mu.Lock()
		e.fault = &sampleconsumer.Fault{Type: "crash", EventID: step.EventID}
		e.mu.Unlock()
	case "restart":
		e.mu.Lock()
		e.fault = nil
		e.mu.Unlock()
		if err := e.subscribe(ctx, runID); err != nil {
			return fmt.Errorf("restart subscribe: %w", err)
		}
	case "deliver", "ack":
		// Delivery and acknowledgement are handled by the subscribed delivery
		// consumer; nothing to do here.
	default:
		return fmt.Errorf("unknown step type %q", step.Type)
	}
	return nil
}

func (e *defaultExecutor) subscribe(ctx context.Context, runID string) error {
	consumer, err := delivery.NewConsumer(delivery.Config{
		Broker:      e.broker,
		Recorder:    e.store,
		RunID:       runID,
		MaxAttempts: 5,
		Logger:      e.log,
	})
	if err != nil {
		return fmt.Errorf("new consumer: %w", err)
	}
	handler := func(ctx context.Context, subject string, env *events.EventEnvelope, attempt int32, ack delivery.Ack) error {
		e.mu.Lock()
		f := e.fault
		if f != nil && (f.EventID == "" || f.EventID == env.EventID) {
			e.fault = nil // one-shot
		}
		e.mu.Unlock()
		if f != nil {
			// Simulated worker crash: return an error without applying the
			// business effect, so the message is negatively acknowledged and
			// may be redelivered after recovery.
			return errors.New("simulated consumer worker crash")
		}
		if _, err := e.sample.ProcessEvent(ctx, env); err != nil {
			return err
		}
		return nil
	}
	return consumer.Consume(ctx, handler)
}

func (e *defaultExecutor) waitForEffects(ctx context.Context, runID string) error {
	interval := 100 * time.Millisecond
	quietPolls := 5
	last := -1
	quiet := 0
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		effects, err := e.store.ListSideEffectRecordsByRun(ctx, runID)
		count := 0
		if err == nil {
			count = len(effects)
		}
		if count == last {
			quiet++
			if quiet >= quietPolls {
				return nil
			}
		} else {
			quiet = 0
			last = count
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(interval):
		}
	}
}
