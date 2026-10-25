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
	"github.com/zannunakiz/GenesisGO/internal/events"
	"github.com/zannunakiz/GenesisGO/internal/invariants"
	"github.com/zannunakiz/GenesisGO/internal/observability"
	"github.com/zannunakiz/GenesisGO/internal/sampleconsumer"
	"github.com/zannunakiz/GenesisGO/internal/scenarios"
	"github.com/zannunakiz/GenesisGO/internal/store"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
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
	ScenarioKey  string            `json:"scenario_key"`
	TraceContext map[string]string `json:"trace_context,omitempty"`
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
	CountEventAttemptsByRun(ctx context.Context, runID string) (int, error)
	CancelRun(ctx context.Context, runID string, expectedVersion int32, reason string) (*store.Run, error)
	ListRunJobsByRun(ctx context.Context, runID string) ([]*store.RunJob, error)
	ListInvariantResultsByRun(ctx context.Context, runID string) ([]*store.InvariantResult, error)
	CreateStateSnapshot(ctx context.Context, runID, stepID, eventID, snapshotType, aggregateID string, stateSummary []byte) (*store.StateSnapshot, error)
	ListStateSnapshotsByRun(ctx context.Context, runID string) ([]*store.StateSnapshot, error)
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
	// MaxEventsPerRun caps the total number of events published for a single
	// run. Zero means no limit beyond the config default.
	MaxEventsPerRun int
	// Logger is optional; slog.Default() is used when nil.
	Logger *slog.Logger
}

// Orchestrator schedules and executes run jobs. It is safe for concurrent use.
type Orchestrator struct {
	cfg   Config
	log   *slog.Logger
	owner string // unique owner prefix for leases claimed by StartWorkers

	mu            sync.Mutex
	executing     map[string]*atomic.Bool       // runID -> job in progress (defense in depth)
	cancelSignals map[string]context.CancelFunc // runID -> cancel func for in-flight execution
	workers       sync.WaitGroup
	stop          atomic.Bool
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
	maxEvents := cfg.MaxEventsPerRun
	if maxEvents <= 0 {
		maxEvents = 1_000
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
			MaxEventsPerRun:   maxEvents,
			Logger:            log,
		},
		log:           log,
		owner:         fmt.Sprintf("orchestrator-%d", time.Now().UnixNano()),
		executing:     make(map[string]*atomic.Bool),
		cancelSignals: make(map[string]context.CancelFunc),
	}, nil
}

// EnqueueScenario validates a scenario, ensures it has a store row, then
// creates a run (pending) and a run job (queued) in a single transaction. It
// returns the created run. The run is not executed until a worker claims its
// job via StartWorkers or ExecuteRun.
func (o *Orchestrator) EnqueueScenario(ctx context.Context, scenarioKey string) (*store.Run, error) {
	ctx, span := otel.Tracer("github.com/zannunakiz/GenesisGO/orchestrator").Start(ctx, "run.enqueue")
	defer span.End()
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
		payload, err := json.Marshal(jobPayload{
			ScenarioKey:  scenarioKey,
			TraceContext: observability.InjectTraceContext(ctx),
		})
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

	// Create a cancelable context so CancelRun can signal downstream work.
	execCtx, cancel := context.WithCancel(ctx)
	o.mu.Lock()
	o.cancelSignals[runID] = cancel
	o.mu.Unlock()
	defer func() {
		o.mu.Lock()
		delete(o.cancelSignals, runID)
		o.mu.Unlock()
		cancel()
	}()

	return o.executeJob(execCtx, run)
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
			if metrics := observability.MetricsForProcess(); metrics != nil &&
				!errors.Is(err, pgx.ErrNoRows) && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
				metrics.WorkerJob("error")
			}
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
		jobCtx := traceContextForJob(ctx, job.Payload)
		jobCtx, jobSpan := otel.Tracer("github.com/zannunakiz/GenesisGO/orchestrator").Start(
			jobCtx, "worker.run_job", trace.WithSpanKind(trace.SpanKindConsumer),
		)
		if metrics := observability.MetricsForProcess(); metrics != nil {
			metrics.WorkerJob("claimed")
		}

		run, err := o.cfg.Store.GetRun(jobCtx, job.RunID)
		if err != nil {
			o.failJob(jobCtx, job.ID, fmt.Sprintf("load run: %v", err))
			jobSpan.SetStatus(codes.Error, "load run failed")
			jobSpan.End()
			continue
		}

		// Heartbeat while executing so an in-progress job is not recovered.
		hctx, hcancel := context.WithCancel(ctx)
		defer hcancel()
		o.workers.Add(1)
		go o.heartbeatLoop(hctx, job.ID, owner, ttlSeconds)

		_, executionErr := o.executeJob(jobCtx, run)

		hcancel()
		if executionErr != nil {
			jobSpan.SetStatus(codes.Error, "run execution failed")
		}
		jobSpan.End()
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
func (o *Orchestrator) executeJob(ctx context.Context, run *store.Run) (result *store.Run, resultErr error) {
	started := time.Now()
	metrics := observability.MetricsForProcess()
	if metrics != nil {
		metrics.RunStarted(ctx)
	}
	ctx, span := otel.Tracer("github.com/zannunakiz/GenesisGO/orchestrator").Start(ctx, "run.execute")
	defer func() {
		status := "error"
		if result != nil {
			status = result.Status
		}
		if resultErr != nil {
			span.SetStatus(codes.Error, "run execution failed")
		}
		span.End()
		if metrics != nil {
			metrics.RunFinished(ctx, status, time.Since(started))
		}
	}()
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

	// Evaluate invariants only when execution succeeded. On execution failure
	// record every declared invariant as not_evaluated so an execution error
	// is never reported as a successful invariant evaluation.
	if execErr != nil {
		if neErr := o.recordNotEvaluated(ctx, run.ID, s); neErr != nil {
			o.log.Warn("record not_evaluated invariants failed", slog.String("run_id", run.ID), slog.String("error", neErr.Error()))
		}
		run, ferr := o.failRun(ctx, run.ID, run.Version, fmt.Errorf("execute scenario: %w", execErr))
		if ferr != nil {
			return nil, ferr
		}
		o.cleanupRun(ctx, run.ID)
		return run, nil
	}

	// Record invariant evidence after successful execution.
	if evalErr := o.evaluateAndRecord(ctx, run.ID, s); evalErr != nil {
		o.log.Warn("evaluate invariants failed", slog.String("run_id", run.ID), slog.String("error", evalErr.Error()))
	}

	allPass, verdictErr := o.allInvariantsPass(ctx, run.ID)
	if verdictErr != nil {
		run, ferr := o.failRun(ctx, run.ID, run.Version, fmt.Errorf("read invariant results: %w", verdictErr))
		if ferr != nil {
			return nil, ferr
		}
		o.cleanupRun(ctx, run.ID)
		return run, nil
	}
	if !allPass {
		run, ferr := o.failRun(ctx, run.ID, run.Version, errors.New("one or more invariants failed"))
		if ferr != nil {
			return nil, ferr
		}
		o.cleanupRun(ctx, run.ID)
		return run, nil
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
	o.cleanupRun(ctx, run.ID)
	return run, nil
}

// cleanupRun releases ephemeral resources for a terminal run. It stops the
// run's NATS durable consumer so no further redeliveries occur. Database
// evidence is intentionally retained for reporting.
func (o *Orchestrator) cleanupRun(ctx context.Context, runID string) {
	if o.cfg.Broker == nil {
		return
	}
	if err := o.cfg.Broker.DeleteConsumer(ctx, runID); err != nil && !errors.Is(err, broker.ErrNotStarted) {
		o.log.Warn("run cleanup failed",
			slog.String("run_id", runID),
			slog.String("error", err.Error()),
		)
	}
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

// CancelRun transitions a running run to cancelled. It is safe to call on a
// run that is pending or already terminal; those cases return a descriptive
// error. Callers must hold no run-specific lock.
func (o *Orchestrator) CancelRun(ctx context.Context, runID string) (*store.Run, error) {
	run, err := o.cfg.Store.GetRun(ctx, runID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrRunNotFound
		}
		return nil, err
	}
	if store.IsTerminal(store.RunState(run.Status)) {
		return nil, fmt.Errorf("%w: run %q is already terminal (%q)", store.ErrInvalidTransition, runID, run.Status)
	}
	expectedVersion := run.Version
	// Attempt the transition; if the version changed concurrently, retry once
	// with the fresh version.
	updated, err := o.cfg.Store.CancelRun(ctx, runID, expectedVersion, "cancelled by caller")
	if err == store.ErrConcurrentUpdate {
		run2, gerr := o.cfg.Store.GetRun(ctx, runID)
		if gerr != nil {
			if errors.Is(gerr, pgx.ErrNoRows) {
				return nil, ErrRunNotFound
			}
			return nil, gerr
		}
		updated, err = o.cfg.Store.CancelRun(ctx, runID, run2.Version, "cancelled by caller")
	}
	if err != nil {
		return nil, err
	}
	// Signal downstream work to stop via context cancellation.
	o.mu.Lock()
	if cancel, ok := o.cancelSignals[runID]; ok {
		cancel()
		delete(o.cancelSignals, runID)
	}
	o.mu.Unlock()
	if job, jerr := o.currentJob(ctx, runID); jerr == nil {
		_ = o.cfg.Store.UpdateRunJobStatus(context.Background(), job.ID, "cancelled")
	}
	return updated, nil
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
		broker:          o.cfg.Broker,
		store:           o.cfg.Store,
		sample:          o.cfg.SampleConsumer,
		log:             o.log,
		maxEventsPerRun: o.cfg.MaxEventsPerRun,
		published:       make(map[string][]byte),
	}
}

// evaluateAndRecord builds run evidence from persisted side-effect records,
// evaluates the invariants declared by the scenario, and records one invariant
// result per declared rule. Rules not declared by the scenario are recorded
// as not_evaluated so every rule has a deterministic verdict for the run.
func (o *Orchestrator) evaluateAndRecord(ctx context.Context, runID string, scenario *scenarios.Scenario) error {
	ctx, span := otel.Tracer("github.com/zannunakiz/GenesisGO/invariants").Start(ctx, "invariants.evaluate")
	defer span.End()
	effects, err := o.cfg.Store.ListSideEffectRecordsByRun(ctx, runID)
	if err != nil {
		span.SetStatus(codes.Error, "load invariant evidence failed")
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

	// Only invariants declared by the scenario are evaluated for this run.
	// Map each rule key to the scenario's invariant key so results are
	// recorded under the scenario's key (e.g. "no_duplicate_payment" rather
	// than the rule's "no_duplicate_effect").
	ruleToScenarioKey := make(map[string]string, len(scenario.Invariants))
	for _, inv := range scenario.Invariants {
		ruleToScenarioKey[inv.Type] = inv.Key
	}

	results := o.cfg.Invariants.EvaluateAll(ctx, invariants.Evidence{RunID: runID, Effects: ev})
	for _, r := range results {
		if metrics := observability.MetricsForProcess(); metrics != nil {
			metrics.Invariant(ctx, string(r.Status))
		}
		scenarioKey, ok := ruleToScenarioKey[r.InvariantKey]
		if !ok {
			// Rule not declared by this scenario: record as not_evaluated
			// instead of skipping so every rule has a deterministic verdict.
			if err := o.recordNotEvaluatedResult(ctx, runID, r.InvariantKey, r.Rule); err != nil {
				return err
			}
			continue
		}
		details, merr := json.Marshal(map[string]any{
			"rule":     r.Rule,
			"expected": r.Expected,
			"actual":   r.Actual,
			"evidence": r.Evidence,
		})
		if merr != nil {
			return merr
		}
		if _, err := o.cfg.Store.CreateInvariantResult(ctx, runID, scenarioKey, string(r.Status), details); err != nil {
			return err
		}
	}
	return nil
}

// recordNotEvaluated records every invariant declared by the scenario as
// not_evaluated. Used when execution failed before invariant evaluation
// could run, so an execution error is never reported as a successful
// invariant evaluation.
func (o *Orchestrator) recordNotEvaluated(ctx context.Context, runID string, scenario *scenarios.Scenario) error {
	for _, inv := range scenario.Invariants {
		if err := o.recordNotEvaluatedResult(ctx, runID, inv.Key, inv.Type); err != nil {
			return err
		}
	}
	return nil
}

// recordNotEvaluatedResult records a single not_evaluated invariant result.
func (o *Orchestrator) recordNotEvaluatedResult(ctx context.Context, runID, invariantKey, rule string) error {
	details, merr := json.Marshal(map[string]any{
		"rule":     rule,
		"expected": "rule evaluated against run evidence",
		"actual":   "not evaluated",
		"evidence": []string{},
	})
	if merr != nil {
		return merr
	}
	if metrics := observability.MetricsForProcess(); metrics != nil {
		metrics.Invariant(ctx, string(invariants.StatusNotEvaluated))
	}
	_, err := o.cfg.Store.CreateInvariantResult(ctx, runID, invariantKey, string(invariants.StatusNotEvaluated), details)
	return err
}

// allInvariantsPass reports whether every recorded invariant result passed.
// A run with no recorded results does not pass. A not_evaluated verdict does
// not block a run from passing; only a fail verdict does.
func (o *Orchestrator) allInvariantsPass(ctx context.Context, runID string) (bool, error) {
	results, err := o.cfg.Store.ListInvariantResultsByRun(ctx, runID)
	if err != nil {
		return false, err
	}
	if len(results) == 0 {
		return false, nil
	}
	for _, r := range results {
		if r.Verdict == string(invariants.StatusFail) {
			return false, nil
		}
	}
	return true, nil
}

// noopExecutor is a placeholder executor used when no broker is configured. It
// completes the plan immediately with no effects.
type noopExecutor struct{}

func (noopExecutor) Execute(context.Context, string, *scenarios.Scenario) error { return nil }

// defaultExecutor runs scenario steps against the NATS JetStream broker and
// the sample consumer. It is used when no custom executor is injected.
//
// Delivery is plan-driven: publish steps stage an event on the run's subject
// and cache its payload; deliver steps apply the event to the sample consumer
// in plan order and record every attempt. This makes duplicate and
// out-of-order delivery reproducible from the scenario plan instead of
// transport timing.
type defaultExecutor struct {
	broker          broker.Broker
	store           RunStore
	sample          *sampleconsumer.Consumer
	log             *slog.Logger
	mu              sync.Mutex
	fault           *sampleconsumer.Fault // one-shot fault applied to the next matching delivery
	published       map[string][]byte     // event_id -> payload staged by publish steps
	maxEventsPerRun int                   // cap on total events published for this run
	publishedCount  int                   // running count of publish steps executed this run
}

// Execute runs every step of scenario for runID.
func (e *defaultExecutor) Execute(ctx context.Context, runID string, scenario *scenarios.Scenario) error {
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
	case "delay":
		if step.DelayMs > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Duration(step.DelayMs) * time.Millisecond):
			}
		}
	case "crash":
		// Inject a one-shot failure for the next matching delivery. The fault
		// type is taken from the step's fault field (default "crash") so a
		// crash step can declare e.g. crash_after_commit. The fault is applied
		// to the next matching delivery by deliverEvent.
		faultType := step.Fault
		if faultType == "" {
			faultType = "crash"
		}
		e.mu.Lock()
		e.fault = &sampleconsumer.Fault{Type: faultType, EventID: step.EventID}
		e.mu.Unlock()
	case "restart":
		e.mu.Lock()
		e.fault = nil
		e.mu.Unlock()
	case "publish":
		// Enforce MaxEventsPerRun before publishing to prevent a run from
		// exhausting the host with unbounded event generation.
		if e.maxEventsPerRun > 0 && e.publishedCount >= e.maxEventsPerRun {
			return fmt.Errorf("max events per run (%d) exceeded", e.maxEventsPerRun)
		}
		// Stage the event on the run's subject and cache its payload so
		// deliver steps can replay the original event ID in plan order.
		payload, merr := json.Marshal(step.Payload)
		if merr != nil {
			return merr
		}
		if err := e.broker.Publish(ctx, runID, payload); err != nil {
			return fmt.Errorf("publish: %w", err)
		}
		e.mu.Lock()
		e.published[step.EventID] = append([]byte{}, payload...)
		e.publishedCount++
		e.mu.Unlock()
	case "deliver":
		// Apply the event to the sample consumer now, in plan order. This is
		// what makes duplicate and out-of-order delivery deterministic: the
		// scenario plan, not the transport, decides which event is delivered
		// when and how many times.
		if err := e.deliverEvent(ctx, runID, step); err != nil {
			return err
		}
	case "ack":
		// A successful deliver already acknowledged the message. An ack step
		// without a preceding deliver is a plan no-op.
	default:
		return fmt.Errorf("unknown step type %q", step.Type)
	}
	return nil
}

// deliverEvent applies one planned delivery to the sample consumer. The
// payload is the one staged by the matching publish step, so the original
// event ID travels with every attempt of the event. Every attempt is recorded
// durably with its number and any scenario-injected fault.
//
// Scenario-controlled faults declared on the step (or injected by a preceding
// "crash" step via e.fault) are expected plan events, not random failures:
// they are recorded as attempts and execution continues so the scenario can
// demonstrate redelivery, restart, and crash recovery deterministically.
func (e *defaultExecutor) deliverEvent(ctx context.Context, runID string, step scenarios.Step) error {
	// Determine the active fault for this delivery. A fault declared directly
	// on the deliver step takes precedence over a one-shot executor fault set
	// by a preceding "crash" step.
	e.mu.Lock()
	f := e.fault
	if f != nil && (f.EventID == "" || f.EventID == step.EventID) {
		e.fault = nil // one-shot fault
	}
	e.mu.Unlock()

	activeFault := step.Fault
	if activeFault == "" && f != nil {
		activeFault = f.Type
	}

	// Build the envelope from the payload staged by the matching publish step.
	payload, ok := e.published[step.EventID]
	if !ok {
		return fmt.Errorf("deliver step %q references unknown event %q", step.ID, step.EventID)
	}
	var payloadMap map[string]any
	if err := json.Unmarshal(payload, &payloadMap); err != nil {
		return fmt.Errorf("unmarshal payload: %w", err)
	}
	eventType, err := eventTypeFromPayload(payloadMap)
	if err != nil {
		return err
	}
	raw := payload
	env := &events.EventEnvelope{
		EventID:       step.EventID,
		EventType:     eventType,
		SchemaVersion: events.SchemaVersion,
		OccurredAt:    time.Now().UTC(),
		RunID:         runID,
		Payload:       raw,
	}
	if err := events.Validate(env); err != nil {
		return fmt.Errorf("validate envelope: %w", err)
	}
	deliveredAt := time.Now().UTC()

	// Extract the aggregate id from the payload so state snapshots can be
	// scoped to the business aggregate affected by this delivery.
	aggregateID, _ := aggregateIDFromPayload(env.Payload)

	switch activeFault {
	case "retry":
		// Retryable failure BEFORE database commit: no effect is persisted,
		// so a later redelivery can apply the effect exactly once.
		_ = e.captureSnapshot(ctx, runID, step.ID, step.EventID, "before", aggregateID)
		if err := e.recordAttempt(ctx, runID, step, deliveredAt, nil, "retry"); err != nil {
			return fmt.Errorf("record attempt: %w", err)
		}
		return nil // expected retryable failure; the plan continues to a later redelivery
	case "crash_after_commit":
		// Crash AFTER commit but BEFORE ack: the consumer persists the effect
		// (dedup-safe) and then crashes before acknowledging, forcing the
		// broker to redeliver the event whose side effect already persisted.
		_ = e.captureSnapshot(ctx, runID, step.ID, step.EventID, "before", aggregateID)
		if _, perr := e.sample.ProcessEvent(ctx, env); perr != nil && !sampleconsumer.IsCrashAfterCommit(perr) {
			return fmt.Errorf("process event %q: %w", step.EventID, perr)
		}
		_ = e.captureSnapshot(ctx, runID, step.ID, step.EventID, "after", aggregateID)
		if err := e.recordAttempt(ctx, runID, step, deliveredAt, nil, "crash_after_commit"); err != nil {
			return fmt.Errorf("record attempt: %w", err)
		}
		return nil // expected crash after commit; redelivery will follow
	}

	// One-shot crash fault set by a preceding "crash" step: crash before
	// committing any effect.
	if f != nil {
		_ = e.captureSnapshot(ctx, runID, step.ID, step.EventID, "before", aggregateID)
		if err := e.recordAttempt(ctx, runID, step, deliveredAt, nil, f.Type); err != nil {
			return fmt.Errorf("record attempt: %w", err)
		}
		return nil // expected crash; the scenario continues
	}

	// Normal delivery: apply the effect and acknowledge. Any fault declared
	// on the step that is not an execution fault (retry, crash_after_commit)
	// is still recorded on the attempt for timeline observability.
	_ = e.captureSnapshot(ctx, runID, step.ID, step.EventID, "before", aggregateID)
	record, perr := e.sample.ProcessEvent(ctx, env)
	if perr != nil {
		// Processing failed: record the attempt without an acknowledgement
		// and surface the error so the run does not pass on a broken step.
		_ = e.captureSnapshot(ctx, runID, step.ID, step.EventID, "after", aggregateID)
		if err := e.recordAttempt(ctx, runID, step, deliveredAt, nil, step.Fault); err != nil {
			return fmt.Errorf("record failed attempt: %w", err)
		}
		if record != nil {
			_ = record.ID
		}
		return fmt.Errorf("process event %q: %w", step.EventID, perr)
	}
	_ = e.captureSnapshot(ctx, runID, step.ID, step.EventID, "after", aggregateID)
	_ = record
	acknowledgedAt := deliveredAt
	if err := e.recordAttempt(ctx, runID, step, deliveredAt, &acknowledgedAt, step.Fault); err != nil {
		return err
	}
	return nil
}

// recordAttempt persists one delivery attempt for a scenario step. Attempts
// default to 1 when the step carries no attempt number. faultInjected names
// the scenario-induced fault (empty when no fault) so the timeline shows
// which deliveries were anomalies.
func (e *defaultExecutor) recordAttempt(
	ctx context.Context,
	runID string,
	step scenarios.Step,
	deliveredAt time.Time,
	acknowledgedAt *time.Time,
	faultInjected string,
) error {
	attempt := step.Attempt
	if attempt <= 0 {
		attempt = 1
	}
	if attempt > 1 {
		if metrics := observability.MetricsForProcess(); metrics != nil {
			metrics.Retry(ctx)
		}
	}
	var fault *string
	if faultInjected != "" {
		fault = &faultInjected
	}
	if _, err := e.store.CreateEventAttempt(ctx, runID, step.EventID, int32(attempt), deliveredAt, acknowledgedAt, fault); err != nil {
		return fmt.Errorf("record attempt: %w", err)
	}
	return nil
}

func traceContextForJob(ctx context.Context, payload []byte) context.Context {
	var job jobPayload
	if err := json.Unmarshal(payload, &job); err != nil {
		return ctx
	}
	return observability.ExtractTraceContext(ctx, job.TraceContext)
}

// eventTypeFromPayload maps the synthetic business payload type to the
// envelope event type used for transport validation.
func eventTypeFromPayload(payload map[string]any) (events.EventType, error) {
	t, _ := payload["type"].(string)
	switch t {
	case "payment.succeeded":
		return events.EventTypePayment, nil
	case "inventory.adjusted":
		return events.EventTypeInventory, nil
	default:
		return "", fmt.Errorf("unsupported event type %q in payload", t)
	}
}

// aggregateIDFromPayload extracts the aggregate_id from a synthetic event
// payload. Returns empty string if the payload cannot be parsed or has no
// aggregate_id.
func aggregateIDFromPayload(raw []byte) (string, error) {
	var p struct {
		AggregateID string `json:"aggregate_id"`
	}
	if err := json.Unmarshal(raw, &p); err != nil {
		return "", err
	}
	return p.AggregateID, nil
}

// captureSnapshot records a before/after state summary for an event delivery.
// The state_summary contains only safe aggregate-level data (effect count and
// latest effect for the aggregate) — never raw payloads or secrets.
func (e *defaultExecutor) captureSnapshot(
	ctx context.Context,
	runID, stepID, eventID, snapshotType, aggregateID string,
) error {
	effects, err := e.store.ListSideEffectRecordsByRun(ctx, runID)
	if err != nil {
		return err
	}
	var matching []map[string]any
	for _, eff := range effects {
		if eff.AggregateID == aggregateID {
			matching = append(matching, map[string]any{
				"id":              eff.ID,
				"effect_type":     eff.EffectType,
				"aggregate_id":    eff.AggregateID,
				"idempotency_key": eff.IdempotencyKey,
				"amount_cents":    eff.AmountCents,
				"currency":        eff.Currency,
				"recorded_at":     eff.RecordedAt,
			})
		}
	}
	summary := map[string]any{
		"aggregate_id": aggregateID,
		"effect_count": len(matching),
		"effects":      matching,
	}
	raw, merr := json.Marshal(summary)
	if merr != nil {
		return merr
	}
	if _, err := e.store.CreateStateSnapshot(ctx, runID, stepID, eventID, snapshotType, aggregateID, raw); err != nil {
		return fmt.Errorf("create state snapshot: %w", err)
	}
	return nil
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
