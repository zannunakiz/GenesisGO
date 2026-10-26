//go:build integration

// Package integration contains reproducible integration and end-to-end tests
// for GenesisGo. They provision live PostgreSQL and NATS containers with
// Testcontainers and skip when no Docker daemon is available.
package integration

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/zannunakiz/GenesisGO/internal/app"
	"github.com/zannunakiz/GenesisGO/internal/auth"
	"github.com/zannunakiz/GenesisGO/internal/broker"
	"github.com/zannunakiz/GenesisGO/internal/httpapi"
	"github.com/zannunakiz/GenesisGO/internal/invariants"
	"github.com/zannunakiz/GenesisGO/internal/orchestrator"
	"github.com/zannunakiz/GenesisGO/internal/report"
	"github.com/zannunakiz/GenesisGO/internal/sampleconsumer"
	"github.com/zannunakiz/GenesisGO/internal/scenarios"
	"github.com/zannunakiz/GenesisGO/internal/sse"
	"github.com/zannunakiz/GenesisGO/internal/store"
)

const (
	stackTimeout = 3 * time.Minute
	execTimeout  = 60 * time.Second
	sseWait      = 8 * time.Second
	sseRead      = 5 * time.Second
	fixtureDir   = "internal/scenarios/fixtures"
)

// passFixture is a single-idempotent-payment scenario exercised independently
// of the committed fault fixtures.
const passFixture = `{
  "key": "pass_safe",
  "version": 1,
  "name": "Pass Safe",
  "description": "a single idempotent payment that satisfies all invariants",
  "steps": [
    { "id": "publish-1", "type": "publish", "event_id": "evt-pass-1", "payload": { "type": "payment.succeeded", "aggregate_id": "order-1", "amount_cents": 1000, "currency": "USD", "idempotency_key": "pay-1" } },
    { "id": "deliver-1", "type": "deliver", "event_id": "evt-pass-1", "attempt": 1 },
    { "id": "ack-1", "type": "ack", "event_id": "evt-pass-1" }
  ],
  "parameters": { "max_events": 5, "max_retries": 3, "timeout_ms": 15000, "concurrency": 1 },
  "invariants": [
    { "key": "no_duplicate_payment", "type": "no_duplicate_effect", "effect_type": "payment", "aggregate": "order-1" },
    { "key": "idempotency_key_unique", "type": "idempotency_key_unique", "effect_type": "payment" }
  ],
  "seed": 1
}`

// integrationStack provisions PostgreSQL and NATS containers and wires the
// store and broker needed for an integration test. Containers are terminated
// and connections closed in Cleanup, on success and failure alike.
type integrationStack struct {
	t        *testing.T
	ctx      context.Context
	cancel   context.CancelFunc
	pool     *pgxpool.Pool
	store    *store.Store
	broker   broker.Broker
	postgres testcontainers.Container
	nats     testcontainers.Container
}

func (s *integrationStack) Close() {
	if s.broker != nil {
		_ = s.broker.Close()
	}
	if s.store != nil {
		s.store.Close(context.Background())
	}
	if s.nats != nil {
		_ = s.nats.Terminate(context.Background())
	}
	if s.postgres != nil {
		_ = s.postgres.Terminate(context.Background())
	}
	s.cancel()
}

// newIntegrationStack starts PostgreSQL and NATS via Testcontainers, applies
// the committed migrations, and returns a ready stack. It skips the test
// when no Docker daemon is available so the suite stays runnable everywhere.
func newIntegrationStack(t *testing.T) *integrationStack {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), stackTimeout)
	s := &integrationStack{t: t, ctx: ctx, cancel: cancel}
	t.Cleanup(s.Close)

	pgImage := os.Getenv("GG_TEST_POSTGRES_IMAGE")
	if pgImage == "" {
		pgImage = "postgres:15-alpine"
	}
	pg, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        pgImage,
			ExposedPorts: []string{"5432/tcp"},
			Env: map[string]string{
				"POSTGRES_DB":       "genesisgo",
				"POSTGRES_USER":     "genesisgo",
				"POSTGRES_PASSWORD": "genesisgo",
			},
			WaitingFor: wait.ForLog("database system is ready to accept connections"),
		},
	})
	if err != nil {
		t.Skipf("skipping integration test: cannot start PostgreSQL container (is Docker running?): %v", err)
	}
	s.postgres = pg

	natsImage := os.Getenv("GG_TEST_NATS_IMAGE")
	if natsImage == "" {
		natsImage = "nats:2.10-alpine"
	}
	natsC, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        natsImage,
			ExposedPorts: []string{"4222/tcp"},
			WaitingFor:   wait.ForListeningPort("4222/tcp"),
		},
	})
	if err != nil {
		t.Skipf("skipping integration test: cannot start NATS container (is Docker running?): %v", err)
	}
	s.nats = natsC

	pgEndpoint, err := pg.Endpoint(ctx, "5432/tcp")
	if err != nil {
		t.Fatalf("postgres endpoint: %v", err)
	}
	dsn := fmt.Sprintf("postgres://genesisgo:genesisgo@%s/genesisgo?sslmode=disable", pgEndpoint)

	pool, err := store.NewPool(context.Background(), dsn)
	if err != nil {
		t.Fatalf("new database pool: %v", err)
	}
	var pingErr error
	for i := 0; i < 60; i++ {
		if pingErr = pool.Ping(context.Background()); pingErr == nil {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	if pingErr != nil {
		t.Fatalf("postgres not reachable: %v", pingErr)
	}
	s.pool = pool
	s.store = store.NewStore(pool)

	if err := applyMigrations(context.Background(), pool, "migrations"); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}

	natEndpoint, err := natsC.Endpoint(ctx, "4222/tcp")
	if err != nil {
		t.Fatalf("nats endpoint: %v", err)
	}
	s.broker, err = newBrokerReady(context.Background(), "nats://"+natEndpoint)
	if err != nil {
		t.Fatalf("new broker: %v", err)
	}
	return s
}

func newBrokerReady(ctx context.Context, url string) (broker.Broker, error) {
	var brk broker.Broker
	var err error
	for i := 0; i < 20; i++ {
		brk, err = broker.New(broker.Config{
			URL:           url,
			Stream:        "genesisgo.events",
			SubjectPrefix: "gg.run.it",
			MaxDeliver:    5,
			MaxAckPending: 1000,
		})
		if err == nil {
			return brk, nil
		}
		time.Sleep(500 * time.Millisecond)
	}
	return nil, fmt.Errorf("connect to nats: %w", err)
}

func applyMigrations(ctx context.Context, pool *pgxpool.Pool, dir string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".up.sql") {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(dir, entry.Name()))
		if err != nil {
			return err
		}
		for _, chunk := range splitStatements(string(raw)) {
			stmt := strings.TrimSpace(stripCommentLines(chunk))
			if stmt == "" {
				continue
			}
			if _, err := pool.Exec(ctx, stmt); err != nil {
				return fmt.Errorf("apply %s: %w", entry.Name(), err)
			}
		}
	}
	return nil
}

func stripCommentLines(raw string) string {
	var kept []string
	for _, line := range strings.Split(raw, "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "--") {
			continue
		}
		kept = append(kept, line)
	}
	return strings.Join(kept, "\n")
}

func splitStatements(raw string) []string {
	var out []string
	var cur strings.Builder
	for _, line := range strings.Split(raw, "\n") {
		cur.WriteString(line)
		cur.WriteString("\n")
		if strings.HasSuffix(strings.TrimSpace(line), ";") {
			out = append(out, cur.String())
			cur.Reset()
		}
	}
	if rem := strings.TrimSpace(cur.String()); rem != "" {
		out = append(out, rem)
	}
	return out
}

func newOrchestrator(t *testing.T, st *store.Store, brk broker.Broker, fixtureDir string, mode sampleconsumer.Mode) *orchestrator.Orchestrator {
	t.Helper()
	loader := scenarios.NewLoader(fixtureDir)
	if _, err := loader.Load(); err != nil {
		t.Fatalf("load scenarios: %v", err)
	}
	consumer, err := sampleconsumer.New(sampleconsumer.Config{Ledger: st, Mode: mode})
	if err != nil {
		t.Fatalf("new consumer: %v", err)
	}
	eng := invariants.NewEngine()
	for _, r := range invariants.DefaultRules() {
		eng.Register(r)
	}
	o, err := orchestrator.New(orchestrator.Config{
		Store:             st,
		Broker:            brk,
		ScenarioLoader:    loader,
		Invariants:        eng,
		SampleConsumer:    consumer,
		WorkerConcurrency: 1,
		RunTimeout:        execTimeout,
		LeaseTTL:          30 * time.Second,
	})
	if err != nil {
		t.Fatalf("new orchestrator: %v", err)
	}
	return o
}

func executeScenario(t *testing.T, o *orchestrator.Orchestrator, key string) *store.Run {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), execTimeout)
	defer cancel()
	run, err := o.EnqueueScenario(ctx, key)
	if err != nil {
		t.Fatalf("enqueue scenario %q: %v", key, err)
	}
	run, err = o.ExecuteRun(ctx, run.ID)
	if err != nil {
		t.Fatalf("execute run %s: %v", run.ID, err)
	}
	return run
}

func requireStatus(t *testing.T, st *store.Store, runID, want string) {
	t.Helper()
	run, err := st.GetRun(context.Background(), runID)
	if err != nil {
		t.Fatalf("get run %s: %v", runID, err)
	}
	if run.Status != want {
		t.Fatalf("run %s status = %q, want %q", runID, run.Status, want)
	}
}

func requireEffects(t *testing.T, st *store.Store, runID string) []*store.SideEffectRecord {
	t.Helper()
	effects, err := st.ListSideEffectRecordsByRun(context.Background(), runID)
	if err != nil {
		t.Fatalf("list side effects: %v", err)
	}
	return effects
}

func requireVerdict(t *testing.T, st *store.Store, runID, key, want string) {
	t.Helper()
	results, err := st.ListInvariantResultsByRun(context.Background(), runID)
	if err != nil {
		t.Fatalf("list invariant results: %v", err)
	}
	for _, r := range results {
		if r.InvariantKey == key {
			if r.Verdict != want {
				t.Fatalf("invariant %q verdict = %q, want %q", key, r.Verdict, want)
			}
			return
		}
	}
	t.Fatalf("no invariant result for key %q", key)
}

func effectKey(e *store.SideEffectRecord) string {
	if e.IdempotencyKey == nil {
		return ""
	}
	return *e.IdempotencyKey
}

func TestIntegrationNormalDeliverySafeConsumerPasses(t *testing.T) {
	s := newIntegrationStack(t)
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "pass_safe.json"), []byte(passFixture), 0o600); err != nil {
		t.Fatalf("write fixture: %v", err)
	}
	o := newOrchestrator(t, s.store, s.broker, dir, sampleconsumer.ModeSafe)

	run := executeScenario(t, o, "pass_safe")
	requireStatus(t, s.store, run.ID, string(store.RunStateCompleted))

	effects := requireEffects(t, s.store, run.ID)
	if len(effects) != 1 {
		t.Fatalf("expected 1 payment effect, got %d", len(effects))
	}
	if effects[0].EffectType != "payment" || effects[0].AggregateID != "order-1" {
		t.Fatalf("unexpected effect: %+v", effects[0])
	}
	if effectKey(effects[0]) != "pay-1" {
		t.Fatalf("unexpected idempotency key: %+v", effects[0])
	}
	requireVerdict(t, s.store, run.ID, "no_duplicate_payment", "pass")
	requireVerdict(t, s.store, run.ID, "idempotency_key_unique", "pass")
}

func TestIntegrationDuplicateDeliverySafeConsumerDedups(t *testing.T) {
	s := newIntegrationStack(t)
	o := newOrchestrator(t, s.store, s.broker, fixtureDir, sampleconsumer.ModeSafe)

	run := executeScenario(t, o, "duplicate_delivery")
	requireStatus(t, s.store, run.ID, string(store.RunStateCompleted))

	effects := requireEffects(t, s.store, run.ID)
	if len(effects) != 1 {
		t.Fatalf("safe consumer must dedupe duplicates: expected 1 payment effect, got %d", len(effects))
	}
	if effectKey(effects[0]) != "pay-order-100" {
		t.Fatalf("unexpected idempotency key: %+v", effects[0])
	}
	requireVerdict(t, s.store, run.ID, "no_duplicate_payment", "pass")
	requireVerdict(t, s.store, run.ID, "idempotency_key_unique", "pass")
}

func TestIntegrationDuplicateDeliveryUnsafeConsumerFails(t *testing.T) {
	s := newIntegrationStack(t)
	o := newOrchestrator(t, s.store, s.broker, fixtureDir, sampleconsumer.ModeUnsafeDemo)

	run := executeScenario(t, o, "duplicate_delivery")
	requireStatus(t, s.store, run.ID, string(store.RunStateFailed))

	effects := requireEffects(t, s.store, run.ID)
	if len(effects) != 2 {
		t.Fatalf("unsafe consumer must reproduce the duplicate effect: expected 2 payment effects, got %d", len(effects))
	}
	requireVerdict(t, s.store, run.ID, "no_duplicate_payment", "fail")
	requireVerdict(t, s.store, run.ID, "idempotency_key_unique", "fail")
}

func TestIntegrationCrashAfterCommitBeforeAckRecovers(t *testing.T) {
	s := newIntegrationStack(t)
	o := newOrchestrator(t, s.store, s.broker, fixtureDir, sampleconsumer.ModeSafe)

	run := executeScenario(t, o, "crash_after_commit_before_ack")
	requireStatus(t, s.store, run.ID, string(store.RunStateCompleted))

	effects := requireEffects(t, s.store, run.ID)
	if len(effects) != 1 {
		t.Fatalf("crash-after-commit redelivery must not duplicate: expected 1 payment effect, got %d", len(effects))
	}
	requireVerdict(t, s.store, run.ID, "no_duplicate_payment", "pass")
	requireVerdict(t, s.store, run.ID, "idempotency_key_unique", "pass")

	attempts, err := s.store.ListEventAttemptsByRun(context.Background(), run.ID)
	if err != nil {
		t.Fatalf("list event attempts: %v", err)
	}
	if len(attempts) < 2 {
		t.Fatalf("expected at least 2 attempts (crash + redelivery), got %d", len(attempts))
	}
	var crashed bool
	for _, a := range attempts {
		if a.FaultInjected != nil && *a.FaultInjected == "crash_after_commit" && a.AcknowledgedAt == nil {
			crashed = true
		}
	}
	if !crashed {
		t.Fatal("expected an attempt recorded with crash_after_commit and no acknowledgement")
	}
}

func TestIntegrationCancellationProtectsTerminalStatus(t *testing.T) {
	s := newIntegrationStack(t)
	o := newOrchestrator(t, s.store, s.broker, fixtureDir, sampleconsumer.ModeSafe)
	ctx := context.Background()

	scenario, err := s.store.CreateScenario(ctx, "duplicate_delivery", "Duplicate Delivery", "fixture")
	if err != nil {
		t.Fatalf("create scenario: %v", err)
	}
	run, err := s.store.CreateRun(ctx, scenario.ID, string(store.RunStatePending))
	if err != nil {
		t.Fatalf("create run: %v", err)
	}
	if _, err := s.store.TransitionRun(ctx, store.TransitionInput{
		RunID:           run.ID,
		From:            store.RunStatePending,
		To:              store.RunStateRunning,
		Reason:          "worker started",
		ExpectedVersion: run.Version,
	}); err != nil {
		t.Fatalf("transition to running: %v", err)
	}

	cancelled, err := o.CancelRun(ctx, run.ID)
	if err != nil {
		t.Fatalf("cancel run: %v", err)
	}
	requireStatus(t, s.store, run.ID, string(store.RunStateCancelled))
	if cancelled.CancellationReason == nil || *cancelled.CancellationReason == "" {
		t.Fatal("expected cancellation reason to be set")
	}

	// A cancelled run is terminal: executing it again must be rejected.
	if _, err := o.ExecuteRun(ctx, run.ID); !errors.Is(err, orchestrator.ErrRunAlreadyTerminal) {
		t.Fatalf("expected ErrRunAlreadyTerminal, got %v", err)
	}
	// A second cancel must be rejected too.
	if _, err := o.CancelRun(ctx, run.ID); err == nil {
		t.Fatal("expected second cancel of a terminal run to fail")
	}
}

func TestIntegrationSimultaneousIndependentRuns(t *testing.T) {
	s := newIntegrationStack(t)
	o := newOrchestrator(t, s.store, s.broker, fixtureDir, sampleconsumer.ModeSafe)

	var wg sync.WaitGroup
	var runA, runB *store.Run
	wg.Add(2)
	go func() {
		defer wg.Done()
		runA = executeScenario(t, o, "duplicate_delivery")
	}()
	go func() {
		defer wg.Done()
		runB = executeScenario(t, o, "crash_after_commit_before_ack")
	}()
	wg.Wait()
	if runA == nil || runB == nil {
		t.Fatal("expected both runs to execute")
	}
	if runA.ID == runB.ID {
		t.Fatal("expected two distinct runs")
	}

	requireStatus(t, s.store, runA.ID, string(store.RunStateCompleted))
	requireStatus(t, s.store, runB.ID, string(store.RunStateCompleted))

	for id := range map[string]*store.Run{runA.ID: runA, runB.ID: runB} {
		effects := requireEffects(t, s.store, id)
		if len(effects) != 1 {
			t.Fatalf("run %s: expected 1 effect, got %d", id, len(effects))
		}
		if effects[0].RunID != id {
			t.Fatalf("run %s: effect leaked into wrong run (%q)", id, effects[0].RunID)
		}
		requireVerdict(t, s.store, id, "no_duplicate_payment", "pass")
		requireVerdict(t, s.store, id, "idempotency_key_unique", "pass")
	}
}

type testReportGen struct {
	store *store.Store
}

func (g *testReportGen) GenerateReport(ctx context.Context, runID string) (*report.Report, error) {
	return report.GenerateReport(ctx, g.store, runID)
}

func TestIntegrationRESTAPIAndSSEStream(t *testing.T) {
	s := newIntegrationStack(t)
	o := newOrchestrator(t, s.store, s.broker, fixtureDir, sampleconsumer.ModeSafe)

	generated, err := auth.Generate("integration-test-key", []string{auth.ScopeRead, auth.ScopeRun})
	if err != nil {
		t.Fatalf("generate api key: %v", err)
	}
	authStore, err := auth.NewMemoryStore(generated.Record)
	if err != nil {
		t.Fatalf("create auth store: %v", err)
	}
	uc := app.NewUseCases(
		&scenarioList{loader: scenarios.NewLoader(fixtureDir)},
		s.store, o, o, &testReportGen{store: s.store},
	)
	uc.WithAuditLogger(s.store)
	hub := sse.NewHub(100)

	mux := http.NewServeMux()
	httpapi.Register(mux, uc, authStore, hub)
	handler := httpapi.Timeout(httpapi.DefaultTimeouts().HandlerTimeoutOrDefault())(
		httpapi.Recover(httpapi.RequestID(mux)))
	srv := httptest.NewServer(handler)
	defer srv.Close()

	client := &http.Client{Timeout: 30 * time.Second}
	authHeader := func(req *http.Request) {
		req.Header.Set("Authorization", "Bearer "+generated.Secret)
	}

	req := httptest.NewRequest(http.MethodPost, srv.URL+"/api/v1/runs",
		strings.NewReader(`{"scenario_key":"duplicate_delivery"}`))
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("unauthenticated create: %v", err)
	}
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401 without credentials, got %d", resp.StatusCode)
	}
	resp.Body.Close()

	req = httptest.NewRequest(http.MethodPost, srv.URL+"/api/v1/runs",
		strings.NewReader(`{"scenario_key":"duplicate_delivery"}`))
	req.Header.Set("Content-Type", "application/json")
	authHeader(req)
	resp, err = client.Do(req)
	if err != nil {
		t.Fatalf("create run: %v", err)
	}
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201 creating a run, got %d", resp.StatusCode)
	}
	var created struct {
		Run *store.Run `json:"run"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&created); err != nil {
		resp.Body.Close()
		t.Fatalf("decode create response: %v", err)
	}
	resp.Body.Close()
	runID := created.Run.ID
	if runID == "" {
		t.Fatal("created run missing id")
	}

	ctx, cancel := context.WithTimeout(context.Background(), execTimeout)
	defer cancel()
	if _, err := o.ExecuteRun(ctx, runID); err != nil {
		t.Fatalf("execute run: %v", err)
	}

	req = httptest.NewRequest(http.MethodGet, srv.URL+"/api/v1/runs/"+runID, nil)
	authHeader(req)
	resp, err = client.Do(req)
	if err != nil {
		t.Fatalf("get run: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 getting run, got %d", resp.StatusCode)
	}
	var gotRun struct {
		Run *store.Run `json:"run"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&gotRun); err != nil {
		resp.Body.Close()
		t.Fatalf("decode run: %v", err)
	}
	resp.Body.Close()
	requireStatus(t, s.store, runID, string(store.RunStateCompleted))
	if gotRun.Run.ID != runID {
		t.Fatalf("run id mismatch: %q != %q", gotRun.Run.ID, runID)
	}

	req = httptest.NewRequest(http.MethodGet, srv.URL+"/api/v1/runs/"+runID+"/report", nil)
	authHeader(req)
	resp, err = client.Do(req)
	if err != nil {
		t.Fatalf("get report: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 getting report, got %d", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.Contains(ct, "application/json") {
		t.Fatalf("expected application/json content type, got %q", ct)
	}
	var rep report.Report
	if err := json.NewDecoder(resp.Body).Decode(&rep); err != nil {
		resp.Body.Close()
		t.Fatalf("decode report: %v", err)
	}
	resp.Body.Close()
	if rep.Run.ID != runID {
		t.Fatalf("report run id mismatch: %q != %q", rep.Run.ID, runID)
	}

	req = httptest.NewRequest(http.MethodGet,
		srv.URL+"/api/v1/runs/00000000-0000-0000-0000-000000000000/report", nil)
	authHeader(req)
	resp, err = client.Do(req)
	if err != nil {
		t.Fatalf("get missing report: %v", err)
	}
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404 for missing report, got %d", resp.StatusCode)
	}
	resp.Body.Close()

	type sseResult struct {
		resp *http.Response
		err  error
	}
	sseReq := httptest.NewRequest(http.MethodGet, srv.URL+"/api/v1/runs/"+runID+"/events", nil)
	authHeader(sseReq)
	delivered := make(chan sseResult, 1)
	go func() {
		r, e := client.Do(sseReq)
		delivered <- sseResult{resp: r, err: e}
	}()
	deadline := time.Now().Add(sseWait)
	var respSSE *http.Response
	for time.Now().Before(deadline) {
		hub.Publish(runID, sse.Event{Type: sse.EventRunStarted, RunID: runID,
			Data: map[string]string{"note": "sse stream"}})
		select {
		case r := <-delivered:
			respSSE = r.resp
			if r.err != nil {
				t.Fatalf("sse request: %v", r.err)
			}
			goto gotSSE
		default:
			time.Sleep(50 * time.Millisecond)
		}
	}
	t.Fatal("sse stream did not respond")
gotSSE:
	defer respSSE.Body.Close()
	if ct := respSSE.Header.Get("Content-Type"); !strings.Contains(ct, "text/event-stream") {
		t.Fatalf("expected text/event-stream content type, got %q", ct)
	}
	reader := bufio.NewReader(respSSE.Body)
	readDeadline := time.Now().Add(sseRead)
	var gotData []byte
	for time.Now().Before(readDeadline) {
		line, err := reader.ReadString('\n')
		if err != nil {
			break
		}
		line = strings.TrimRight(line, "\r\n")
		if strings.HasPrefix(line, "data: ") {
			gotData = []byte(strings.TrimPrefix(line, "data: "))
			break
		}
	}
	if len(gotData) == 0 {
		t.Fatal("expected at least one SSE data event")
	}
	var ev sse.Event
	if err := json.Unmarshal(gotData, &ev); err != nil {
		t.Fatalf("decode SSE event: %v", err)
	}
	if ev.Type != sse.EventRunStarted || ev.RunID != runID {
		t.Fatalf("unexpected SSE event: %+v", ev)
	}
}

type scenarioList struct {
	loader *scenarios.Loader
}

func (l *scenarioList) Load() (map[string]*scenarios.Scenario, error) {
	return l.loader.Load()
}
