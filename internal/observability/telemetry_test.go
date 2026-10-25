package observability

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

func TestTelemetrySmokeExportsTraceMetricsAndQueueDepth(t *testing.T) {
	var traceOutput bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	telemetry := SetupTelemetry(context.Background(), "genesisgo-test", "test", "", &traceOutput, logger)
	ctx := context.Background()

	apiCtx, apiSpan := otel.Tracer("test/api").Start(ctx, "api.create_run")
	apiSpanContext := trace.SpanContextFromContext(apiCtx)
	workerCtx := ExtractTraceContext(ctx, InjectTraceContext(apiCtx))
	workerCtx, workerSpan := otel.Tracer("test/worker").Start(workerCtx, "worker.run_job")
	if trace.SpanContextFromContext(workerCtx).TraceID() != apiSpanContext.TraceID() {
		t.Fatal("worker trace did not continue the API trace")
	}
	workerSpan.End()
	apiSpan.End()
	dbCtx := (PGXQueryTracer{}).TraceQueryStart(ctx, nil, pgx.TraceQueryStartData{SQL: "SELECT sensitive_payload FROM events"})
	(PGXQueryTracer{}).TraceQueryEnd(dbCtx, nil, pgx.TraceQueryEndData{})

	metrics := telemetry.Metrics()
	metrics.SetQueueDepthReader(func(context.Context) (int64, error) { return 4, nil })
	handler := telemetry.HTTPMiddleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/v1/runs/run-123/report", nil))
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("instrumentation changed the business response: got %d", response.Code)
	}
	metrics.Retry(ctx)
	metrics.RunStarted(ctx)
	metrics.RunFinished(ctx, "completed", 20*time.Millisecond)
	metrics.WorkerJob("claimed")
	metrics.NATS(ctx, "publish", 3*time.Millisecond, nil)
	metrics.RecordHTTP(ctx, "GET", "/api/v1/scenarios", http.StatusUnauthorized, time.Millisecond)
	metrics.Invariant(ctx, "fail")
	metrics.AI(ctx, 4*time.Millisecond, io.ErrUnexpectedEOF)
	if err := telemetry.Flush(ctx); err != nil {
		t.Fatalf("flush telemetry: %v", err)
	}
	if !strings.Contains(traceOutput.String(), "worker.run_job") {
		t.Fatal("expected the local stdout trace exporter to emit the worker span")
	}
	if !strings.Contains(traceOutput.String(), "db.query") || strings.Contains(traceOutput.String(), "sensitive_payload") {
		t.Fatal("database spans must be present without recording SQL text")
	}

	scrape := httptest.NewRecorder()
	telemetry.MetricsHandler().ServeHTTP(scrape, httptest.NewRequest("GET", "/metrics", nil))
	body := scrape.Body.String()
	for _, name := range []string{
		"genesisgo_http_server_requests_total",
		"genesisgo_http_server_rejected_total",
		"genesisgo_delivery_retries_total",
		"genesisgo_invariants_evaluated_total",
		"genesisgo_ai_requests_total",
		"genesisgo_nats_operations_total",
		"genesisgo_db_query_duration_seconds",
		"genesisgo_runs_completed_total",
		"genesisgo_worker_queue_depth",
	} {
		if !strings.Contains(body, name) {
			t.Errorf("Prometheus scrape is missing %s", name)
		}
	}
	if strings.Contains(body, "run-123") || strings.Contains(body, "event-456") {
		t.Fatal("metrics must not contain run or event identifiers")
	}
	if !strings.Contains(body, `http_route="/api/v1/runs/{id}/report"`) {
		t.Fatal("HTTP metrics did not use the bounded route template")
	}
	if route := metricRoute("/api/v1/runs/run-123/report"); route != "/api/v1/runs/{id}/report" {
		t.Fatalf("run ID leaked into metric route: %q", route)
	}

	if err := telemetry.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown telemetry: %v", err)
	}
}

func TestQueueMetricsFailureDoesNotFailScrapeOrRequest(t *testing.T) {
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	telemetry := SetupTelemetry(context.Background(), "genesisgo-test", "test", "", io.Discard, logger)
	telemetry.Metrics().SetQueueDepthReader(func(context.Context) (int64, error) {
		return 0, io.ErrUnexpectedEOF
	})

	request := httptest.NewRecorder()
	telemetry.HTTPMiddleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusAccepted)
	})).ServeHTTP(request, httptest.NewRequest(http.MethodGet, "/health/live", nil))
	if request.Code != http.StatusAccepted {
		t.Fatalf("telemetry changed the business response: got %d", request.Code)
	}

	scrape := httptest.NewRecorder()
	telemetry.MetricsHandler().ServeHTTP(scrape, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if scrape.Code != http.StatusOK || !strings.Contains(scrape.Body.String(), "genesisgo_metrics_queue_depth_errors_total") {
		t.Fatalf("queue query failure broke metrics scrape: status=%d body=%s", scrape.Code, scrape.Body.String())
	}
	if err := telemetry.Shutdown(context.Background()); err != nil {
		t.Fatalf("shutdown telemetry: %v", err)
	}
}
