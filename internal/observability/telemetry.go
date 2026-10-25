package observability

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	clientprom "github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	otelprom "go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

type Telemetry struct {
	traces         *sdktrace.TracerProvider
	meters         *sdkmetric.MeterProvider
	metrics        *Metrics
	metricsHandler http.Handler
}

type Metrics struct {
	httpRequests    metric.Int64Counter
	httpRejected    metric.Int64Counter
	httpDuration    metric.Float64Histogram
	runsActive      metric.Int64UpDownCounter
	runOutcomes     metric.Int64Counter
	runDuration     metric.Float64Histogram
	workerJobs      metric.Int64Counter
	deliveryRetries metric.Int64Counter
	invariants      metric.Int64Counter
	aiRequests      metric.Int64Counter
	aiDuration      metric.Float64Histogram
	natsOperations  metric.Int64Counter
	natsDuration    metric.Float64Histogram
	dbDuration      metric.Float64Histogram
	queueDepth      metric.Int64ObservableGauge
	queueErrors     metric.Int64Counter
	queueReaderMu   sync.RWMutex
	queueReader     func(context.Context) (int64, error)
}

var processMetrics atomic.Pointer[Metrics]

// SetupTelemetry configures local Prometheus metrics and optional OTLP traces.
// Development and test processes emit spans as JSON to output when no OTLP
// endpoint is configured. Exporter failures are reported without stopping the
// business process.
func SetupTelemetry(ctx context.Context, serviceName, environment, endpoint string, output io.Writer, logger *slog.Logger) *Telemetry {
	if logger == nil {
		logger = slog.Default()
	}
	if output == nil {
		output = io.Discard
	}

	registry := clientprom.NewRegistry()
	promExporter, promErr := otelprom.New(
		otelprom.WithRegisterer(registry),
		otelprom.WithoutTargetInfo(),
	)
	if promErr != nil {
		logger.Warn("Prometheus exporter unavailable; continuing without metrics export")
	}

	meterOptions := []sdkmetric.Option{
		sdkmetric.WithCardinalityLimit(100),
		sdkmetric.WithResource(resource.NewSchemaless(attribute.String("service.name", serviceName))),
	}
	if promErr == nil {
		meterOptions = append(meterOptions, sdkmetric.WithReader(promExporter))
	}
	meterProvider := sdkmetric.NewMeterProvider(meterOptions...)
	metrics, metricErr := newMetrics(meterProvider.Meter("github.com/zannunakiz/GenesisGO"))
	if metricErr != nil {
		logger.Warn("metric instruments unavailable; continuing without metric recording")
		metrics = &Metrics{}
	}
	processMetrics.Store(metrics)
	otel.SetMeterProvider(meterProvider)

	var exporter sdktrace.SpanExporter
	if endpoint != "" {
		options := []otlptracehttp.Option{otlptracehttp.WithEndpointURL(traceEndpoint(endpoint))}
		if parsed, parseErr := url.Parse(endpoint); parseErr == nil && parsed.Scheme == "http" {
			options = append(options, otlptracehttp.WithInsecure())
		}
		var traceErr error
		exporter, traceErr = otlptracehttp.New(ctx, options...)
		if traceErr != nil {
			logger.Warn("OTLP trace exporter unavailable; continuing without remote trace export")
			exporter = nil
		}
	} else if environment != "production" {
		var traceErr error
		exporter, traceErr = stdouttrace.New(stdouttrace.WithWriter(output))
		if traceErr != nil {
			logger.Warn("stdout trace exporter unavailable; continuing without trace export")
			exporter = nil
		}
	}

	traceOptions := []sdktrace.TracerProviderOption{
		sdktrace.WithResource(resource.NewSchemaless(attribute.String("service.name", serviceName))),
	}
	if exporter != nil {
		traceOptions = append(traceOptions, sdktrace.WithBatcher(
			exporter,
			sdktrace.WithBatchTimeout(time.Second),
			sdktrace.WithExportTimeout(5*time.Second),
		))
	}
	traceProvider := sdktrace.NewTracerProvider(traceOptions...)
	otel.SetTracerProvider(traceProvider)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	otel.SetErrorHandler(otel.ErrorHandlerFunc(func(error) {
		logger.Warn("telemetry export failed")
	}))

	handler := http.NotFoundHandler()
	if promErr == nil && promExporter != nil {
		handler = promhttp.HandlerFor(registry, promhttp.HandlerOpts{})
	}
	return &Telemetry{
		traces:         traceProvider,
		meters:         meterProvider,
		metrics:        metrics,
		metricsHandler: handler,
	}
}

func traceEndpoint(endpoint string) string {
	parsed, err := url.Parse(endpoint)
	if err != nil {
		return endpoint
	}
	if !strings.HasSuffix(strings.TrimRight(parsed.Path, "/"), "/v1/traces") {
		parsed.Path = strings.TrimRight(parsed.Path, "/") + "/v1/traces"
	}
	return parsed.String()
}

func newMetrics(meter metric.Meter) (*Metrics, error) {
	metrics := &Metrics{}
	var err error
	if metrics.httpRequests, err = meter.Int64Counter("genesisgo.http.server.requests"); err != nil {
		return nil, err
	}
	if metrics.httpRejected, err = meter.Int64Counter("genesisgo.http.server.rejected"); err != nil {
		return nil, err
	}
	if metrics.httpDuration, err = meter.Float64Histogram("genesisgo.http.server.duration", metric.WithUnit("s")); err != nil {
		return nil, err
	}
	if metrics.runsActive, err = meter.Int64UpDownCounter("genesisgo.runs.active"); err != nil {
		return nil, err
	}
	if metrics.runOutcomes, err = meter.Int64Counter("genesisgo.runs.completed"); err != nil {
		return nil, err
	}
	if metrics.runDuration, err = meter.Float64Histogram("genesisgo.runs.duration", metric.WithUnit("s")); err != nil {
		return nil, err
	}
	if metrics.workerJobs, err = meter.Int64Counter("genesisgo.worker.jobs"); err != nil {
		return nil, err
	}
	if metrics.deliveryRetries, err = meter.Int64Counter("genesisgo.delivery.retries"); err != nil {
		return nil, err
	}
	if metrics.invariants, err = meter.Int64Counter("genesisgo.invariants.evaluated"); err != nil {
		return nil, err
	}
	if metrics.aiRequests, err = meter.Int64Counter("genesisgo.ai.requests"); err != nil {
		return nil, err
	}
	if metrics.aiDuration, err = meter.Float64Histogram("genesisgo.ai.duration", metric.WithUnit("s")); err != nil {
		return nil, err
	}
	if metrics.natsOperations, err = meter.Int64Counter("genesisgo.nats.operations"); err != nil {
		return nil, err
	}
	if metrics.natsDuration, err = meter.Float64Histogram("genesisgo.nats.duration", metric.WithUnit("s")); err != nil {
		return nil, err
	}
	if metrics.dbDuration, err = meter.Float64Histogram("genesisgo.db.query.duration", metric.WithUnit("s")); err != nil {
		return nil, err
	}
	if metrics.queueDepth, err = meter.Int64ObservableGauge("genesisgo.worker.queue.depth"); err != nil {
		return nil, err
	}
	if metrics.queueErrors, err = meter.Int64Counter("genesisgo.metrics.queue_depth_errors"); err != nil {
		return nil, err
	}
	_, err = meter.RegisterCallback(func(ctx context.Context, observer metric.Observer) error {
		metrics.queueReaderMu.RLock()
		reader := metrics.queueReader
		metrics.queueReaderMu.RUnlock()
		if reader == nil {
			return nil
		}
		depth, err := reader(ctx)
		if err != nil {
			metrics.queueErrors.Add(ctx, 1)
			return nil
		}
		observer.ObserveInt64(metrics.queueDepth, depth)
		return nil
	}, metrics.queueDepth)
	return metrics, err
}

// Metrics returns the instruments for the active process, if initialized.
func MetricsForProcess() *Metrics { return processMetrics.Load() }

func (t *Telemetry) Metrics() *Metrics {
	if t == nil {
		return nil
	}
	return t.metrics
}

// SetQueueDepthReader adds a bounded queue-depth query used only when metrics
// are scraped. The callback must not return identifiers or payload data.
func (m *Metrics) SetQueueDepthReader(reader func(context.Context) (int64, error)) {
	if m == nil {
		return
	}
	m.queueReaderMu.Lock()
	m.queueReader = reader
	m.queueReaderMu.Unlock()
}

func (m *Metrics) RecordHTTP(ctx context.Context, method, route string, status int, duration time.Duration) {
	if m == nil || m.httpRequests == nil {
		return
	}
	statusClass := "other"
	switch {
	case status >= 200 && status < 300:
		statusClass = "2xx"
	case status >= 300 && status < 400:
		statusClass = "3xx"
	case status >= 400 && status < 500:
		statusClass = "4xx"
	case status >= 500:
		statusClass = "5xx"
	}
	attrs := metric.WithAttributes(
		attribute.String("http.request.method", method),
		attribute.String("http.route", route),
		attribute.String("http.response.status_class", statusClass),
	)
	m.httpRequests.Add(ctx, 1, attrs)
	m.httpDuration.Record(ctx, duration.Seconds(), attrs)
	if status >= 400 && status < 500 {
		m.httpRejected.Add(ctx, 1, attrs)
	}
}

func (m *Metrics) RunStarted(ctx context.Context) {
	if m != nil && m.runsActive != nil {
		m.runsActive.Add(ctx, 1)
	}
}

func (m *Metrics) RunFinished(ctx context.Context, status string, duration time.Duration) {
	if m == nil || m.runOutcomes == nil {
		return
	}
	status = boundedRunStatus(status)
	attrs := metric.WithAttributes(attribute.String("run.status", status))
	m.runsActive.Add(ctx, -1)
	m.runOutcomes.Add(ctx, 1, attrs)
	m.runDuration.Record(ctx, duration.Seconds(), attrs)
}

func (m *Metrics) WorkerJob(result string) {
	if m == nil || m.workerJobs == nil {
		return
	}
	if result != "claimed" && result != "empty" && result != "error" {
		result = "other"
	}
	m.workerJobs.Add(context.Background(), 1, metric.WithAttributes(attribute.String("result", result)))
}

func (m *Metrics) Retry(ctx context.Context) {
	if m != nil && m.deliveryRetries != nil {
		m.deliveryRetries.Add(ctx, 1)
	}
}

func (m *Metrics) Invariant(ctx context.Context, verdict string) {
	if m == nil || m.invariants == nil {
		return
	}
	if verdict != "pass" && verdict != "fail" && verdict != "not_evaluated" {
		verdict = "other"
	}
	m.invariants.Add(ctx, 1, metric.WithAttributes(attribute.String("verdict", verdict)))
}

func (m *Metrics) AI(ctx context.Context, duration time.Duration, err error) {
	if m == nil || m.aiRequests == nil {
		return
	}
	result := "success"
	if err != nil {
		result = "error"
	}
	attrs := metric.WithAttributes(attribute.String("result", result))
	m.aiRequests.Add(ctx, 1, attrs)
	m.aiDuration.Record(ctx, duration.Seconds(), attrs)
}

func (m *Metrics) NATS(ctx context.Context, operation string, duration time.Duration, err error) {
	if m == nil || m.natsOperations == nil {
		return
	}
	operation = boundedNATSOperation(operation)
	result := "success"
	if err != nil {
		result = "error"
	}
	m.natsOperations.Add(ctx, 1, metric.WithAttributes(
		attribute.String("operation", operation), attribute.String("result", result),
	))
	m.natsDuration.Record(ctx, duration.Seconds(), metric.WithAttributes(
		attribute.String("operation", operation), attribute.String("result", result),
	))
}

func (m *Metrics) Database(ctx context.Context, operation string, duration time.Duration, err error) {
	if m == nil || m.dbDuration == nil {
		return
	}
	operation = boundedDBOperation(operation)
	result := "success"
	if err != nil {
		result = "error"
	}
	m.dbDuration.Record(ctx, duration.Seconds(), metric.WithAttributes(
		attribute.String("db.operation", operation), attribute.String("result", result),
	))
}

func boundedRunStatus(status string) string {
	switch status {
	case "pending", "running", "completed", "failed", "cancelled", "timed_out", "error":
		return status
	default:
		return "other"
	}
}

func boundedNATSOperation(operation string) string {
	switch operation {
	case "publish", "subscribe", "consume", "delete_consumer":
		return operation
	default:
		return "other"
	}
}

func boundedDBOperation(operation string) string {
	switch operation {
	case "SELECT", "INSERT", "UPDATE", "DELETE", "WITH":
		return strings.ToLower(operation)
	default:
		return "other"
	}
}

// MetricsHandler exposes the process-local Prometheus scrape endpoint.
func (t *Telemetry) MetricsHandler() http.Handler {
	if t == nil || t.metricsHandler == nil {
		return http.NotFoundHandler()
	}
	return t.metricsHandler
}

// Shutdown flushes and closes telemetry exporters. Errors are returned for
// logging but do not affect application work already completed.
func (t *Telemetry) Shutdown(ctx context.Context) error {
	if t == nil {
		return nil
	}
	var shutdownErr error
	if t.traces != nil {
		if err := t.traces.Shutdown(ctx); err != nil {
			shutdownErr = err
		}
	}
	if t.meters != nil {
		if err := t.meters.Shutdown(ctx); err != nil && shutdownErr == nil {
			shutdownErr = err
		}
	}
	return shutdownErr
}

func (t *Telemetry) Flush(ctx context.Context) error {
	if t == nil {
		return nil
	}
	if t.traces != nil {
		if err := t.traces.ForceFlush(ctx); err != nil {
			return err
		}
	}
	if t.meters != nil {
		return t.meters.ForceFlush(ctx)
	}
	return nil
}

// InjectTraceContext serializes only W3C traceparent/tracestate for a queued job.
func InjectTraceContext(ctx context.Context) map[string]string {
	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	if len(carrier) == 0 {
		return nil
	}
	return map[string]string(carrier)
}

// ExtractTraceContext reconstructs the remote parent for a worker span.
func ExtractTraceContext(ctx context.Context, carrier map[string]string) context.Context {
	if len(carrier) == 0 {
		return ctx
	}
	return otel.GetTextMapPropagator().Extract(ctx, propagation.MapCarrier(carrier))
}

// HTTPMiddleware traces requests and records metrics using a fixed route name.
func (t *Telemetry) HTTPMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		method := boundedHTTPMethod(r.Method)
		route := metricRoute(r.URL.Path)
		ctx, span := otel.Tracer("github.com/zannunakiz/GenesisGO/http").Start(
			r.Context(), method+" "+route,
			trace.WithSpanKind(trace.SpanKindServer),
			trace.WithAttributes(attribute.String("http.request.method", method), attribute.String("http.route", route)),
		)
		started := time.Now()
		writer := &statusWriter{ResponseWriter: w}
		defer func() {
			status := writer.status
			if status == 0 {
				status = http.StatusOK
			}
			span.SetAttributes(attribute.Int("http.response.status_code", status))
			if status >= 500 {
				span.SetStatus(codes.Error, "server error")
			}
			span.End()
			if t != nil {
				if metrics := t.metrics; metrics != nil {
					metrics.RecordHTTP(ctx, method, route, status, time.Since(started))
				}
			}
		}()
		next.ServeHTTP(writer, r.WithContext(ctx))
	})
}

func boundedHTTPMethod(method string) string {
	switch method {
	case http.MethodGet, http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete, http.MethodHead, http.MethodOptions:
		return method
	default:
		return "OTHER"
	}
}

func metricRoute(path string) string {
	switch path {
	case "/health/live", "/health/ready", "/healthz":
		return path
	case "/api/v1/scenarios":
		return path
	case "/api/v1/runs":
		return path
	}
	if !strings.HasPrefix(path, "/api/v1/runs/") {
		return "unmatched"
	}
	parts := strings.Split(strings.Trim(strings.TrimPrefix(path, "/api/v1/runs/"), "/"), "/")
	if len(parts) == 1 && parts[0] != "" {
		return "/api/v1/runs/{id}"
	}
	if len(parts) == 2 && parts[0] != "" {
		switch parts[1] {
		case "cancel", "report", "export", "export.csv", "events":
			return "/api/v1/runs/{id}/" + parts[1]
		}
	}
	return "unmatched"
}

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(status int) {
	if w.status != 0 {
		return
	}
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

func (w *statusWriter) Write(data []byte) (int, error) {
	if w.status == 0 {
		w.WriteHeader(http.StatusOK)
	}
	return w.ResponseWriter.Write(data)
}

func (w *statusWriter) Flush() {
	_ = http.NewResponseController(w.ResponseWriter).Flush()
}

func (w *statusWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

// PGXQueryTracer adds database spans without recording SQL text or arguments.
type PGXQueryTracer struct{}

type querySpanKey struct{}
type querySpanState struct {
	span      trace.Span
	startedAt time.Time
	operation string
}

func (PGXQueryTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	operation := queryOperation(data.SQL)
	ctx, span := otel.Tracer("github.com/jackc/pgx").Start(ctx, "db.query",
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			attribute.String("db.system.name", "postgresql"),
			attribute.String("db.operation.name", strings.ToLower(operation)),
		),
	)
	return context.WithValue(ctx, querySpanKey{}, querySpanState{span: span, startedAt: time.Now(), operation: operation})
}

func (PGXQueryTracer) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryEndData) {
	state, ok := ctx.Value(querySpanKey{}).(querySpanState)
	if !ok {
		return
	}
	if data.Err != nil {
		state.span.SetStatus(codes.Error, "database query failed")
	}
	if metrics := MetricsForProcess(); metrics != nil {
		metrics.Database(ctx, state.operation, time.Since(state.startedAt), data.Err)
	}
	state.span.End()
}

func queryOperation(query string) string {
	for _, field := range strings.Fields(query) {
		field = strings.Trim(field, "();")
		if field != "" {
			return strings.ToUpper(field)
		}
	}
	return "other"
}
