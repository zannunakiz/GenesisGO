package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/zannunakiz/GenesisGO/internal/app"
	"github.com/zannunakiz/GenesisGO/internal/auth"
	"github.com/zannunakiz/GenesisGO/internal/config"
	"github.com/zannunakiz/GenesisGO/internal/httpapi"
	"github.com/zannunakiz/GenesisGO/internal/invariants"
	"github.com/zannunakiz/GenesisGO/internal/observability"
	"github.com/zannunakiz/GenesisGO/internal/orchestrator"
	"github.com/zannunakiz/GenesisGO/internal/protection"
	"github.com/zannunakiz/GenesisGO/internal/report"
	"github.com/zannunakiz/GenesisGO/internal/sampleconsumer"
	"github.com/zannunakiz/GenesisGO/internal/scenarios"
	"github.com/zannunakiz/GenesisGO/internal/sse"
	"github.com/zannunakiz/GenesisGO/internal/store"
)

func main() {
	if err := run(context.Background()); err != nil {
		fmt.Fprintln(os.Stderr, "GenesisGo API failed to start")
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	level, err := observability.ParseLevel(cfg.LogLevel)
	if err != nil {
		return err
	}
	logger := observability.NewLogger(os.Stdout, level)
	startTime := time.Now()
	telemetry := observability.SetupTelemetry(ctx, "genesisgo-api", cfg.Environment, cfg.TelemetryEndpoint, os.Stdout, logger)

	authStore, err := loadAuthStore(cfg)
	if err != nil {
		return err
	}

	// Database and orchestration wiring for run/scenario endpoints.
	pool, err := store.NewPool(ctx, cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("create database pool: %w", err)
	}
	defer pool.Close()
	st := store.NewStore(pool)
	if metrics := telemetry.Metrics(); metrics != nil {
		metrics.SetQueueDepthReader(st.CountQueuedRunJobs)
	}

	// Record the effective, non-secret configuration as an audit event so
	// startup-time configuration changes are visible in the audit trail.
	if err := st.RecordAuditLog(ctx, "", "config.loaded", "",
		fmt.Sprintf("env=%s listen=%s metrics=%s retention=%s max_runs=%d api_keys=%t arcjet=%t",
			cfg.Environment, cfg.ListenAddress, cfg.MetricsListenAddress,
			cfg.RetentionPeriod, cfg.MaxRuns, cfg.APIKeysEnabled, cfg.ArcjetEnabled)); err != nil {
		logger.Warn("record configuration audit log: " + err.Error())
	}

	loader := scenarios.NewLoader("internal/scenarios/fixtures")
	if _, err := loader.Load(); err != nil {
		return fmt.Errorf("load scenarios: %w", err)
	}

	eng := invariants.NewEngine()
	for _, r := range invariants.DefaultRules() {
		eng.Register(r)
	}

	sample, err := sampleconsumer.New(sampleconsumer.Config{
		Ledger: st,
		Mode:   sampleconsumer.ModeSafe,
	})
	if err != nil {
		return fmt.Errorf("create sample consumer: %w", err)
	}

	o, err := orchestrator.New(orchestrator.Config{
		Store:             st,
		Broker:            nil, // broker not needed for API-only wiring
		ScenarioLoader:    loader,
		Invariants:        eng,
		SampleConsumer:    sample,
		WorkerConcurrency: cfg.WorkerConcurrency,
		RunTimeout:        cfg.RunTimeout,
		LeaseTTL:          30 * time.Second,
		Logger:            logger,
	})
	if err != nil {
		return fmt.Errorf("create orchestrator: %w", err)
	}

	uc := app.NewUseCases(loader, st, o, o, &reportGen{store: st})
	uc.WithAuditLogger(st)
	hub := sse.NewHub(100)

	live := httpapi.LiveHandler(httpapi.LiveDeps{StartTime: startTime})
	ready := httpapi.ReadyHandler(apiReadyDeps(cfg, startTime))
	mux := httpapi.NewMux(live, ready)
	httpapi.Register(mux, uc, authStore, hub)

	guard, err := protection.New(protection.Settings{
		Enabled:           cfg.ArcjetEnabled,
		Key:               cfg.ArcjetKey,
		TrustedProxies:    cfg.ArcjetTrustedProxies,
		FallbackPerMinute: cfg.ArcjetFallbackPerMinute,
		FailClosed:        cfg.ArcjetFailClosed,
	})
	if err != nil {
		return err
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = guard.Close(closeCtx)
	}()

	handler := httpapi.RequestID(mux)
	handler = httpapi.RequestLogger(logger)(handler)
	handler = httpapi.Recover(handler)
	handler = httpapi.Timeout(httpapi.DefaultTimeouts().HandlerTimeoutOrDefault())(handler)
	handler = telemetry.HTTPMiddleware(handler)

	srv := httpapi.WithTimeouts(&http.Server{Addr: cfg.ListenAddress, Handler: handler}, httpapi.DefaultTimeouts())
	metricsSrv := &http.Server{
		Addr:              cfg.MetricsListenAddress,
		Handler:           telemetry.MetricsHandler(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		if err := metricsSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Warn("metrics endpoint unavailable; API remains available")
		}
	}()

	startErr := make(chan error, 1)
	go func() {
		observability.LogRedacted(logger, ctx, slog.LevelInfo, "api listening",
			slog.String("listen_addr", cfg.ListenAddress),
		)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			startErr <- err
			return
		}
		startErr <- nil
	}()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(signals)

	select {
	case err := <-startErr:
		return err
	case sig := <-signals:
		observability.LogRedacted(logger, ctx, slog.LevelInfo, "shutdown signal received",
			slog.String("signal", sig.String()),
		)
	}

	return observability.ShutdownWithClosers(ctx, nil, observability.ShutdownConfig{}, logger,
		func(context.Context) {},
		[]observability.Closer{
			{Name: "http_server", Close: func(shutdown context.Context) error { return srv.Shutdown(shutdown) }},
			{Name: "metrics_server", Close: func(shutdown context.Context) error { return metricsSrv.Shutdown(shutdown) }},
			{Name: "database", Close: func(context.Context) error { return nil }},
			{Name: "nats", Close: func(context.Context) error { return nil }},
			{Name: "telemetry", Close: telemetry.Shutdown},
		},
	)
}

func apiReadyDeps(cfg config.Config, startTime time.Time) httpapi.ReadyDeps {
	return httpapi.ReadyDeps{
		StartTime:          startTime,
		DatabaseConfigured: cfg.DatabaseURL != "",
		NATSConfigured:     cfg.NATSURL != "",
		ArcjetEnabled:      cfg.ArcjetEnabled,
		AIConfigured:       cfg.OllamaURL != "" || cfg.OllamaModel != "",
	}
}

// loadAuthStore opens the digest-only key file when authentication is
// enabled. Control-plane routes added in later work share this store; the
// API never loads plaintext secrets and never logs key material.
func loadAuthStore(cfg config.Config) (auth.Store, error) {
	if !cfg.APIKeysEnabled {
		store, err := auth.NewMemoryStore()
		if err != nil {
			return nil, err
		}
		return store, nil
	}
	store, err := auth.NewFileStore(cfg.APIKeysFile)
	if err != nil {
		return nil, err
	}
	if len(store.List()) == 0 && cfg.Environment == "production" {
		return nil, fmt.Errorf("no API keys are configured; bootstrap one before starting production")
	}
	return store, nil
}

// reportGen adapts store.Store to the app.reportGenerator interface.
type reportGen struct {
	store *store.Store
}

func (g *reportGen) GenerateReport(ctx context.Context, runID string) (*report.Report, error) {
	return report.GenerateReport(ctx, g.store, runID)
}
