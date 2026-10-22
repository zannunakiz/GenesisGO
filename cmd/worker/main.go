package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/zannunakiz/GenesisGO/internal/broker"
	"github.com/zannunakiz/GenesisGO/internal/config"
	"github.com/zannunakiz/GenesisGO/internal/invariants"
	"github.com/zannunakiz/GenesisGO/internal/observability"
	"github.com/zannunakiz/GenesisGO/internal/orchestrator"
	"github.com/zannunakiz/GenesisGO/internal/sampleconsumer"
	"github.com/zannunakiz/GenesisGO/internal/scenarios"
	"github.com/zannunakiz/GenesisGO/internal/store"
)

func main() {
	if err := run(context.Background()); err != nil {
		fmt.Fprintln(os.Stderr, "GenesisGo worker failed to start")
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

	pool, err := store.NewPool(ctx, cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("create database pool: %w", err)
	}
	defer pool.Close()
	st := store.NewStore(pool)

	brk, err := broker.New(broker.Config{
		URL:           cfg.NATSURL,
		Stream:        "genesisgo.events",
		SubjectPrefix: "gg.run",
		MaxAge:        time.Hour,
		MaxDeliver:    5,
		AckWait:       30 * time.Second,
		MaxAckPending: 1000,
		Logger:        logger,
	})
	if err != nil {
		return fmt.Errorf("create broker: %w", err)
	}
	defer func() { _ = brk.Close() }()

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
		Broker:            brk,
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

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})
	srv := &http.Server{Addr: "127.0.0.1:8081", Handler: mux}
	go func() { _ = srv.ListenAndServe() }()
	defer func() { _ = srv.Shutdown(context.Background()) }()

	o.StartWorkers(ctx, cfg.WorkerConcurrency)
	logger.Info("worker started",
		slog.Int("worker_concurrency", cfg.WorkerConcurrency),
	)

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	<-signals
	logger.Info("shutdown signal received")

	o.Stop()
	return nil
}
