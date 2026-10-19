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

	"github.com/zannunakiz/GenesisGO/internal/auth"
	"github.com/zannunakiz/GenesisGO/internal/config"
	"github.com/zannunakiz/GenesisGO/internal/httpapi"
	"github.com/zannunakiz/GenesisGO/internal/observability"
	"github.com/zannunakiz/GenesisGO/internal/protection"
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

	authStore, err := loadAuthStore(cfg)
	if err != nil {
		return err
	}
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

	live := httpapi.LiveHandler(httpapi.LiveDeps{StartTime: startTime})
	ready := httpapi.ReadyHandler(apiReadyDeps(cfg, startTime))
	mux := httpapi.NewMux(live, ready)
	_ = authStore
	handler := httpapi.RequestID(mux)
	handler = httpapi.RequestLogger(logger)(handler)
	handler = httpapi.Recover(handler)
	handler = httpapi.Timeout(httpapi.DefaultTimeouts().HandlerTimeoutOrDefault())(handler)

	srv := httpapi.WithTimeouts(&http.Server{Addr: cfg.ListenAddress, Handler: handler}, httpapi.DefaultTimeouts())

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
			{Name: "database", Close: func(context.Context) error { return nil }},
			{Name: "nats", Close: func(context.Context) error { return nil }},
			{Name: "telemetry", Close: func(context.Context) error { return nil }},
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
