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

	"github.com/zannunakiz/GenesisGO/internal/config"
	"github.com/zannunakiz/GenesisGO/internal/httpapi"
	"github.com/zannunakiz/GenesisGO/internal/observability"
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

	live := httpapi.LiveHandler(httpapi.LiveDeps{StartTime: startTime})
	ready := httpapi.ReadyHandler(apiReadyDeps(cfg, startTime))
	mux := httpapi.NewMux(live, ready)
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
