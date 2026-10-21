package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/zannunakiz/GenesisGO/internal/config"
	"github.com/zannunakiz/GenesisGO/internal/observability"
	"github.com/zannunakiz/GenesisGO/internal/sampleconsumer"
	"github.com/zannunakiz/GenesisGO/internal/store"
)

func main() {
	if err := run(context.Background()); err != nil {
		fmt.Fprintln(os.Stderr, "GenesisGo sample consumer failed to start")
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

	consumer, err := sampleconsumer.New(sampleconsumer.Config{
		Ledger: store.NewStore(pool),
		Mode:   sampleconsumer.ModeSafe,
	})
	if err != nil {
		return fmt.Errorf("create consumer: %w", err)
	}

	observability.LogRedacted(logger, ctx, slog.LevelInfo, "sample consumer ready",
		slog.String("mode", string(consumer.Mode())),
		slog.String("database_url", cfg.DatabaseURL),
	)

	started := time.Now()
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(signals)

	select {
	case <-ctx.Done():
		return ctx.Err()
	case sig := <-signals:
		observability.LogRedacted(logger, ctx, slog.LevelInfo, "shutdown signal received",
			slog.String("signal", sig.String()),
			slog.String("uptime", time.Since(started).String()),
		)
	}

	return observability.ShutdownWithClosers(ctx, nil, observability.ShutdownConfig{}, logger,
		func(context.Context) {},
		[]observability.Closer{
			{Name: "database", Close: func(context.Context) error { return nil }},
		},
	)
}

