package observability

import (
	"context"
	"log/slog"
	"time"
)

// RunPhase identifies shutdown-sensitive work so the process can drain
// in-flight execution, verification, and reporting before exiting.
type RunPhase string

const (
	// PhaseSetup covers run preparation before execution.
	PhaseSetup RunPhase = "setup"
	// PhaseExecuting covers worker fault execution.
	PhaseExecuting RunPhase = "executing"
	// PhaseVerifying covers invariant evaluation.
	PhaseVerifying RunPhase = "verifying"
	// PhaseReporting covers report generation/persistence.
	PhaseReporting RunPhase = "reporting"
)

// ShutdownConfig bounds graceful termination.
type ShutdownConfig struct {
	// Timeout caps the whole drain. Non-positive values use DefaultShutdownTimeout.
	Timeout time.Duration
}

// DefaultShutdownTimeout keeps local shutdown responsive while allowing
// active runs to checkpoint before termination.
const DefaultShutdownTimeout = 15 * time.Second

func (c ShutdownConfig) timeoutOrDefault() time.Duration {
	if c.Timeout > 0 {
		return c.Timeout
	}
	return DefaultShutdownTimeout
}

// RunTracker records cancellable in-flight work by run ID. Implementations
// must be safe for concurrent use and must never block Shutdown.
type RunTracker interface {
	// ActiveRuns returns the IDs of unfinished runs for shutdown logging.
	ActiveRuns() []string
	// CancelRun asks one run to stop promptly; unknown IDs are ignored.
	CancelRun(runID string)
	// Wait blocks until tracked work finishes or ctx expires.
	Wait(ctx context.Context) error
}

// Shutdown drains tracked runs, checkpoints their terminal state, and always
// releases HTTP resources. Shutdown never marks an interrupted run as passed:
// checkpoint handlers must record cancelled/timed_out (or leave the existing
// terminal state untouched). It returns nil when shutdown completes within
// cfg.Timeout and a descriptive timeout error otherwise.
//
// Callers supply cancelAll to checkpoint terminal run states, closers to
// release resources in order, and closeRun for compatibility with the
// previous single-resource shape. Every closer runs even when the drain
// times out so the process still exits boundedly. chkptCtx carries timeout,
// never secrets or payloads.
func Shutdown(ctx context.Context, tracker RunTracker, cfg ShutdownConfig, logger *slog.Logger, cancelAll func(chkptCtx context.Context), closeRun func(ctx context.Context) error) error {
	return ShutdownWithClosers(ctx, tracker, cfg, logger, cancelAll, closersFromLegacy(closeRun))
}

// Closer releases one shutdown-sensitive resource: HTTP listener, database
// pool, message-broker connection, or telemetry exporter.
type Closer struct {
	// Name appears in shutdown logs; it must be a stable identifier.
	Name string
	// Close releases the resource using the bounded shutdown context.
	Close func(ctx context.Context) error
}

func closersFromLegacy(closeRun func(ctx context.Context) error) []Closer {
	if closeRun == nil {
		return nil
	}
	return []Closer{{Name: "http_server", Close: closeRun}}
}

// ShutdownWithClosers drains tracked runs, checkpoints terminal state, then
// closes resources in order. Every closer runs even after a drain timeout.
func ShutdownWithClosers(ctx context.Context, tracker RunTracker, cfg ShutdownConfig, logger *slog.Logger, cancelAll func(chkptCtx context.Context), closers []Closer) error {
	if logger == nil {
		logger = slog.Default()
	}
	timeout := cfg.timeoutOrDefault()
	parent := ctx
	if parent == nil {
		parent = context.Background()
	}
	chkptCtx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()

	active := []string(nil)
	if tracker != nil {
		active = tracker.ActiveRuns()
		if cancelAll != nil {
			for _, runID := range active {
				tracker.CancelRun(runID)
			}
		}
	}
	LogRedacted(logger, parent, slog.LevelInfo, "shutdown started",
		slog.Int("active_runs", len(active)),
		slog.Int64("timeout_ms", timeout.Milliseconds()),
	)

	if cancelAll != nil {
		cancelAll(chkptCtx)
	}
	if tracker != nil {
		_ = tracker.Wait(chkptCtx)
	}

	var closeErr error
	for _, closer := range closers {
		if closer.Close == nil {
			continue
		}
		name := closer.Name
		if name == "" {
			name = "resource"
		}
		if err := closer.Close(chkptCtx); err != nil {
			LogRedacted(logger, parent, slog.LevelWarn, "shutdown resource close failed",
				slog.String("resource", name),
			)
			if closeErr == nil {
				closeErr = err
			}
			continue
		}
		LogRedacted(logger, parent, slog.LevelInfo, "shutdown resource closed",
			slog.String("resource", name),
		)
	}

	if err := chkptCtx.Err(); err == context.DeadlineExceeded {
		LogRedacted(logger, parent, slog.LevelWarn, "shutdown timed out",
			slog.Int("active_runs", len(active)),
		)
		if closeErr != nil {
			return closeErr
		}
		return context.DeadlineExceeded
	}
	LogRedacted(logger, parent, slog.LevelInfo, "shutdown completed",
		slog.Int("active_runs", len(active)),
	)
	return closeErr
}
