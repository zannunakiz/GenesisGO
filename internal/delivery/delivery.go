// Package delivery provides the application-level event publish, acknowledgement,
// and redelivery handling layer for GenesisGo. It wraps the Step 17 NATS JetStream
// broker and records every delivery attempt (attempt number, delivery metadata,
// acknowledgement/negative-acknowledgement outcome, and timestamps) in the store.
//
// The business domain does not depend on NATS types. Delivery attempts are
// recorded durably so the event timeline and redelivery behaviour remain
// observable. Explicit ack/nack/ack-wait behaviour is enforced and redelivery
// stops at the configured maximum attempt count; correctness is protected at the
// business layer, not by claiming exactly-once processing.
package delivery

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/zannunakiz/GenesisGO/internal/broker"
	"github.com/zannunakiz/GenesisGO/internal/events"
	"github.com/zannunakiz/GenesisGO/internal/store"
)

// Ack is the acknowledgement contract offered to a delivery handler. Exactly one
// of Ack, Nak, or Term should be called per message. When the handler returns
// without calling any of them, the delivery layer acknowledges on success and
// negative-acknowledges on error (which triggers redelivery).
type Ack interface {
	Ack() error
	Nak() error
	Term() error
}

// HandlerFunc processes a single delivered event. attemptNumber is the current
// delivery attempt for the event. ack lets the handler explicitly acknowledge,
// negative-acknowledge, or terminate the message.
type HandlerFunc func(ctx context.Context, subject string, envelope *events.EventEnvelope, attemptNumber int32, ack Ack) error

// FaultError is returned by a handler to inject a named fault (e.g. a scenario
// fault such as "retry" or "crash_after_commit") while still reporting a
// delivery failure. The fault name is recorded on the attempt record.
type FaultError struct {
	Fault string
	Err   error
}

func (e *FaultError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("delivery fault %q: %v", e.Fault, e.Err)
	}
	return fmt.Sprintf("delivery fault %q", e.Fault)
}

func (e *FaultError) Unwrap() error { return e.Err }

// IsFaultError reports whether err is a fault injection error.
func IsFaultError(err error) bool {
	var target *FaultError
	return errors.As(err, &target)
}

// AttemptStore records delivery attempts. *store.Store satisfies it.
type AttemptStore interface {
	CreateEventAttempt(
		ctx context.Context,
		runID, eventID string,
		attemptNumber int32,
		deliveredAt time.Time,
		acknowledgedAt *time.Time,
		faultInjected *string,
	) (*store.EventAttempt, error)
}

// Compile-time check that *store.Store satisfies AttemptStore.
var _ AttemptStore = (*store.Store)(nil)

// Config holds the dependencies and limits for the delivery layer.
type Config struct {
	// Broker is the NATS JetStream transport (Step 17).
	Broker broker.Broker
	// Recorder persists delivery attempts.
	Recorder AttemptStore
	// RunID scopes all delivery attempts to a single run.
	RunID string
	// MaxAttempts caps redelivery attempts per event. Must be >= 1.
	MaxAttempts int
	// FaultInjected, when non-nil, records the same fault name on every
	// attempt recorded by this consumer. Optional scenario-level fault.
	FaultInjected *string
	// Logger is optional; slog.Default() is used when nil.
	Logger *slog.Logger
}

// Publisher publishes events to a run-scoped subject and records the first
// delivery attempt.
// Publish wraps payload in a versioned event envelope, publishes it to the
// run-scoped subject, and records delivery attempt #1.
func (p *Publisher) Publish(ctx context.Context, eventID string, payload []byte) error {
	if eventID == "" {
		return errors.New("delivery event id is required")
	}
	if err := brokerValidateRunID(p.runID); err != nil {
		return err
	}

	env := &events.EventEnvelope{
		EventID:       eventID,
		EventType:     events.EventTypePayment,
		SchemaVersion: events.SchemaVersion,
		OccurredAt:    time.Now().UTC(),
		RunID:         p.runID,
		Payload:       payload,
	}
	if err := events.Validate(env); err != nil {
		return fmt.Errorf("validate envelope: %w", err)
	}
	data, err := json.Marshal(env)
	if err != nil {
		return fmt.Errorf("marshal envelope: %w", err)
	}

	if err := p.broker.Publish(ctx, p.runID, data); err != nil {
		return fmt.Errorf("publish: %w", err)
	}

	now := time.Now().UTC()
	if _, err := p.recorder.CreateEventAttempt(ctx, p.runID, eventID, 1, now, nil, nil); err != nil {
		p.log.Warn("recorded publish but attempt record failed",
			slog.String("run_id", p.runID),
			slog.String("event_id", eventID),
			slog.String("error", safeError(err)),
		)
		return err
	}
	p.log.Debug("published event and recorded attempt",
		slog.String("run_id", p.runID),
		slog.String("event_id", eventID),
		slog.Int("attempt", 1),
	)
	return nil
}

type Publisher struct {
	broker   broker.Broker
	recorder AttemptStore
	runID    string
	log      *slog.Logger
}

// NewPublisher builds a Publisher.
func NewPublisher(cfg Config) (*Publisher, error) {
	if cfg.Broker == nil {
		return nil, errors.New("delivery broker is required")
	}
	if cfg.Recorder == nil {
		return nil, errors.New("delivery attempt recorder is required")
	}
	if cfg.RunID == "" {
		return nil, errors.New("delivery run id is required")
	}
	return &Publisher{
		broker:   cfg.Broker,
		recorder: cfg.Recorder,
		runID:    cfg.RunID,
		log:      logOrDefault(cfg.Logger),
	}, nil
}

// logOrDefault returns cfg.Logger when non-nil, otherwise slog.Default().
func logOrDefault(logger *slog.Logger) *slog.Logger {
	if logger != nil {
		return logger
	}
	return slog.Default()
}

// Consumer subscribes to a run-scoped subject and records each delivery attempt,
// managing explicit ack/nack/ack-wait behaviour with a maximum attempt limit.
type Consumer struct {
	broker      broker.Broker
	recorder    AttemptStore
	runID       string
	maxAttempts int
	fault       *string
	log         *slog.Logger
	mu          sync.Mutex
	attempts    map[string]int32 // eventID -> next attempt number
}

// NewConsumer builds a Consumer. MaxAttempts defaults to 5 when <= 0.
func NewConsumer(cfg Config) (*Consumer, error) {
	if cfg.Broker == nil {
		return nil, errors.New("delivery broker is required")
	}
	if cfg.Recorder == nil {
		return nil, errors.New("delivery attempt recorder is required")
	}
	if cfg.RunID == "" {
		return nil, errors.New("delivery run id is required")
	}
	maxAttempts := cfg.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = 5
	}
	return &Consumer{
		broker:      cfg.Broker,
		recorder:    cfg.Recorder,
		runID:       cfg.RunID,
		maxAttempts: maxAttempts,
		fault:       cfg.FaultInjected,
		log:         logOrDefault(cfg.Logger),
		attempts:    make(map[string]int32),
	}, nil
}

// Consume binds to the run-scoped subject and invokes handler for each
// delivered message. Each delivery is recorded as an attempt. When the handler
// returns nil the message is acknowledged; when it returns an error the message
// is negatively acknowledged (redelivered) unless MaxAttempts has been reached,
// in which case it is terminated. The handler may call ack methods explicitly,
// in which case the layer does not act again.
func (c *Consumer) Consume(ctx context.Context, handler HandlerFunc) error {
	if handler == nil {
		return errors.New("delivery handler is required")
	}
	return c.broker.Subscribe(ctx, c.runID, func(ctx context.Context, subject string, payload []byte, bck broker.Ack) error {
		env, err := parseEnvelope(payload)
		if err != nil {
			if termErr := bck.Term(); termErr != nil {
				c.log.Warn("terminate unparseable message", slog.String("subject", subject), slog.String("error", safeError(termErr)))
			}
			return nil
		}

		c.mu.Lock()
		attempt := c.attempts[env.EventID] + 1
		c.attempts[env.EventID] = attempt
		c.mu.Unlock()

		deliveredAt := time.Now().UTC()
		ack := &deliveryAck{brokerAck: bck}

		var handlerErr error
		func() {
			defer func() {
				if p := recover(); p != nil {
					handlerErr = fmt.Errorf("handler panic: %v", p)
				}
			}()
			handlerErr = handler(ctx, subject, env, attempt, ack)
		}()

		var fault *string
		if c.fault != nil {
			fault = c.fault
		}
		if fe := (*FaultError)(nil); errors.As(handlerErr, &fe) {
			fault = &fe.Fault
		}

		acknowledgedAt := (*time.Time)(nil)
		if ack.acted() {
			// The handler explicitly acknowledged, negatively acknowledged, or
			// terminated. Do not act again on the broker; only record the outcome.
			if ack.taken() == "ack" {
				t := time.Now().UTC()
				acknowledgedAt = &t
			}
		} else if handlerErr != nil {
			if attempt < int32(c.maxAttempts) {
				if err := ack.Nak(); err != nil {
					c.log.Warn("nak failed", slog.String("subject", subject), slog.String("error", safeError(err)))
				}
				c.log.Debug("message negatively acknowledged",
					slog.String("run_id", c.runID),
					slog.String("event_id", env.EventID),
					slog.Int("attempt", int(attempt)),
					slog.String("error", safeError(handlerErr)),
				)
			} else {
				if err := ack.Term(); err != nil {
					c.log.Warn("term failed", slog.String("subject", subject), slog.String("error", safeError(err)))
				}
				c.log.Debug("message terminated at max attempts",
					slog.String("run_id", c.runID),
					slog.String("event_id", env.EventID),
					slog.Int("attempt", int(attempt)),
					slog.Int("max_attempts", c.maxAttempts),
				)
			}
		} else {
			if err := ack.Ack(); err != nil {
				c.log.Warn("ack failed", slog.String("subject", subject), slog.String("error", safeError(err)))
			}
			t := time.Now().UTC()
			acknowledgedAt = &t
			c.log.Debug("message acknowledged",
				slog.String("run_id", c.runID),
				slog.String("event_id", env.EventID),
				slog.Int("attempt", int(attempt)),
			)
		}

		if _, err := c.recorder.CreateEventAttempt(ctx, c.runID, env.EventID, attempt, deliveredAt, acknowledgedAt, fault); err != nil {
			c.log.Warn("record attempt failed",
				slog.String("run_id", c.runID),
				slog.String("event_id", env.EventID),
				slog.Int("attempt", int(attempt)),
				slog.String("error", safeError(err)),
			)
			return err
		}
		return nil
	})
}

// deliveryAck wraps a broker.Ack so acknowledgement can be called at most once
// and records which explicit action the handler took.
type deliveryAck struct {
	brokerAck broker.Ack
	done      atomic.Bool
	action    string // "ack", "nak", or "term"; valid only after done is set
}

func (d *deliveryAck) Ack() error    { d.action = "ack"; d.done.Store(true); return d.brokerAck.Ack() }
func (d *deliveryAck) Nak() error    { d.action = "nak"; d.done.Store(true); return d.brokerAck.Nak() }
func (d *deliveryAck) Term() error   { d.action = "term"; d.done.Store(true); return d.brokerAck.Term() }
func (d *deliveryAck) acted() bool   { return d.done.Load() }
func (d *deliveryAck) taken() string { return d.action }

// parseEnvelope decodes a raw message payload into an event envelope.
func parseEnvelope(payload []byte) (*events.EventEnvelope, error) {
	if len(payload) == 0 {
		return nil, errors.New("empty payload")
	}
	var env events.EventEnvelope
	if err := json.Unmarshal(payload, &env); err != nil {
		return nil, fmt.Errorf("parse envelope: %w", err)
	}
	if env.EventID == "" {
		return nil, errors.New("envelope missing event_id")
	}
	return &env, nil
}

// brokerValidateRunID validates a run id is safe to embed in a NATS subject.
func brokerValidateRunID(runID string) error {
	if runID == "" {
		return errors.New("run id is required")
	}
	for _, ch := range runID {
		switch ch {
		case '.', '*', '>', ':', '/', '\\', ' ', '\t', '\n', '\r':
			return fmt.Errorf("run id %q contains unsupported character %q", runID, ch)
		}
	}
	if len(runID) > 128 {
		return fmt.Errorf("run id exceeds %d characters", 128)
	}
	return nil
}

// safeError returns an error message safe for logs.
func safeError(err error) string {
	if err == nil {
		return ""
	}
	return fmt.Sprintf("%v", err)
}
