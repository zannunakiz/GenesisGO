// Package broker provides a NATS JetStream adapter for transporting
// run-scoped events within GenesisGo.
//
// The business domain never depends on NATS types: all application-facing
// signatures use the Broker interface and plain Go values. Events are
// published to run-scoped subjects (gg.run.<run_id>.<kind>) so evidence from
// one run can never be delivered to another run's consumer. Consumers are
// durable, use explicit acknowledgement, and enforce redelivery limits so
// repeatable fault scenarios terminate deterministically.
package broker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/zannunakiz/GenesisGO/internal/observability"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// Defaults for the JetStream stream and durable consumers.
const (
	defaultStream         = "genesisgo.events"
	defaultSubjectPrefix  = "gg.run"
	defaultMaxAge         = 1 * time.Hour
	defaultMaxMsgs        = 1_000_000
	defaultMaxMsgsPerSubj = 10_000
	defaultAckWait        = 30 * time.Second
	defaultMaxDeliver     = 5
	defaultMaxAckPending  = 1_000
)

// ErrNotStarted is returned when an operation is attempted on a broker that
// has not been successfully started or has already been closed.
var ErrNotStarted = errors.New("broker is not started")

// ErrAlreadyClosed is returned when Close is called after the broker has shut
// down. Close remains idempotent; callers may ignore this error.
var ErrAlreadyClosed = errors.New("broker already closed")

// ErrInvalidSubject is returned when a run id or subject kind would produce an
// unsafe NATS subject (wildcards, separators, or empty segments).
var ErrInvalidSubject = errors.New("invalid run subject")

// Ack is the acknowledgement contract delivered to a message handler. Exactly
// one of Ack, Nak, or Term should be called per message. When the handler
// returns without calling any of them, the broker acknowledges on success and
// negative-acknowledges on error.
type Ack interface {
	Ack() error
	Nak() error
	Term() error
}

// HandlerFunc processes a single delivered message. subject is the NATS
// subject the message was received on and payload is the message body.
type HandlerFunc func(ctx context.Context, subject string, payload []byte, ack Ack) error

// Broker is the application-facing contract for JetStream delivery. It does
// not expose NATS types.
type Broker interface {
	// Publish writes payload to the run-scoped events subject and waits for
	// the stream to acknowledge persistence.
	Publish(ctx context.Context, runID string, payload []byte) error

	// Subscribe binds a durable push consumer to the run-scoped events subject
	// and invokes handler for each message. The durable consumer is created
	// idempotently.
	Subscribe(ctx context.Context, runID string, handler HandlerFunc) error

	// DeleteConsumer stops and removes a run's durable consumer so no further
	// redeliveries occur for that run. It is idempotent: deleting a consumer
	// that does not exist is not an error.
	DeleteConsumer(ctx context.Context, runID string) error

	// Close shuts down the NATS connection and stops all consumers. It is
	// idempotent.
	Close() error

	// Ready reports whether the broker connection is currently established.
	Ready() bool
}

// Config holds NATS connection and JetStream stream settings.
type Config struct {
	// URL is the NATS server URL (nats://, tls://, ws://, or wss://).
	URL string
	// Stream is the JetStream stream name.
	Stream string
	// SubjectPrefix namespaces subjects per run, e.g. "gg.run".
	SubjectPrefix string
	// MaxAge retains stream messages for at most this duration.
	MaxAge time.Duration
	// MaxMsgs caps total stream messages.
	MaxMsgs int64
	// MaxMsgsPerSubject caps messages per subject.
	MaxMsgsPerSubject int64
	// AckWait is how long JetStream waits for an acknowledgement before
	// redelivering.
	AckWait time.Duration
	// MaxDeliver caps redelivery attempts per message.
	MaxDeliver int
	// MaxAckPending caps unacknowledged messages per consumer.
	MaxAckPending int
	// Logger is optional; slog.Default() is used when nil.
	Logger *slog.Logger
	// Metrics records bounded NATS operation metrics when configured.
	Metrics *observability.Metrics
}

// Compile-time check.
var _ Broker = (*jetStreamBroker)(nil)

type jetStreamBroker struct {
	cfg      Config
	conn     *nats.Conn
	js       jetstream.JetStream
	stream   string
	prefix   string
	log      *slog.Logger
	metrics  *observability.Metrics
	mu       sync.Mutex
	consumes map[string]jetstream.ConsumeContext
	started  atomic.Bool
	closed   atomic.Bool
}

// New connects to NATS, creates the JetStream stream idempotently, and
// returns a Broker. It returns an error when the broker is not reachable so
// callers can handle broker-not-ready explicitly instead of failing later.
func New(cfg Config) (Broker, error) {
	if cfg.URL == "" {
		return nil, errors.New("broker NATS URL is required")
	}
	if cfg.Stream == "" {
		cfg.Stream = defaultStream
	}
	if cfg.SubjectPrefix == "" {
		cfg.SubjectPrefix = defaultSubjectPrefix
	}
	if err := validateSubjectPrefix(cfg.SubjectPrefix); err != nil {
		return nil, err
	}
	if cfg.MaxAge <= 0 {
		cfg.MaxAge = defaultMaxAge
	}
	if cfg.MaxMsgs <= 0 {
		cfg.MaxMsgs = defaultMaxMsgs
	}
	if cfg.MaxMsgsPerSubject <= 0 {
		cfg.MaxMsgsPerSubject = defaultMaxMsgsPerSubj
	}
	if cfg.AckWait <= 0 {
		cfg.AckWait = defaultAckWait
	}
	if cfg.MaxDeliver <= 0 {
		cfg.MaxDeliver = defaultMaxDeliver
	}
	if cfg.MaxAckPending <= 0 {
		cfg.MaxAckPending = defaultMaxAckPending
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}

	opts := []nats.Option{
		nats.Name("genesisgo-broker"),
		nats.MaxReconnects(10),
		nats.ReconnectWait(2 * time.Second),
		nats.Timeout(10 * time.Second),
		nats.PingInterval(20 * time.Second),
		nats.DisconnectHandler(func(_ *nats.Conn) {
			cfg.Logger.Info("nats disconnected")
		}),
		nats.ReconnectHandler(func(_ *nats.Conn) {
			cfg.Logger.Info("nats reconnected")
		}),
		nats.ClosedHandler(func(_ *nats.Conn) {
			cfg.Logger.Info("nats connection closed")
		}),
	}

	conn, err := nats.Connect(cfg.URL, opts...)
	if err != nil {
		return nil, fmt.Errorf("connect nats: %w", err)
	}

	js, err := jetstream.New(conn)
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("create jetstream context: %w", err)
	}

	b := &jetStreamBroker{
		cfg:      cfg,
		conn:     conn,
		js:       js,
		stream:   cfg.Stream,
		prefix:   cfg.SubjectPrefix,
		log:      cfg.Logger,
		metrics:  cfg.Metrics,
		consumes: make(map[string]jetstream.ConsumeContext),
	}

	if err := b.ensureStream(context.Background()); err != nil {
		conn.Close()
		return nil, fmt.Errorf("ensure stream: %w", err)
	}

	b.started.Store(true)
	b.log.Info("broker ready",
		slog.String("stream", cfg.Stream),
		slog.String("subject_prefix", cfg.SubjectPrefix),
	)
	return b, nil
}

// Compile-time check.
var _ Broker = (*jetStreamBroker)(nil)

// ensureStream creates the stream if it does not already exist. An existing
// stream is left untouched, which makes broker startup idempotent.
func (b *jetStreamBroker) ensureStream(ctx context.Context) error {
	if _, err := b.js.Stream(ctx, b.stream); err == nil {
		return nil
	} else if !errors.Is(err, jetstream.ErrStreamNotFound) {
		return err
	}

	_, err := b.js.CreateStream(ctx, jetstream.StreamConfig{
		Name:              b.stream,
		Subjects:          []string{b.subjectWildcard()},
		Retention:         jetstream.LimitsPolicy,
		MaxAge:            b.cfg.MaxAge,
		MaxMsgs:           b.cfg.MaxMsgs,
		MaxMsgsPerSubject: b.cfg.MaxMsgsPerSubject,
		Replicas:          1,
		Discard:           jetstream.DiscardOld,
		Duplicates:        2 * time.Minute,
	})
	if err != nil && errors.Is(err, jetstream.ErrStreamNameAlreadyInUse) {
		return nil
	}
	return err
}

// Publish writes payload to the run-scoped events subject and waits for the
// stream persistence acknowledgement.
func (b *jetStreamBroker) Publish(ctx context.Context, runID string, payload []byte) (resultErr error) {
	started := time.Now()
	ctx, span := otel.Tracer("github.com/zannunakiz/GenesisGO/nats").Start(ctx, "nats.publish",
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(attribute.String("messaging.system", "nats"), attribute.String("messaging.operation.name", "publish")),
	)
	defer func() {
		if resultErr != nil {
			span.SetStatus(codes.Error, "NATS publish failed")
		}
		span.End()
		if b.metrics != nil {
			b.metrics.NATS(ctx, "publish", time.Since(started), resultErr)
		}
	}()
	if !b.started.Load() || b.closed.Load() {
		return ErrNotStarted
	}
	subject, err := b.subjectFor(runID, KindEvents)
	if err != nil {
		return err
	}
	msg := nats.NewMsg(subject)
	msg.Data = payload
	otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(msg.Header))
	if _, err := b.js.PublishMsg(ctx, msg); err != nil {
		return fmt.Errorf("publish %q: %w", subject, err)
	}
	b.log.Debug("published event",
		slog.String("run_id", runID),
		slog.String("subject", subject),
		slog.Int("bytes", len(payload)),
	)
	return nil
}

// Subscribe binds a durable push consumer to the run-scoped events subject
// and invokes handler for each delivered message. The consumer is created
// idempotently and uses explicit acknowledgement with a redelivery limit.
func (b *jetStreamBroker) Subscribe(ctx context.Context, runID string, handler HandlerFunc) (resultErr error) {
	started := time.Now()
	messageParent := ctx
	ctx, span := otel.Tracer("github.com/zannunakiz/GenesisGO/nats").Start(ctx, "nats.subscribe",
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(attribute.String("messaging.system", "nats"), attribute.String("messaging.operation.name", "subscribe")),
	)
	defer func() {
		if resultErr != nil {
			span.SetStatus(codes.Error, "NATS subscribe failed")
		}
		span.End()
		if b.metrics != nil {
			b.metrics.NATS(messageParent, "subscribe", time.Since(started), resultErr)
		}
	}()
	if !b.started.Load() || b.closed.Load() {
		return ErrNotStarted
	}
	subject, err := b.subjectFor(runID, KindEvents)
	if err != nil {
		return err
	}
	deliverSubject, err := b.deliverySubjectFor(runID)
	if err != nil {
		return err
	}
	durable := b.consumerName(runID)

	consumer, err := b.js.CreateOrUpdatePushConsumer(ctx, b.stream, jetstream.ConsumerConfig{
		Durable:        durable,
		FilterSubject:  subject,
		AckPolicy:      jetstream.AckExplicitPolicy,
		AckWait:        b.cfg.AckWait,
		MaxDeliver:     b.cfg.MaxDeliver,
		MaxAckPending:  b.cfg.MaxAckPending,
		DeliverSubject: deliverSubject,
	})
	if err != nil {
		return fmt.Errorf("ensure consumer %q: %w", durable, err)
	}

	consumeCtx, err := consumer.Consume(func(msg jetstream.Msg) {
		messageCtx := otel.GetTextMapPropagator().Extract(messageParent, propagation.HeaderCarrier(msg.Headers()))
		b.handleMessage(messageCtx, msg, func(ctx context.Context, subject string, payload []byte, ack Ack) error {
			started := time.Now()
			ctx, span := otel.Tracer("github.com/zannunakiz/GenesisGO/nats").Start(ctx, "nats.consume",
				trace.WithSpanKind(trace.SpanKindConsumer),
				trace.WithAttributes(attribute.String("messaging.system", "nats"), attribute.String("messaging.operation.name", "consume")),
			)
			err := handler(ctx, subject, payload, ack)
			if err != nil {
				span.SetStatus(codes.Error, "NATS message handler failed")
			}
			span.End()
			if b.metrics != nil {
				b.metrics.NATS(ctx, "consume", time.Since(started), err)
			}
			return err
		})
	})
	if err != nil {
		return fmt.Errorf("consume %q: %w", subject, err)
	}

	b.mu.Lock()
	b.consumes[runID] = consumeCtx
	b.mu.Unlock()

	b.log.Debug("subscribed",
		slog.String("run_id", runID),
		slog.String("subject", subject),
		slog.String("durable", durable),
	)
	return nil
}

// DeleteConsumer stops and removes a run's durable consumer so no further
// redeliveries occur for that run. It is idempotent: deleting a consumer that
// does not exist is not an error. The stream and its messages are retained so
// evidence remains available for reporting.
func (b *jetStreamBroker) DeleteConsumer(ctx context.Context, runID string) (resultErr error) {
	started := time.Now()
	ctx, span := otel.Tracer("github.com/zannunakiz/GenesisGO/nats").Start(ctx, "nats.delete_consumer",
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(attribute.String("messaging.system", "nats"), attribute.String("messaging.operation.name", "delete_consumer")),
	)
	defer func() {
		if resultErr != nil {
			span.SetStatus(codes.Error, "NATS consumer cleanup failed")
		}
		span.End()
		if b.metrics != nil {
			b.metrics.NATS(ctx, "delete_consumer", time.Since(started), resultErr)
		}
	}()
	if !b.started.Load() || b.closed.Load() {
		return ErrNotStarted
	}
	if err := validateRunID(runID); err != nil {
		return err
	}
	durable := b.consumerName(runID)
	if err := b.js.DeleteConsumer(ctx, b.stream, durable); err != nil {
		// Consumer may already be gone; treat as success for idempotency.
		if isConsumerNotFoundErr(err) {
			b.log.Debug("consumer already removed",
				slog.String("run_id", runID),
				slog.String("durable", durable),
			)
			return nil
		}
		return fmt.Errorf("delete consumer %q: %w", durable, err)
	}
	b.mu.Lock()
	if cc, ok := b.consumes[runID]; ok {
		cc.Stop()
		delete(b.consumes, runID)
	}
	b.mu.Unlock()
	b.log.Debug("consumer deleted",
		slog.String("run_id", runID),
		slog.String("durable", durable),
	)
	return nil
}

// Close shuts down the NATS connection and stops all consumers. It is
// idempotent and safe to call multiple times.
func (b *jetStreamBroker) Close() error {
	if b.closed.Swap(true) {
		return ErrAlreadyClosed
	}
	b.started.Store(false)
	b.mu.Lock()
	for runID, cc := range b.consumes {
		cc.Stop()
		delete(b.consumes, runID)
	}
	b.mu.Unlock()
	if b.conn != nil {
		b.conn.Close()
	}
	return nil
}

// Ready reports whether the broker connection is currently established.
func (b *jetStreamBroker) Ready() bool {
	return b.started.Load() && !b.closed.Load() && b.conn != nil && b.conn.IsConnected()
}

// Kind identifies the category of a run-scoped subject.
type Kind string

const (
	// KindEvents is the subject used to deliver scenario events to a run's
	// consumer.
	KindEvents Kind = "events"
	// KindCommands is reserved for run-scoped control messages.
	KindCommands Kind = "commands"
)

// subjectFor builds the fully qualified subject for a run and kind:
// <prefix>.<run_id>.<kind>. Run ids are validated so they cannot introduce
// subject segments or wildcards.
func (b *jetStreamBroker) subjectFor(runID string, kind Kind) (string, error) {
	if err := validateRunID(runID); err != nil {
		return "", err
	}
	if kind != KindEvents && kind != KindCommands {
		return "", fmt.Errorf("unsupported subject kind %q", kind)
	}
	return fmt.Sprintf("%s.%s.%s", b.prefix, runID, kind), nil
}

// deliverySubjectFor builds the push-consumer delivery subject for a run.
func (b *jetStreamBroker) deliverySubjectFor(runID string) (string, error) {
	if err := validateRunID(runID); err != nil {
		return "", err
	}
	return fmt.Sprintf("%s.%s.delivery", b.prefix, runID), nil
}

// subjectWildcard returns the stream subject wildcard matching all run-scoped
// subjects under the configured prefix.
func (b *jetStreamBroker) subjectWildcard() string {
	return fmt.Sprintf("%s.*.*", b.prefix)
}

// consumerName derives a deterministic durable consumer name for a run.
func (b *jetStreamBroker) consumerName(runID string) string {
	return fmt.Sprintf("consumer-%s-events", runID)
}

// handleMessage invokes the handler and manages acknowledgement. If the
// handler acknowledges explicitly, the broker does not act again. Otherwise
// the broker acknowledges on a nil return and negative-acknowledges on error,
// which triggers redelivery up to MaxDeliver times.
func (b *jetStreamBroker) handleMessage(ctx context.Context, msg jetstream.Msg, handler HandlerFunc) {
	ack := &msgAck{msg: msg}
	var handlerErr error
	func() {
		defer func() {
			if p := recover(); p != nil {
				handlerErr = fmt.Errorf("handler panic: %v", p)
			}
		}()
		handlerErr = handler(ctx, msg.Subject(), msg.Data(), ack)
	}()

	if ack.done.Load() {
		return
	}
	if handlerErr != nil {
		if err := msg.Nak(); err != nil {
			b.log.Warn("nak failed", slog.String("subject", msg.Subject()), slog.String("error", safeError(err)))
		}
		b.log.Debug("message negatively acknowledged",
			slog.String("subject", msg.Subject()),
			slog.String("error", safeError(handlerErr)),
		)
		return
	}
	if err := msg.Ack(); err != nil {
		b.log.Warn("ack failed", slog.String("subject", msg.Subject()), slog.String("error", safeError(err)))
	}
}

// msgAck wraps a JetStream message so acknowledgement can be called at most
// once. Subsequent calls are no-ops.
type msgAck struct {
	msg  jetstream.Msg
	done atomic.Bool
}

func (a *msgAck) Ack() error  { return a.doneOnce(a.msg.Ack) }
func (a *msgAck) Nak() error  { return a.doneOnce(a.msg.Nak) }
func (a *msgAck) Term() error { return a.doneOnce(a.msg.Term) }

func (a *msgAck) doneOnce(fn func() error) error {
	if a.done.Swap(true) {
		return nil
	}
	return fn()
}

// validateRunID rejects empty ids and any id that could alter NATS subject
// segmentation or introduce wildcard matching.
func validateRunID(runID string) error {
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

// validateSubjectPrefix ensures the subject prefix cannot match every run or
// introduce leading/trailing wildcards.
func validateSubjectPrefix(prefix string) error {
	if prefix == "" {
		return errors.New("subject prefix is required")
	}
	for _, ch := range prefix {
		switch ch {
		case '*', '>', ':', '/', '\\', ' ', '\t', '\n', '\r':
			return fmt.Errorf("subject prefix %q contains unsupported character %q", prefix, ch)
		}
	}
	return nil
}

// safeError returns an error message safe for logs: it never includes the NATS
// URL or connection strings.
func safeError(err error) string {
	if err == nil {
		return ""
	}
	return fmt.Sprintf("%v", err)
}

// isConsumerNotFoundErr reports whether err indicates a JetStream consumer
// does not exist (name varies by NATS server version).
func isConsumerNotFoundErr(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "consumer not found") ||
		strings.Contains(msg, "no consumer") ||
		strings.Contains(msg, "10094") // NATS API: consumer not found
}
