package delivery

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/zannunakiz/GenesisGO/internal/broker"
	"github.com/zannunakiz/GenesisGO/internal/events"
	"github.com/zannunakiz/GenesisGO/internal/store"
)

// fakeBroker captures publishes and exposes subscribers to simulate broker
// delivery without a live NATS server.
type fakeBroker struct {
	mu           sync.Mutex
	published    map[string][][]byte
	subscribers  map[string]broker.HandlerFunc
	started      bool
	closed       bool
	publishErr   error
	subscribeErr error
}

func newFakeBroker() *fakeBroker {
	return &fakeBroker{
		published:   make(map[string][][]byte),
		subscribers: make(map[string]broker.HandlerFunc),
	}
}

func (f *fakeBroker) Publish(_ context.Context, runID string, payload []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.publishErr != nil {
		return f.publishErr
	}
	f.published[runID] = append(f.published[runID], append([]byte{}, payload...))
	return nil
}

func (f *fakeBroker) Subscribe(_ context.Context, runID string, handler broker.HandlerFunc) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.subscribeErr != nil {
		return f.subscribeErr
	}
	if f.closed {
		return broker.ErrNotStarted
	}
	f.subscribers[runID] = handler
	return nil
}

// Deliver simulates the broker delivering a payload to a run's subscriber.
func (f *fakeBroker) Deliver(runID string, payload []byte) error {
	f.mu.Lock()
	handler := f.subscribers[runID]
	f.mu.Unlock()
	if handler == nil {
		return errors.New("no subscriber for run")
	}
	ack := &fakeAck{}
	return handler(context.Background(), "", payload, ack)
}

func (f *fakeBroker) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return broker.ErrAlreadyClosed
	}
	f.closed = true
	return nil
}

func (f *fakeBroker) DeleteConsumer(_ context.Context, _ string) error {
	return nil
}

func (f *fakeBroker) Ready() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.started && !f.closed
}

func (f *fakeBroker) PublishedCount(runID string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.published[runID])
}

func (f *fakeBroker) LastPublished(runID string) []byte {
	f.mu.Lock()
	defer f.mu.Unlock()
	msgs := f.published[runID]
	if len(msgs) == 0 {
		return nil
	}
	return append([]byte{}, msgs[len(msgs)-1]...)
}

type fakeAck struct {
	mu    sync.Mutex
	acked bool
	naked bool
	term  bool
}

func (a *fakeAck) Ack() error  { a.mu.Lock(); defer a.mu.Unlock(); a.acked = true; return nil }
func (a *fakeAck) Nak() error  { a.mu.Lock(); defer a.mu.Unlock(); a.naked = true; return nil }
func (a *fakeAck) Term() error { a.mu.Lock(); defer a.mu.Unlock(); a.term = true; return nil }

// fakeStore captures attempt records.
type fakeStore struct {
	mu       sync.Mutex
	attempts []*store.EventAttempt
}

func (f *fakeStore) CreateEventAttempt(_ context.Context, runID, eventID string, attemptNumber int32, deliveredAt time.Time, acknowledgedAt *time.Time, faultInjected *string) (*store.EventAttempt, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	a := &store.EventAttempt{
		ID:             "attempt-1",
		RunID:          runID,
		EventID:        eventID,
		AttemptNumber:  attemptNumber,
		DeliveredAt:    deliveredAt,
		AcknowledgedAt: acknowledgedAt,
		FaultInjected:  faultInjected,
		CreatedAt:      time.Now().UTC(),
	}
	f.attempts = append(f.attempts, a)
	return a, nil
}

func (f *fakeStore) attemptsFor(runID, eventID string) []*store.EventAttempt {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []*store.EventAttempt
	for _, a := range f.attempts {
		if a.RunID == runID && a.EventID == eventID {
			out = append(out, a)
		}
	}
	return out
}

func envBytes(t *testing.T, runID, eventID string, payload []byte) []byte {
	t.Helper()
	env := &events.EventEnvelope{
		EventID:       eventID,
		EventType:     events.EventTypePayment,
		SchemaVersion: events.SchemaVersion,
		OccurredAt:    time.Now().UTC(),
		RunID:         runID,
		Payload:       payload,
	}
	data, err := json.Marshal(env)
	if err != nil {
		t.Fatalf("marshal envelope: %v", err)
	}
	return data
}

func paymentPayload(aggregate, idem string, amount int64) []byte {
	return []byte(`{"type":"payment.succeeded","aggregate_id":"` + aggregate + `","amount_cents":` + itoa(amount) + `,"currency":"USD","idempotency_key":"` + idem + `"}`)
}

func itoa(n int64) string {
	if n == 0 {
		return "0"
	}
	var b []byte
	for n > 0 {
		b = append([]byte{byte('0' + n%10)}, b...)
		n /= 10
	}
	return string(b)
}
func TestNewPublisherRejectsMissingDeps(t *testing.T) {
	_, err := NewPublisher(Config{})
	if err == nil {
		t.Fatal("expected error for missing broker")
	}
	_, err = NewPublisher(Config{Broker: newFakeBroker()})
	if err == nil {
		t.Fatal("expected error for missing recorder")
	}
	_, err = NewPublisher(Config{Recorder: &fakeStore{}})
	if err == nil {
		t.Fatal("expected error for missing broker")
	}
	_, err = NewPublisher(Config{Broker: newFakeBroker(), Recorder: &fakeStore{}})
	if err == nil {
		t.Fatal("expected error for empty run id")
	}
}

func TestPublisherRecordsFirstAttempt(t *testing.T) {
	fb := newFakeBroker()
	fs := &fakeStore{}
	pub, err := NewPublisher(Config{Broker: fb, Recorder: fs, RunID: "run-1"})
	if err != nil {
		t.Fatalf("new publisher: %v", err)
	}
	payload := paymentPayload("order-100", "pay-100", 1000)
	if err := pub.Publish(context.Background(), "evt-1", payload); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if fb.PublishedCount("run-1") != 1 {
		t.Fatalf("expected 1 publish, got %d", fb.PublishedCount("run-1"))
	}
	published := fb.LastPublished("run-1")
	var env events.EventEnvelope
	if err := json.Unmarshal(published, &env); err != nil {
		t.Fatalf("published message is not an envelope: %v", err)
	}
	if env.EventID != "evt-1" || env.RunID != "run-1" {
		t.Fatalf("envelope mismatch: %+v", env)
	}
	as := fs.attemptsFor("run-1", "evt-1")
	if len(as) != 1 {
		t.Fatalf("expected 1 attempt, got %d", len(as))
	}
	if as[0].AttemptNumber != 1 {
		t.Fatalf("expected attempt 1, got %d", as[0].AttemptNumber)
	}
	if as[0].AcknowledgedAt != nil {
		t.Fatalf("expected no acknowledgement on publish, got %v", as[0].AcknowledgedAt)
	}
	if as[0].DeliveredAt.IsZero() {
		t.Fatal("expected delivered_at to be set")
	}
}

func TestPublisherRejectsEmptyEventID(t *testing.T) {
	pub, err := NewPublisher(Config{Broker: newFakeBroker(), Recorder: &fakeStore{}, RunID: "run-1"})
	if err != nil {
		t.Fatalf("new publisher: %v", err)
	}
	if err := pub.Publish(context.Background(), "", []byte("{}")); err == nil {
		t.Fatal("expected error for empty event id")
	}
}

func TestNewConsumerRejectsMissingDeps(t *testing.T) {
	_, err := NewConsumer(Config{})
	if err == nil {
		t.Fatal("expected error for missing broker")
	}
	_, err = NewConsumer(Config{Broker: newFakeBroker()})
	if err == nil {
		t.Fatal("expected error for missing recorder")
	}
	_, err = NewConsumer(Config{Recorder: &fakeStore{}})
	if err == nil {
		t.Fatal("expected error for missing broker")
	}
}

func TestConsumerAcksOnSuccess(t *testing.T) {
	fb := newFakeBroker()
	fs := &fakeStore{}
	consumer, err := NewConsumer(Config{Broker: fb, Recorder: fs, RunID: "run-1", MaxAttempts: 3})
	if err != nil {
		t.Fatalf("new consumer: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := consumer.Consume(ctx, func(ctx context.Context, subject string, env *events.EventEnvelope, attempt int32, ack Ack) error {
		if attempt != 1 {
			return errors.New("unexpected attempt")
		}
		return nil // success -> ack
	}); err != nil {
		t.Fatalf("consume: %v", err)
	}

	if err := fb.Deliver("run-1", envBytes(t, "run-1", "evt-1", paymentPayload("order-100", "pay-100", 1000))); err != nil {
		t.Fatalf("deliver: %v", err)
	}

	as := fs.attemptsFor("run-1", "evt-1")
	if len(as) != 1 {
		t.Fatalf("expected 1 attempt, got %d", len(as))
	}
	if as[0].AttemptNumber != 1 {
		t.Fatalf("expected attempt 1, got %d", as[0].AttemptNumber)
	}
	if as[0].AcknowledgedAt == nil {
		t.Fatal("expected acknowledged_at to be set on success")
	}
	if as[0].FaultInjected != nil {
		t.Fatalf("expected no fault, got %v", *as[0].FaultInjected)
	}
}

func TestConsumerNacksAndRedeliversOnFailure(t *testing.T) {
	fb := newFakeBroker()
	fs := &fakeStore{}
	consumer, err := NewConsumer(Config{Broker: fb, Recorder: fs, RunID: "run-1", MaxAttempts: 3})
	if err != nil {
		t.Fatalf("new consumer: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var mu sync.Mutex
	calls := 0
	if err := consumer.Consume(ctx, func(ctx context.Context, subject string, env *events.EventEnvelope, attempt int32, ack Ack) error {
		mu.Lock()
		calls++
		n := calls
		mu.Unlock()
		if n == 1 {
			return errors.New("transient failure") // fail -> nack, redeliver
		}
		return nil // succeed on retry -> ack
	}); err != nil {
		t.Fatalf("consume: %v", err)
	}

	payload := envBytes(t, "run-1", "evt-1", paymentPayload("order-100", "pay-100", 1000))
	if err := fb.Deliver("run-1", payload); err != nil {
		t.Fatalf("deliver #1: %v", err)
	}
	if err := fb.Deliver("run-1", payload); err != nil {
		t.Fatalf("deliver #2: %v", err)
	}

	as := fs.attemptsFor("run-1", "evt-1")
	if len(as) != 2 {
		t.Fatalf("expected 2 attempts, got %d", len(as))
	}
	if as[0].AttemptNumber != 1 || as[0].AcknowledgedAt != nil {
		t.Fatalf("attempt 1 should not be acknowledged: %+v", as[0])
	}
	if as[1].AttemptNumber != 2 || as[1].AcknowledgedAt == nil {
		t.Fatalf("attempt 2 should be acknowledged: %+v", as[1])
	}
}

func TestConsumerTermsAtMaxAttempts(t *testing.T) {
	fb := newFakeBroker()
	fs := &fakeStore{}
	consumer, err := NewConsumer(Config{Broker: fb, Recorder: fs, RunID: "run-1", MaxAttempts: 2})
	if err != nil {
		t.Fatalf("new consumer: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := consumer.Consume(ctx, func(ctx context.Context, subject string, env *events.EventEnvelope, attempt int32, ack Ack) error {
		return errors.New("always fails") // always fails -> nack then term at max
	}); err != nil {
		t.Fatalf("consume: %v", err)
	}

	payload := envBytes(t, "run-1", "evt-1", paymentPayload("order-100", "pay-100", 1000))
	// JetStream redelivers at most MaxDeliver times; after Term at max it stops,
	// so the consumer observes exactly maxAttempts deliveries.
	for i := 0; i < 2; i++ {
		if err := fb.Deliver("run-1", payload); err != nil {
			t.Fatalf("deliver #%d: %v", i+1, err)
		}
	}

	as := fs.attemptsFor("run-1", "evt-1")
	if len(as) != 2 {
		t.Fatalf("expected exactly 2 attempts (max), got %d", len(as))
	}
	if as[0].AttemptNumber != 1 || as[0].AcknowledgedAt != nil {
		t.Fatalf("attempt 1 should not be acknowledged: %+v", as[0])
	}
	if as[1].AttemptNumber != 2 || as[1].AcknowledgedAt != nil {
		t.Fatalf("attempt 2 (max) should be terminated, not acknowledged: %+v", as[1])
	}
}

func TestConsumerExplicitAckSuppressedAutoAck(t *testing.T) {
	fb := newFakeBroker()
	fs := &fakeStore{}
	consumer, err := NewConsumer(Config{Broker: fb, Recorder: fs, RunID: "run-1", MaxAttempts: 3})
	if err != nil {
		t.Fatalf("new consumer: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := consumer.Consume(ctx, func(ctx context.Context, subject string, env *events.EventEnvelope, attempt int32, ack Ack) error {
		if err := ack.Ack(); err != nil {
			return err
		}
		return errors.New("error after explicit ack") // must not auto-nack
	}); err != nil {
		t.Fatalf("consume: %v", err)
	}
	if err := fb.Deliver("run-1", envBytes(t, "run-1", "evt-1", paymentPayload("order-100", "pay-100", 1000))); err != nil {
		t.Fatalf("deliver: %v", err)
	}
	as := fs.attemptsFor("run-1", "evt-1")
	if len(as) != 1 {
		t.Fatalf("expected 1 attempt (explicit ack prevents redelivery), got %d", len(as))
	}
	if as[0].AcknowledgedAt == nil {
		t.Fatal("expected acknowledged_at set after explicit ack")
	}
}

func TestConsumerRecordsFaultError(t *testing.T) {
	fb := newFakeBroker()
	fs := &fakeStore{}
	consumer, err := NewConsumer(Config{Broker: fb, Recorder: fs, RunID: "run-1", MaxAttempts: 2})
	if err != nil {
		t.Fatalf("new consumer: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	fault := "retry"
	if err := consumer.Consume(ctx, func(ctx context.Context, subject string, env *events.EventEnvelope, attempt int32, ack Ack) error {
		return &FaultError{Fault: fault, Err: errors.New("boom")}
	}); err != nil {
		t.Fatalf("consume: %v", err)
	}
	if err := fb.Deliver("run-1", envBytes(t, "run-1", "evt-1", paymentPayload("order-100", "pay-100", 1000))); err != nil {
		t.Fatalf("deliver: %v", err)
	}
	as := fs.attemptsFor("run-1", "evt-1")
	if len(as) != 1 {
		t.Fatalf("expected 1 attempt, got %d", len(as))
	}
	if as[0].FaultInjected == nil || *as[0].FaultInjected != fault {
		t.Fatalf("expected fault %q, got %v", fault, as[0].FaultInjected)
	}
}

func TestConfigFaultInjected(t *testing.T) {
	fb := newFakeBroker()
	fs := &fakeStore{}
	fault := "crash_after_commit"
	consumer, err := NewConsumer(Config{Broker: fb, Recorder: fs, RunID: "run-1", MaxAttempts: 2, FaultInjected: &fault})
	if err != nil {
		t.Fatalf("new consumer: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := consumer.Consume(ctx, func(ctx context.Context, subject string, env *events.EventEnvelope, attempt int32, ack Ack) error {
		return nil
	}); err != nil {
		t.Fatalf("consume: %v", err)
	}
	if err := fb.Deliver("run-1", envBytes(t, "run-1", "evt-1", paymentPayload("order-100", "pay-100", 1000))); err != nil {
		t.Fatalf("deliver: %v", err)
	}
	as := fs.attemptsFor("run-1", "evt-1")
	if len(as) != 1 || as[0].FaultInjected == nil || *as[0].FaultInjected != fault {
		t.Fatalf("expected config fault %q on attempt, got %v", fault, as[0].FaultInjected)
	}
}
