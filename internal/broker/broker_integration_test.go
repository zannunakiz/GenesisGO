//go:build integration

package broker

import (
	"context"
	"errors"
	"os"
	"sync"
	"testing"
	"time"
)

func brokerFromEnv(t *testing.T) Broker {
	t.Helper()
	url := os.Getenv("NATS_URL")
	if url == "" {
		t.Skip("NATS_URL is not set; integration test requires a live NATS server")
	}
	b, err := New(Config{
		URL:           url,
		Stream:        "genesisgo-it-events",
		SubjectPrefix: "gg.it",
		MaxDeliver:    5,
		MaxAckPending: 100,
	})
	if err != nil {
		t.Skipf("NATS broker is not reachable: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	return b
}

func TestIntegrationPublishConsumeAck(t *testing.T) {
	b := brokerFromEnv(t)
	runID := "integration-run-ack"
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	var received sync.WaitGroup
	received.Add(1)
	var gotPayload []byte
	var acked bool

	err := b.Subscribe(ctx, runID, func(ctx context.Context, subject string, payload []byte, ack Ack) error {
		gotPayload = payload
		if err := ack.Ack(); err != nil {
			return err
		}
		acked = true
		received.Done()
		return nil
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	payload := []byte(`{"event_id":"evt-1","run_id":"` + runID + `"}`)
	if err := b.Publish(ctx, runID, payload); err != nil {
		t.Fatalf("publish: %v", err)
	}

	done := make(chan struct{})
	go func() { received.Wait(); close(done) }()
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatalf("timed out waiting for message; acked=%v", acked)
	}
	if !acked {
		t.Fatal("expected message to be acknowledged")
	}
	if string(gotPayload) != string(payload) {
		t.Fatalf("payload mismatch: got %q", gotPayload)
	}
}

func TestIntegrationRedeliveryTerminatesAtMaxDeliver(t *testing.T) {
	b := brokerFromEnv(t)
	runID := "integration-run-redeliver"
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()

	var mu sync.Mutex
	deliveries := 0

	err := b.Subscribe(ctx, runID, func(ctx context.Context, subject string, payload []byte, ack Ack) error {
		mu.Lock()
		deliveries++
		mu.Unlock()
		// Always fail so the broker negative-acknowledges and JetStream
		// redelivers until MaxDeliver is reached.
		return errors.New("force redelivery")
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	if err := b.Publish(ctx, runID, []byte("{}")); err != nil {
		t.Fatalf("publish: %v", err)
	}

	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		n := deliveries
		mu.Unlock()
		if n >= 5 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	mu.Lock()
	n := deliveries
	mu.Unlock()
	if n < 2 {
		t.Fatalf("expected redelivery (>=2 deliveries), got %d", n)
	}
	if n > 5 {
		t.Fatalf("expected deliveries to stop at MaxDeliver (5), got %d", n)
	}
}

func TestIntegrationRunIsolation(t *testing.T) {
	b := brokerFromEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	runA := "integration-run-isolate-a"
	runB := "integration-run-isolate-b"

	var mu sync.Mutex
	receivedA := false
	receivedB := false

	if err := b.Subscribe(ctx, runA, func(ctx context.Context, subject string, payload []byte, ack Ack) error {
		mu.Lock()
		receivedA = true
		mu.Unlock()
		return ack.Ack()
	}); err != nil {
		t.Fatalf("subscribe run-a: %v", err)
	}
	if err := b.Subscribe(ctx, runB, func(ctx context.Context, subject string, payload []byte, ack Ack) error {
		mu.Lock()
		receivedB = true
		mu.Unlock()
		return ack.Ack()
	}); err != nil {
		t.Fatalf("subscribe run-b: %v", err)
	}

	// Publish to run-a only; run-b's consumer must not observe it.
	if err := b.Publish(ctx, runA, []byte("run-a-payload")); err != nil {
		t.Fatalf("publish run-a: %v", err)
	}

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		a := receivedA
		mu.Unlock()
		if a {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	mu.Lock()
	a, bb := receivedA, receivedB
	mu.Unlock()
	if !a {
		t.Fatal("expected run-a consumer to receive run-a's message")
	}
	if bb {
		t.Fatal("expected run isolation: run-b consumer must not receive run-a's message")
	}
}
