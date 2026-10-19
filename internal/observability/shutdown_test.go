package observability

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type fakeTracker struct {
	mu        sync.Mutex
	active    []string
	cancelled []string
	release   chan struct{}
	waitErr   error
}

func newFakeTracker(active ...string) *fakeTracker {
	return &fakeTracker{active: append([]string(nil), active...), release: make(chan struct{})}
}

func (f *fakeTracker) ActiveRuns() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.active...)
}

func (f *fakeTracker) CancelRun(runID string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.cancelled = append(f.cancelled, runID)
}

func (f *fakeTracker) Wait(ctx context.Context) error {
	if f.waitErr != nil {
		return f.waitErr
	}
	select {
	case <-f.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (f *fakeTracker) finish() {
	select {
	case <-f.release:
	default:
		close(f.release)
	}
}

func cancelledIDs(f *fakeTracker) []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.cancelled...)
}

func TestShutdownDrainsAndCheckpointActiveRuns(t *testing.T) {
	tracker := newFakeTracker("run-1", "run-2")
	go func() {
		time.Sleep(20 * time.Millisecond)
		tracker.finish()
	}()

	var checkpointed bool
	err := Shutdown(context.Background(), tracker, ShutdownConfig{Timeout: 2 * time.Second}, nil,
		func(context.Context) { checkpointed = true },
		func(context.Context) error { return nil },
	)
	if err != nil {
		t.Fatalf("expected clean shutdown, got %v", err)
	}
	if !checkpointed {
		t.Fatal("expected terminal run states to be checkpointed")
	}
	got := cancelledIDs(tracker)
	if len(got) != 2 || got[0] != "run-1" || got[1] != "run-2" {
		t.Fatalf("expected both runs to be cancelled, got %v", got)
	}
}

func TestShutdownReleasesResourcesOnTimeout(t *testing.T) {
	tracker := newFakeTracker("run-blocked")
	var closed bool
	start := time.Now()
	err := Shutdown(context.Background(), tracker, ShutdownConfig{Timeout: 50 * time.Millisecond}, nil,
		func(context.Context) {},
		func(context.Context) error { closed = true; return nil },
	)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected deadline error, got %v", err)
	}
	if !closed {
		t.Fatal("server resources must still be released on timeout")
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("shutdown must stay bounded, took %s", elapsed)
	}
}

func TestShutdownWithNoTrackedWork(t *testing.T) {
	var checkpointed bool
	err := Shutdown(context.Background(), nil, ShutdownConfig{}, nil,
		func(context.Context) { checkpointed = true },
		func(context.Context) error { return nil },
	)
	if err != nil {
		t.Fatalf("expected clean shutdown, got %v", err)
	}
	if !checkpointed {
		t.Fatal("checkpoint hook must still run")
	}
}

func TestShutdownNeverMarksInterruptedRunsPassed(t *testing.T) {
	tracker := newFakeTracker("run-interrupted")
	checkpointStates := map[string]string{"run-interrupted": "running"}
	go func() {
		time.Sleep(20 * time.Millisecond)
		tracker.finish()
	}()

	err := Shutdown(context.Background(), tracker, ShutdownConfig{Timeout: 2 * time.Second}, nil,
		func(context.Context) { checkpointStates["run-interrupted"] = "cancelled" },
		func(context.Context) error { return nil },
	)
	if err != nil {
		t.Fatalf("expected clean shutdown, got %v", err)
	}
	if checkpointStates["run-interrupted"] == "passed" {
		t.Fatal("interrupted runs must never checkpoint as passed")
	}
	if checkpointStates["run-interrupted"] != "cancelled" {
		t.Fatalf("expected cancelled terminal state, got %q", checkpointStates["run-interrupted"])
	}
}

func TestShutdownClosesAllResourcesInOrder(t *testing.T) {
	var order []string
	closers := []Closer{
		{Name: "http_server", Close: func(context.Context) error { order = append(order, "http_server"); return nil }},
		{Name: "database", Close: func(context.Context) error { order = append(order, "database"); return nil }},
		{Name: "nats", Close: func(context.Context) error { order = append(order, "nats"); return nil }},
		{Name: "telemetry", Close: func(context.Context) error { order = append(order, "telemetry"); return nil }},
	}
	err := ShutdownWithClosers(context.Background(), nil, ShutdownConfig{}, nil, nil, closers)
	if err != nil {
		t.Fatalf("expected clean shutdown, got %v", err)
	}
	want := []string{"http_server", "database", "nats", "telemetry"}
	if len(order) != len(want) {
		t.Fatalf("expected %v, got %v", want, order)
	}
	for i := range want {
		if order[i] != want[i] {
			t.Fatalf("expected %v, got %v", want, order)
		}
	}
}
