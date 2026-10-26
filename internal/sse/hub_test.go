package sse

import (
	"sync"
	"testing"
	"time"
)

func TestHubPublishAndSubscribe(t *testing.T) {
	hub := NewHub(10)
	defer hub.Close()

	ch := hub.Subscribe("run-1", "")

	hub.Publish("run-1", Event{
		Type:  EventRunStarted,
		RunID: "run-1",
		Data:  map[string]string{"status": "queued"},
	})

	select {
	case ev := <-ch:
		if ev.Type != EventRunStarted {
			t.Fatalf("expected %s, got %s", EventRunStarted, ev.Type)
		}
		if ev.RunID != "run-1" {
			t.Fatalf("expected run-1, got %s", ev.RunID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for event")
	}
}

func TestHubRunIsolation(t *testing.T) {
	hub := NewHub(10)
	defer hub.Close()

	ch1 := hub.Subscribe("run-1", "")
	ch2 := hub.Subscribe("run-2", "")

	hub.Publish("run-1", Event{Type: EventRunStarted, RunID: "run-1"})

	// run-1 should receive the event
	select {
	case ev := <-ch1:
		if ev.RunID != "run-1" {
			t.Fatalf("expected run-1, got %s", ev.RunID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for event on run-1")
	}

	// run-2 should NOT receive run-1's event
	select {
	case ev := <-ch2:
		t.Fatalf("run-2 should not receive run-1's event, got %v", ev)
	case <-time.After(100 * time.Millisecond):
		// expected - no event
	}
}

func TestHubDisconnectCleanup(t *testing.T) {
	hub := NewHub(10)
	defer hub.Close()

	ch := hub.Subscribe("run-1", "")
	hub.Unsubscribe(ch)

	hub.Publish("run-1", Event{Type: EventRunStarted, RunID: "run-1"})

	// After unsubscribe, no event should arrive
	select {
	case ev := <-ch:
		t.Fatalf("should not receive event after unsubscribe, got %v", ev)
	case <-time.After(100 * time.Millisecond):
		// expected
	}
}

func TestHubBoundedBuffer(t *testing.T) {
	hub := NewHub(3)
	defer hub.Close()

	// Publish 3 events that fit in the buffer.
	hub.Publish("run-1", Event{ID: "e1", Type: EventEventDelivered, RunID: "run-1", Data: 0})
	hub.Publish("run-1", Event{ID: "e2", Type: EventEventDelivered, RunID: "run-1", Data: 1})
	hub.Publish("run-1", Event{ID: "e3", Type: EventEventDelivered, RunID: "run-1", Data: 2})

	// Replay from e1 should return e2, e3 (2 events).
	ch := hub.Subscribe("run-1", "e1")

	received := []string{}
	for i := 0; i < 2; i++ {
		select {
		case ev := <-ch:
			received = append(received, ev.ID)
		case <-time.After(2 * time.Second):
			break
		}
	}
	if len(received) != 2 {
		t.Fatalf("expected 2 buffered events, got %d: %v", len(received), received)
	}
	if received[0] != "e2" || received[1] != "e3" {
		t.Fatalf("expected [e2 e3], got %v", received)
	}
}

func TestHubConcurrentPublish(t *testing.T) {
	hub := NewHub(100)
	defer hub.Close()

	ch := hub.Subscribe("run-1", "")

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			hub.Publish("run-1", Event{Type: EventEventDelivered, RunID: "run-1", Data: i})
		}(i)
	}
	wg.Wait()

	// Client buffer is 16; non-blocking fan-out may drop events for a slow
	// reader. Drain what arrives and verify at least one event came through.
	time.Sleep(100 * time.Millisecond)
	received := 0
	for {
		select {
		case <-ch:
			received++
		default:
			goto done
		}
	}
done:
	if received == 0 {
		t.Fatal("expected at least one event from concurrent publish")
	}
}

func TestHubSlowClientDoesNotBlock(t *testing.T) {
	hub := NewHub(10)
	defer hub.Close()

	// Client with full buffer (size 1)
	ch := make(chan Event, 1)
	rh := hub.getOrCreate("run-1")
	rh.mu.Lock()
	rh.clients[ch] = struct{}{}
	rh.mu.Unlock()

	// Fill the client buffer
	ch <- Event{Type: EventRunStarted, RunID: "run-1"}

	// Publish more events - slow client drops them, hub should not block
	hub.Publish("run-1", Event{Type: EventEventDelivered, RunID: "run-1"})
	hub.Publish("run-1", Event{Type: EventRunCompleted, RunID: "run-1"})

	// Remove slow client
	rh.mu.Lock()
	delete(rh.clients, ch)
	rh.mu.Unlock()
	close(ch)

	// Subscribe a new client and publish - should receive events
	ch2 := hub.Subscribe("run-1", "")
	hub.Publish("run-1", Event{Type: EventRunCompleted, RunID: "run-1"})
	select {
	case ev := <-ch2:
		if ev.Type != EventRunCompleted {
			t.Fatalf("expected %s, got %s", EventRunCompleted, ev.Type)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for event on new client")
	}
}
