// Package sse provides a run-scoped server-sent events hub for live run
// progress. Each run has an isolated event buffer and client set; events are
// never broadcast across run IDs or tenants.
package sse

import (
	"strconv"
	"sync"
	"time"
)

// Event is a single SSE lifecycle event sent to clients of a specific run.
type Event struct {
	ID        string    `json:"id"`
	Type      string    `json:"type"` // run.started, event.delivered, consumer.retrying, invariant.failed, run.completed
	RunID     string    `json:"run_id"`
	Data      any       `json:"data,omitempty"`
	Timestamp time.Time `json:"timestamp"`
}

// Lifecycle event types.
const (
	EventRunStarted      = "run.started"
	EventEventDelivered  = "event.delivered"
	EventConsumerRetry   = "consumer.retrying"
	EventInvariantFailed = "invariant.failed"
	EventRunCompleted    = "run.completed"
)

// Hub manages run-scoped SSE event distribution. It is safe for concurrent
// use. Each run maintains a bounded event buffer and an isolated set of
// client channels.
type Hub struct {
	mu        sync.Mutex
	runs      map[string]*runHub
	maxEvents int // max buffered events per run
}

// runHub holds the bounded buffer and client set for a single run.
type runHub struct {
	mu      sync.Mutex
	clients map[chan Event]struct{}
	buffer  []Event // ring buffer of recent events
	maxBuf  int
	head    int    // next write index
	count   int    // number of valid entries
	seq     uint64 // monotonically increasing event sequence
}

// NewHub creates a hub with the given per-run buffer size.
func NewHub(maxEventsPerRun int) *Hub {
	if maxEventsPerRun <= 0 {
		maxEventsPerRun = 100
	}
	return &Hub{
		runs:      make(map[string]*runHub),
		maxEvents: maxEventsPerRun,
	}
}

// getOrCreate returns the runHub for runID, creating it if needed.
func (h *Hub) getOrCreate(runID string) *runHub {
	h.mu.Lock()
	defer h.mu.Unlock()
	rh, ok := h.runs[runID]
	if !ok {
		rh = &runHub{
			clients: make(map[chan Event]struct{}),
			buffer:  make([]Event, h.maxEvents),
			maxBuf:  h.maxEvents,
		}
		h.runs[runID] = rh
	}
	return rh
}

// Publish appends an event to the run's buffer and fans it out to all
// connected clients. Non-blocking: a full client channel is skipped.
func (h *Hub) Publish(runID string, ev Event) {
	rh := h.getOrCreate(runID)

	ev.Timestamp = time.Now().UTC()
	if ev.ID == "" {
		rh.mu.Lock()
		rh.seq++
		ev.ID = strconv.FormatUint(rh.seq, 10)
		rh.mu.Unlock()
	}

	// Append to ring buffer.
	rh.mu.Lock()
	rh.buffer[rh.head] = ev
	rh.head = (rh.head + 1) % rh.maxBuf
	if rh.count < rh.maxBuf {
		rh.count++
	}

	// Snapshot clients under lock, then fan out without holding it.
	clients := make([]chan Event, 0, len(rh.clients))
	for ch := range rh.clients {
		clients = append(clients, ch)
	}
	rh.mu.Unlock()

	for _, ch := range clients {
		select {
		case ch <- ev:
		default: // slow client: drop this event for this client
		}
	}
}

// Subscribe registers a client channel for runID and returns it. If
// lastEventID is non-empty, buffered events after that id are replayed first.
// The caller must call Unsubscribe when the client disconnects.
func (h *Hub) Subscribe(runID string, lastEventID string) chan Event {
	rh := h.getOrCreate(runID)

	ch := make(chan Event, 16) // client buffer; drops on slow client

	// Collect replay events under lock, send outside the lock to avoid
	// blocking while holding the mutex.
	var replay []Event
	rh.mu.Lock()
	rh.clients[ch] = struct{}{}

	started := false
	if lastEventID != "" {
		for i := 0; i < rh.count; i++ {
			idx := (rh.head - rh.count + i + rh.maxBuf) % rh.maxBuf
			if rh.buffer[idx].ID == lastEventID {
				started = true
				continue
			}
			if started {
				replay = append(replay, rh.buffer[idx])
			}
		}
	}
	rh.mu.Unlock()

	// Send replay events without holding the lock.
	for _, ev := range replay {
		select {
		case ch <- ev:
		default:
		}
	}

	return ch
}

// Unsubscribe removes a client channel from its run hub.
func (h *Hub) Unsubscribe(ch chan Event) {
	h.mu.Lock()
	for _, rh := range h.runs {
		rh.mu.Lock()
		delete(rh.clients, ch)
		rh.mu.Unlock()
	}
	h.mu.Unlock()
}

// Close shuts down all run hubs and closes every client channel.
func (h *Hub) Close() {
	h.mu.Lock()
	for _, rh := range h.runs {
		rh.mu.Lock()
		for ch := range rh.clients {
			close(ch)
		}
		rh.clients = make(map[chan Event]struct{})
		rh.mu.Unlock()
	}
	h.runs = make(map[string]*runHub)
	h.mu.Unlock()
}

// nextID generates a best-effort unique id without crypto/rand to keep this
// package dependency-light. Collisions are extremely unlikely in practice.
func nextID() string {
	return time.Now().UTC().Format("15:04:05.000000")
}
