package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/zannunakiz/GenesisGO/internal/events"
)

// RejectedEvent is the store-side view of a rejected event, mirrored from the
// events package for persistence.
type RejectedEvent struct {
	ID         string    `json:"id"`
	RunID      string    `json:"run_id"`
	EventID    string    `json:"event_id"`
	EventType  string    `json:"event_type"`
	Reason     string    `json:"reason"`
	ReceivedAt time.Time `json:"received_at"`
	Envelope   []byte    `json:"envelope,omitempty"`
}

func scanRejectedEvent(row pgx.Row) (*RejectedEvent, error) {
	var r RejectedEvent
	var envelope sql.NullString
	if err := row.Scan(
		&r.ID, &r.RunID, &r.EventID, &r.EventType, &r.Reason, &r.ReceivedAt, &envelope,
	); err != nil {
		return nil, err
	}
	if envelope.Valid {
		r.Envelope = []byte(envelope.String)
	}
	return &r, nil
}

// RecordRejectedEvent persists a rejected event for audit within queryer q.
func RecordRejectedEvent(
	ctx context.Context,
	q queryer,
	runID, eventID, eventType, reason string,
	envelope []byte,
) (*RejectedEvent, error) {
	var env *string
	if envelope != nil {
		s := string(envelope)
		env = &s
	}
	const query = `
		INSERT INTO rejected_events (run_id, event_id, event_type, reason, envelope)
		VALUES ($1, $2, $3, $4, $5)
		RETURNING id, run_id, event_id, event_type, reason, received_at, envelope
	`
	row := q.QueryRow(ctx, query, runID, eventID, eventType, reason, env)
	return scanRejectedEvent(row)
}

func (s *Store) RecordRejectedEvent(
	ctx context.Context,
	runID, eventID, eventType, reason string,
	envelope []byte,
) (*RejectedEvent, error) {
	return RecordRejectedEvent(ctx, s.pool, runID, eventID, eventType, reason, envelope)
}

// ListRejectedEventsByRun returns rejected events for a run, oldest first.
func ListRejectedEventsByRun(ctx context.Context, q queryer, runID string) ([]*RejectedEvent, error) {
	const query = `
		SELECT id, run_id, event_id, event_type, reason, received_at, envelope
		FROM rejected_events
		WHERE run_id = $1
		ORDER BY received_at ASC
	`
	rows, err := q.Query(ctx, query, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*RejectedEvent
	for rows.Next() {
		r, err := scanRejectedEvent(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if out == nil {
		out = []*RejectedEvent{}
	}
	return out, nil
}

func (s *Store) ListRejectedEventsByRun(ctx context.Context, runID string) ([]*RejectedEvent, error) {
	return ListRejectedEventsByRun(ctx, s.pool, runID)
}

// StoreRecorder persists rejected events through the store. It implements
// events.Recorder so the events package can record rejections without knowing
// about PostgreSQL.
type StoreRecorder struct {
	store *Store
}

// NewStoreRecorder builds a StoreRecorder backed by s.
func NewStoreRecorder(s *Store) *StoreRecorder {
	return &StoreRecorder{store: s}
}

// Record serializes the envelope and persists the rejection transactionally.
func (r *StoreRecorder) Record(ctx context.Context, envelope *events.EventEnvelope, reason string) error {
	if envelope == nil {
		return errors.New("envelope is required")
	}
	raw, err := json.Marshal(envelope)
	if err != nil {
		return fmt.Errorf("marshal rejected envelope: %w", err)
	}
	err = r.store.WithTx(ctx, func(tx pgx.Tx) error {
		_, err := RecordRejectedEvent(ctx, tx, envelope.RunID, envelope.EventID, string(envelope.EventType), reason, raw)
		return err
	})
	return err
}

var _ events.Recorder = (*StoreRecorder)(nil)
