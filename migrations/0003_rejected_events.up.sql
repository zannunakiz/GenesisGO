-- GenesisGo rejected event audit table.
-- +goose Up
-- +goose StatementBegin

CREATE TABLE rejected_events (
	id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
	run_id      UUID NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
	event_id    TEXT NOT NULL,
	event_type  TEXT NOT NULL,
	reason      TEXT NOT NULL,
	received_at TIMESTAMPTZ NOT NULL DEFAULT now(),
	envelope    JSONB
);

CREATE INDEX idx_rejected_events_run_id ON rejected_events(run_id);
CREATE INDEX idx_rejected_events_received_at ON rejected_events(received_at DESC);

-- +goose StatementEnd
