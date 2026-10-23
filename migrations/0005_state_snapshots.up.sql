-- GenesisGo state snapshots (add).
-- +goose Up
-- +goose StatementBegin

-- state_snapshots capture safe before/after state summaries for each
-- event delivery so a completed run can be reconstructed from stored
-- records. Only non-sensitive summary data is stored; full raw payloads
-- are intentionally omitted.
CREATE TABLE state_snapshots (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id        UUID NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    step_id       TEXT NOT NULL,
    event_id      TEXT NOT NULL,
    snapshot_type TEXT NOT NULL CHECK (snapshot_type IN ('before', 'after')),
    aggregate_id  TEXT NOT NULL,
    state_summary JSONB NOT NULL DEFAULT '{}',
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_state_snapshots_run_id ON state_snapshots(run_id);
CREATE INDEX idx_state_snapshots_run_id_type ON state_snapshots(run_id, snapshot_type);

-- +goose StatementEnd
