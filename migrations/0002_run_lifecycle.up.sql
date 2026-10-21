-- GenesisGo run lifecycle columns and transition audit.
-- +goose Up
-- +goose StatementBegin

ALTER TABLE runs
	ADD COLUMN failure_reason TEXT,
	ADD COLUMN cancellation_reason TEXT,
	ADD COLUMN attempt_count INTEGER NOT NULL DEFAULT 0,
	ADD COLUMN version INTEGER NOT NULL DEFAULT 1;

-- run_transitions records every status change so execution history can be
-- reconstructed for any run. It is append-only; rows are never updated or
-- deleted once written.
CREATE TABLE run_transitions (
	id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
	run_id     UUID NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
	from_status run_status NOT NULL,
	to_status   run_status NOT NULL,
	reason      TEXT,
	created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_run_transitions_run_id ON run_transitions(run_id);
CREATE INDEX idx_run_transitions_created_at ON run_transitions(created_at DESC);

-- +goose StatementEnd
