-- GenesisGo run lifecycle columns and transition audit (revert).
-- +goose Down
-- +goose StatementBegin

DROP INDEX IF EXISTS idx_run_transitions_created_at;
DROP INDEX IF EXISTS idx_run_transitions_run_id;
DROP TABLE IF EXISTS run_transitions;

ALTER TABLE runs
	DROP COLUMN failure_reason,
	DROP COLUMN cancellation_reason,
	DROP COLUMN attempt_count,
	DROP COLUMN version;

-- +goose StatementEnd
