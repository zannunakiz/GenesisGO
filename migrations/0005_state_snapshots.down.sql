-- GenesisGo state snapshots (revert).
-- +goose Down
-- +goose StatementBegin

DROP INDEX IF EXISTS idx_state_snapshots_run_id_type;
DROP INDEX IF EXISTS idx_state_snapshots_run_id;
DROP TABLE IF EXISTS state_snapshots;

-- +goose StatementEnd
