-- GenesisGo rejected event audit table (revert).
-- +goose Down
-- +goose StatementBegin

DROP INDEX IF EXISTS idx_rejected_events_received_at;
DROP INDEX IF EXISTS idx_rejected_events_run_id;
DROP TABLE IF EXISTS rejected_events;

-- +goose StatementEnd
