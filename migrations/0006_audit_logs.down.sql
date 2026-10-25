-- GenesisGo audit logs and retention support.
-- +goose Down
-- +goose StatementBegin

DROP INDEX IF EXISTS idx_audit_logs_created_at;
DROP INDEX IF EXISTS idx_audit_logs_run_id;
DROP TABLE IF EXISTS audit_logs;

-- +goose StatementEnd