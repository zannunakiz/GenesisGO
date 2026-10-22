-- GenesisGo run job leases and heartbeat columns.
-- +goose Down
-- +goose StatementBegin

DROP INDEX IF EXISTS idx_run_jobs_lease_expires_at;
ALTER TABLE run_jobs
    DROP COLUMN IF EXISTS lease_expires_at,
    DROP COLUMN IF EXISTS lease_owner,
    DROP COLUMN IF EXISTS leased_at;

-- +goose StatementEnd
