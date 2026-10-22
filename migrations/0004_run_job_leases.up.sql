-- GenesisGo run job leases and heartbeat columns.
-- +goose Up
-- +goose StatementBegin

-- Lease columns let a worker atomically claim a run job and renew its lease
-- while executing. A worker that crashes or is interrupted leaves an expired
-- lease behind, which RecoverExpiredLeases resets back to queued so the job
-- can be recovered by another worker instead of being stuck forever.
ALTER TABLE run_jobs
    ADD COLUMN leased_at TIMESTAMPTZ,
    ADD COLUMN lease_owner TEXT,
    ADD COLUMN lease_expires_at TIMESTAMPTZ;

CREATE INDEX idx_run_jobs_lease_expires_at ON run_jobs(lease_expires_at) WHERE status = 'claimed';

-- +goose StatementEnd
