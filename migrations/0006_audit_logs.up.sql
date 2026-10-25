-- GenesisGo audit logs and retention support.
-- +goose Up
-- +goose StatementBegin

CREATE TABLE audit_logs (
    id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id     UUID REFERENCES runs(id) ON DELETE SET NULL,
    action     TEXT NOT NULL,
    actor_key_id TEXT,
    detail     TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_audit_logs_run_id ON audit_logs(run_id);
CREATE INDEX idx_audit_logs_created_at ON audit_logs(created_at DESC);

COMMENT ON COLUMN audit_logs.actor_key_id IS 'API key id of the authenticated actor, never the secret.';
COMMENT ON COLUMN audit_logs.detail IS 'Redacted summary only; never credentials or complete request bodies.';

-- +goose StatementEnd