-- GenesisGo initial schema.
-- +goose Up
-- +goose StatementBegin

-- pgcrypto provides gen_random_uuid() on PostgreSQL < 13 and is harmless on
-- newer versions where the function is built-in.
CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TYPE run_status AS ENUM (
	'pending',
	'running',
	'completed',
	'failed',
	'cancelled'
);

-- A predefined controlled fault scenario (duplicate delivery, out-of-order
-- delivery, retry, crash-after-commit, worker restart, ...).
CREATE TABLE scenarios (
	id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
	key         TEXT NOT NULL UNIQUE,
	name        TEXT NOT NULL,
	description TEXT,
	created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
	updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- A single execution of a scenario. All child tables are scoped by run_id.
CREATE TABLE runs (
	id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
	scenario_id  UUID NOT NULL REFERENCES scenarios(id) ON DELETE CASCADE,
	status       run_status NOT NULL DEFAULT 'pending',
	created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
	started_at   TIMESTAMPTZ,
	completed_at TIMESTAMPTZ,
	updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- A bounded unit of work scheduled by the orchestrator for a run.
CREATE TABLE run_jobs (
	id        UUID PRIMARY KEY DEFAULT gen_random_uuid(),
	run_id    UUID NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
	job_type  TEXT NOT NULL,
	status    TEXT NOT NULL DEFAULT 'queued',
	payload   JSONB,
	created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
	updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- A single delivery attempt of an event to the sample consumer.
CREATE TABLE event_attempts (
	id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
	run_id           UUID NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
	event_id         TEXT NOT NULL,
	attempt_number   INTEGER NOT NULL DEFAULT 1,
	delivered_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
	acknowledged_at  TIMESTAMPTZ,
	fault_injected   TEXT,
	created_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- A business effect written by the consumer (payment, inventory deduction, ...).
CREATE TABLE side_effect_records (
	id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
	run_id          UUID NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
	effect_type     TEXT NOT NULL,
	aggregate_id    TEXT NOT NULL,
	idempotency_key TEXT,
	amount_cents    BIGINT,
	currency        TEXT NOT NULL DEFAULT 'USD',
	recorded_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- A deterministic business invariant verdict for a run.
CREATE TABLE invariant_results (
	id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
	run_id         UUID NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
	invariant_key  TEXT NOT NULL,
	verdict        TEXT NOT NULL,
	details        JSONB,
	evaluated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- An optional AI-assisted diagnosis report (one per run, never the verdict).
CREATE TABLE diagnosis_reports (
	id        UUID PRIMARY KEY DEFAULT gen_random_uuid(),
	run_id    UUID NOT NULL UNIQUE REFERENCES runs(id) ON DELETE CASCADE,
	summary   TEXT,
	suggestions JSONB,
	model     TEXT,
	created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Indexes for run lookup, status filtering, and per-run evidence retrieval.
CREATE INDEX idx_runs_scenario_id ON runs(scenario_id);
CREATE INDEX idx_runs_status ON runs(status);
CREATE INDEX idx_runs_created_at ON runs(created_at DESC);

CREATE INDEX idx_run_jobs_run_id ON run_jobs(run_id);
CREATE INDEX idx_run_jobs_run_id_status ON run_jobs(run_id, status);

CREATE INDEX idx_event_attempts_run_id ON event_attempts(run_id);
CREATE INDEX idx_event_attempts_event_id ON event_attempts(event_id);

CREATE INDEX idx_side_effect_records_run_id ON side_effect_records(run_id);

CREATE INDEX idx_invariant_results_run_id ON invariant_results(run_id);

-- +goose StatementEnd
