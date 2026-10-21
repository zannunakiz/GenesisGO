-- Canonical SQL-first queries for GenesisGo run data.
-- Every table is scoped by run_id so evidence from different runs is isolated.

-- name: CreateScenario :one
INSERT INTO scenarios (key, name, description)
VALUES ($1, $2, $3)
RETURNING id, key, name, description, created_at, updated_at;

-- name: GetScenario :one
SELECT id, key, name, description, created_at, updated_at
FROM scenarios
WHERE id = $1;

-- name: ListScenarios :many
SELECT id, key, name, description, created_at, updated_at
FROM scenarios
ORDER BY created_at DESC;

-- name: CreateRun :one
INSERT INTO runs (scenario_id, status)
VALUES ($1, $2)
RETURNING id, scenario_id, status, created_at, started_at, completed_at, updated_at;

-- name: GetRun :one
SELECT id, scenario_id, status, created_at, started_at, completed_at, updated_at
FROM runs
WHERE id = $1;

-- name: UpdateRunStatus :one
UPDATE runs
SET status = $2, updated_at = now()
WHERE id = $1
RETURNING id, scenario_id, status, created_at, started_at, completed_at, updated_at;

-- name: ListRunsByScenario :many
SELECT id, scenario_id, status, created_at, started_at, completed_at, updated_at
FROM runs
WHERE scenario_id = $1
ORDER BY created_at DESC;

-- name: CreateRunJob :one
INSERT INTO run_jobs (run_id, job_type, status, payload)
VALUES ($1, $2, $3, $4)
RETURNING id, run_id, job_type, status, payload, created_at, updated_at;

-- name: ListRunJobsByRun :many
SELECT id, run_id, job_type, status, payload, created_at, updated_at
FROM run_jobs
WHERE run_id = $1
ORDER BY created_at ASC;

-- name: CreateEventAttempt :one
INSERT INTO event_attempts (run_id, event_id, attempt_number, delivered_at, acknowledged_at, fault_injected)
VALUES ($1, $2, $3, $4, $5, $6)
RETURNING id, run_id, event_id, attempt_number, delivered_at, acknowledged_at, fault_injected, created_at;

-- name: ListEventAttemptsByRun :many
SELECT id, run_id, event_id, attempt_number, delivered_at, acknowledged_at, fault_injected, created_at
FROM event_attempts
WHERE run_id = $1
ORDER BY created_at ASC;

-- name: CreateSideEffectRecord :one
INSERT INTO side_effect_records (run_id, effect_type, aggregate_id, idempotency_key, amount_cents, currency)
VALUES ($1, $2, $3, $4, $5, $6)
RETURNING id, run_id, effect_type, aggregate_id, idempotency_key, amount_cents, currency, recorded_at;

-- name: ListSideEffectRecordsByRun :many
SELECT id, run_id, effect_type, aggregate_id, idempotency_key, amount_cents, currency, recorded_at
FROM side_effect_records
WHERE run_id = $1
ORDER BY recorded_at ASC;

-- name: CreateInvariantResult :one
INSERT INTO invariant_results (run_id, invariant_key, verdict, details)
VALUES ($1, $2, $3, $4)
RETURNING id, run_id, invariant_key, verdict, details, evaluated_at;

-- name: ListInvariantResultsByRun :many
SELECT id, run_id, invariant_key, verdict, details, evaluated_at
FROM invariant_results
WHERE run_id = $1
ORDER BY evaluated_at ASC;

-- name: CreateDiagnosisReport :one
INSERT INTO diagnosis_reports (run_id, summary, suggestions, model)
VALUES ($1, $2, $3, $4)
RETURNING id, run_id, summary, suggestions, model, created_at;

-- name: GetDiagnosisReportByRun :one
SELECT id, run_id, summary, suggestions, model, created_at
FROM diagnosis_reports
WHERE run_id = $1;
