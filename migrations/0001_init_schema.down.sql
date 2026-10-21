-- GenesisGo initial schema (revert).
-- +goose Down
-- +goose StatementBegin

DROP INDEX IF EXISTS idx_runs_scenario_id;
DROP INDEX IF EXISTS idx_runs_status;
DROP INDEX IF EXISTS idx_runs_created_at;
DROP INDEX IF EXISTS idx_run_jobs_run_id_status;
DROP INDEX IF EXISTS idx_run_jobs_run_id;
DROP INDEX IF EXISTS idx_event_attempts_event_id;
DROP INDEX IF EXISTS idx_event_attempts_run_id;
DROP INDEX IF EXISTS idx_side_effect_records_run_id;
DROP INDEX IF EXISTS idx_invariant_results_run_id;

DROP TABLE IF EXISTS diagnosis_reports;
DROP TABLE IF EXISTS invariant_results;
DROP TABLE IF EXISTS side_effect_records;
DROP TABLE IF EXISTS event_attempts;
DROP TABLE IF EXISTS run_jobs;
DROP TABLE IF EXISTS runs;
DROP TABLE IF EXISTS scenarios;

DROP TYPE IF EXISTS run_status;

-- +goose StatementEnd
