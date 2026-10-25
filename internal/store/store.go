// Package store provides SQL-first data access for GenesisGo run data.
//
// It is built on pgx/pgxpool connection pooling. Every table is scoped by
// run_id so that evidence from different runs can never be mixed.
package store

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// queryer is implemented by both *pgxpool.Pool and pgx.Tx, which lets the same
// CRUD helpers run inside or outside a transaction.
type queryer interface {
	QueryRow(ctx context.Context, query string, args ...interface{}) pgx.Row
	Query(ctx context.Context, query string, args ...interface{}) (pgx.Rows, error)
	Exec(ctx context.Context, query string, args ...interface{}) (pgconn.CommandTag, error)
}

// Model layer. All ID and JSONB fields are scanned into plain Go types so the
// layer does not depend on a particular UUID or JSON library.

// Scenario is a predefined controlled fault scenario.
type Scenario struct {
	ID          string    `json:"id"`
	Key         string    `json:"key"`
	Name        string    `json:"name"`
	Description string    `json:"description,omitempty"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
}

// Run is a single execution of a scenario.
type Run struct {
	ID                 string     `json:"id"`
	ScenarioID         string     `json:"scenario_id"`
	Status             string     `json:"status"`
	CreatedAt          time.Time  `json:"created_at"`
	StartedAt          *time.Time `json:"started_at,omitempty"`
	CompletedAt        *time.Time `json:"completed_at,omitempty"`
	UpdatedAt          time.Time  `json:"updated_at"`
	FailureReason      *string    `json:"failure_reason,omitempty"`
	CancellationReason *string    `json:"cancellation_reason,omitempty"`
	AttemptCount       int32      `json:"attempt_count"`
	Version            int32      `json:"version"`
}

// RunJob is a bounded unit of work scheduled by the orchestrator for a run.
// A job is claimed by exactly one worker at a time using a lease; an expired
// lease marks a job whose worker was interrupted and that can be reclaimed.
type RunJob struct {
	ID             string     `json:"id"`
	RunID          string     `json:"run_id"`
	JobType        string     `json:"job_type"`
	Status         string     `json:"status"`
	Payload        []byte     `json:"payload,omitempty"`
	LeasedAt       *time.Time `json:"leased_at,omitempty"`
	LeaseOwner     *string    `json:"lease_owner,omitempty"`
	LeaseExpiresAt *time.Time `json:"lease_expires_at,omitempty"`
	CreatedAt      time.Time  `json:"created_at"`
	UpdatedAt      time.Time  `json:"updated_at"`
}

// EventAttempt is a single delivery attempt of an event to the consumer.
type EventAttempt struct {
	ID             string     `json:"id"`
	RunID          string     `json:"run_id"`
	EventID        string     `json:"event_id"`
	AttemptNumber  int32      `json:"attempt_number"`
	DeliveredAt    time.Time  `json:"delivered_at"`
	AcknowledgedAt *time.Time `json:"acknowledged_at,omitempty"`
	FaultInjected  *string    `json:"fault_injected,omitempty"`
	CreatedAt      time.Time  `json:"created_at"`
}

// SideEffectRecord is a business effect written by the consumer.
type SideEffectRecord struct {
	ID             string    `json:"id"`
	RunID          string    `json:"run_id"`
	EffectType     string    `json:"effect_type"`
	AggregateID    string    `json:"aggregate_id"`
	IdempotencyKey *string   `json:"idempotency_key,omitempty"`
	AmountCents    *int64    `json:"amount_cents,omitempty"`
	Currency       string    `json:"currency"`
	RecordedAt     time.Time `json:"recorded_at"`
}

// InvariantResult is a deterministic business invariant verdict for a run.
type InvariantResult struct {
	ID           string    `json:"id"`
	RunID        string    `json:"run_id"`
	InvariantKey string    `json:"invariant_key"`
	Verdict      string    `json:"verdict"`
	Details      []byte    `json:"details,omitempty"`
	EvaluatedAt  time.Time `json:"evaluated_at"`
}

// DiagnosisReport is an optional AI-assisted diagnosis report (one per run).
type DiagnosisReport struct {
	ID          string    `json:"id"`
	RunID       string    `json:"run_id"`
	Summary     *string   `json:"summary,omitempty"`
	Suggestions []byte    `json:"suggestions,omitempty"`
	Model       *string   `json:"model,omitempty"`
	CreatedAt   time.Time `json:"created_at"`
}

// StateSnapshot captures a safe before/after state summary for a single
// event delivery within a run. Only non-sensitive summary data is stored;
// full raw payloads are intentionally omitted.
type StateSnapshot struct {
	ID           string    `json:"id"`
	RunID        string    `json:"run_id"`
	StepID       string    `json:"step_id"`
	EventID      string    `json:"event_id"`
	SnapshotType string    `json:"snapshot_type"` // "before" or "after"
	AggregateID  string    `json:"aggregate_id"`
	StateSummary []byte    `json:"state_summary,omitempty"`
	CreatedAt    time.Time `json:"created_at"`
}

// Scan helpers map pgx rows onto models. NULLable columns scan into nil
// pointers; JSONB columns scan into raw bytes.

func scanScenario(row pgx.Row) (*Scenario, error) {
	var s Scenario
	if err := row.Scan(
		&s.ID, &s.Key, &s.Name, &s.Description, &s.CreatedAt, &s.UpdatedAt,
	); err != nil {
		return nil, err
	}
	return &s, nil
}

func scanRun(row pgx.Row) (*Run, error) {
	var r Run
	if err := row.Scan(
		&r.ID, &r.ScenarioID, &r.Status, &r.CreatedAt,
		&r.StartedAt, &r.CompletedAt, &r.UpdatedAt,
		&r.FailureReason, &r.CancellationReason, &r.AttemptCount, &r.Version,
	); err != nil {
		return nil, err
	}
	return &r, nil
}

func scanRunJob(row pgx.Row) (*RunJob, error) {
	var j RunJob
	if err := row.Scan(
		&j.ID, &j.RunID, &j.JobType, &j.Status, &j.Payload,
		&j.CreatedAt, &j.UpdatedAt, &j.LeasedAt, &j.LeaseOwner, &j.LeaseExpiresAt,
	); err != nil {
		return nil, err
	}
	return &j, nil
}

func scanEventAttempt(row pgx.Row) (*EventAttempt, error) {
	var a EventAttempt
	if err := row.Scan(
		&a.ID, &a.RunID, &a.EventID, &a.AttemptNumber, &a.DeliveredAt,
		&a.AcknowledgedAt, &a.FaultInjected, &a.CreatedAt,
	); err != nil {
		return nil, err
	}
	return &a, nil
}

func scanSideEffectRecord(row pgx.Row) (*SideEffectRecord, error) {
	var r SideEffectRecord
	if err := row.Scan(
		&r.ID, &r.RunID, &r.EffectType, &r.AggregateID,
		&r.IdempotencyKey, &r.AmountCents, &r.Currency, &r.RecordedAt,
	); err != nil {
		return nil, err
	}
	return &r, nil
}

func scanInvariantResult(row pgx.Row) (*InvariantResult, error) {
	var r InvariantResult
	if err := row.Scan(
		&r.ID, &r.RunID, &r.InvariantKey, &r.Verdict, &r.Details, &r.EvaluatedAt,
	); err != nil {
		return nil, err
	}
	return &r, nil
}

func scanDiagnosisReport(row pgx.Row) (*DiagnosisReport, error) {
	var r DiagnosisReport
	if err := row.Scan(
		&r.ID, &r.RunID, &r.Summary, &r.Suggestions, &r.Model, &r.CreatedAt,
	); err != nil {
		return nil, err
	}
	return &r, nil
}

func scanStateSnapshot(row pgx.Row) (*StateSnapshot, error) {
	var s StateSnapshot
	if err := row.Scan(
		&s.ID, &s.RunID, &s.StepID, &s.EventID, &s.SnapshotType,
		&s.AggregateID, &s.StateSummary, &s.CreatedAt,
	); err != nil {
		return nil, err
	}
	return &s, nil
}

// ---- scenarios ----------------------------------------------------------

// CreateScenario inserts a new scenario definition.
func CreateScenario(ctx context.Context, q queryer, key, name, description string) (*Scenario, error) {
	const query = `
		INSERT INTO scenarios (key, name, description)
		VALUES ($1, $2, $3)
		RETURNING id, key, name, description, created_at, updated_at
	`
	row := q.QueryRow(ctx, query, key, name, description)
	return scanScenario(row)
}

func (s *Store) CreateScenario(ctx context.Context, key, name, description string) (*Scenario, error) {
	return CreateScenario(ctx, s.pool, key, name, description)
}

// GetScenario returns a scenario by ID.
func GetScenario(ctx context.Context, q queryer, id string) (*Scenario, error) {
	const query = `
		SELECT id, key, name, description, created_at, updated_at
		FROM scenarios
		WHERE id = $1
	`
	row := q.QueryRow(ctx, query, id)
	return scanScenario(row)
}

func (s *Store) GetScenario(ctx context.Context, id string) (*Scenario, error) {
	return GetScenario(ctx, s.pool, id)
}

// ListScenarios returns all scenarios, newest first.
func ListScenarios(ctx context.Context, q queryer) ([]*Scenario, error) {
	const query = `
		SELECT id, key, name, description, created_at, updated_at
		FROM scenarios
		ORDER BY created_at DESC
	`
	rows, err := q.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*Scenario
	for rows.Next() {
		s, err := scanScenario(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if out == nil {
		out = []*Scenario{}
	}
	return out, nil
}

func (s *Store) ListScenarios(ctx context.Context) ([]*Scenario, error) {
	return ListScenarios(ctx, s.pool)
}

// ---- runs ---------------------------------------------------------------

// CreateRun inserts a new run in pending status.
func CreateRun(ctx context.Context, q queryer, scenarioID, status string) (*Run, error) {
	const query = `
		INSERT INTO runs (scenario_id, status)
		VALUES ($1, $2)
		RETURNING id, scenario_id, status, created_at, started_at, completed_at, updated_at, failure_reason, cancellation_reason, attempt_count, version
	`
	row := q.QueryRow(ctx, query, scenarioID, status)
	return scanRun(row)
}

func (s *Store) CreateRun(ctx context.Context, scenarioID, status string) (*Run, error) {
	return CreateRun(ctx, s.pool, scenarioID, status)
}

// GetRun returns a run by ID.
func GetRun(ctx context.Context, q queryer, id string) (*Run, error) {
	const query = `
		SELECT id, scenario_id, status, created_at, started_at, completed_at, updated_at, failure_reason, cancellation_reason, attempt_count, version
		FROM runs
		WHERE id = $1
	`
	row := q.QueryRow(ctx, query, id)
	return scanRun(row)
}

func (s *Store) GetRun(ctx context.Context, id string) (*Run, error) {
	return GetRun(ctx, s.pool, id)
}

// UpdateRunStatus updates a run's status and returns the updated run.
func UpdateRunStatus(ctx context.Context, q queryer, id, status string) (*Run, error) {
	const query = `
		UPDATE runs
		SET status = $2, updated_at = now()
		WHERE id = $1
		RETURNING id, scenario_id, status, created_at, started_at, completed_at, updated_at, failure_reason, cancellation_reason, attempt_count, version
	`
	row := q.QueryRow(ctx, query, id, status)
	return scanRun(row)
}

func (s *Store) UpdateRunStatus(ctx context.Context, id, status string) (*Run, error) {
	return UpdateRunStatus(ctx, s.pool, id, status)
}

// ListRunsByScenario returns runs for a scenario, newest first.
func ListRunsByScenario(ctx context.Context, q queryer, scenarioID string) ([]*Run, error) {
	const query = `
		SELECT id, scenario_id, status, created_at, started_at, completed_at, updated_at, failure_reason, cancellation_reason, attempt_count, version
		FROM runs
		WHERE scenario_id = $1
		ORDER BY created_at DESC
	`
	rows, err := q.Query(ctx, query, scenarioID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*Run
	for rows.Next() {
		r, err := scanRun(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if out == nil {
		out = []*Run{}
	}
	return out, nil
}

func (s *Store) ListRunsByScenario(ctx context.Context, scenarioID string) ([]*Run, error) {
	return ListRunsByScenario(ctx, s.pool, scenarioID)
}

// ListRuns returns all runs, newest first.
func ListRuns(ctx context.Context, q queryer) ([]*Run, error) {
	const query = `
		SELECT id, scenario_id, status, created_at, started_at, completed_at, updated_at, failure_reason, cancellation_reason, attempt_count, version
		FROM runs
		ORDER BY created_at DESC
	`
	rows, err := q.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*Run
	for rows.Next() {
		r, err := scanRun(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if out == nil {
		out = []*Run{}
	}
	return out, nil
}

func (s *Store) ListRuns(ctx context.Context) ([]*Run, error) {
	return ListRuns(ctx, s.pool)
}

// ---- run jobs -----------------------------------------------------------

// CreateRunJob inserts a bounded unit of work for a run.
func CreateRunJob(ctx context.Context, q queryer, runID, jobType, status string, payload []byte) (*RunJob, error) {
	const query = `
		INSERT INTO run_jobs (run_id, job_type, status, payload)
		VALUES ($1, $2, $3, $4)
		RETURNING id, run_id, job_type, status, payload, created_at, updated_at,
			leased_at, lease_owner, lease_expires_at
	`
	row := q.QueryRow(ctx, query, runID, jobType, status, payload)
	return scanRunJob(row)
}

func (s *Store) CreateRunJob(ctx context.Context, runID, jobType, status string, payload []byte) (*RunJob, error) {
	return CreateRunJob(ctx, s.pool, runID, jobType, status, payload)
}

// ListRunJobsByRun returns run jobs for a run, oldest first.
func ListRunJobsByRun(ctx context.Context, q queryer, runID string) ([]*RunJob, error) {
	const query = `
		SELECT id, run_id, job_type, status, payload, created_at, updated_at,
			leased_at, lease_owner, lease_expires_at
		FROM run_jobs
		WHERE run_id = $1
		ORDER BY created_at ASC
	`
	rows, err := q.Query(ctx, query, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*RunJob
	for rows.Next() {
		j, err := scanRunJob(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, j)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if out == nil {
		out = []*RunJob{}
	}
	return out, nil
}

func (s *Store) ListRunJobsByRun(ctx context.Context, runID string) ([]*RunJob, error) {
	return ListRunJobsByRun(ctx, s.pool, runID)
}

// ---- run job leasing ------------------------------------------------------

// GetScenarioByKey returns a scenario by its unique key.
func GetScenarioByKey(ctx context.Context, q queryer, key string) (*Scenario, error) {
	const query = `
		SELECT id, key, name, description, created_at, updated_at
		FROM scenarios
		WHERE key = $1;
	`
	row := q.QueryRow(ctx, query, key)
	return scanScenario(row)
}

func (s *Store) GetScenarioByKey(ctx context.Context, key string) (*Scenario, error) {
	return GetScenarioByKey(ctx, s.pool, key)
}

// ClaimRunJob atomically claims the oldest queued run job. It uses FOR UPDATE
// SKIP LOCKED so concurrent workers never claim the same job, and sets a lease
// (owned by `owner`) that expires after `ttlSeconds`. An expired lease marks a
// job whose worker was interrupted and that can be recovered by RecoverExpiredLeases.
func ClaimRunJob(ctx context.Context, q queryer, owner string, ttlSeconds int64) (*RunJob, error) {
	const query = `
		UPDATE run_jobs
		SET status = 'claimed', leased_at = now(), lease_owner = $2,
		    lease_expires_at = now() + ($3 * interval '1 second'), updated_at = now()
		WHERE id = (
			SELECT id FROM run_jobs
			WHERE status = 'queued'
			ORDER BY created_at ASC
			LIMIT 1
			FOR UPDATE SKIP LOCKED
		)
		RETURNING id, run_id, job_type, status, payload, created_at, updated_at,
			leased_at, lease_owner, lease_expires_at;
	`
	row := q.QueryRow(ctx, query, owner, ttlSeconds)
	return scanRunJob(row)
}

func (s *Store) ClaimRunJob(ctx context.Context, owner string, ttlSeconds int64) (*RunJob, error) {
	return ClaimRunJob(ctx, s.pool, owner, ttlSeconds)
}

// UpdateRunJobStatus sets a run job's status.
func UpdateRunJobStatus(ctx context.Context, q queryer, jobID, status string) error {
	const query = `
		UPDATE run_jobs
		SET status = $2, updated_at = now()
		WHERE id = $1;
	`
	_, err := q.Exec(ctx, query, jobID, status)
	return err
}

func (s *Store) UpdateRunJobStatus(ctx context.Context, jobID, status string) error {
	return UpdateRunJobStatus(ctx, s.pool, jobID, status)
}

// HeartbeatRunJob renews a claimed job's lease. It is a no-op unless the job is
// still claimed by the same owner, which prevents a stale worker from extending
// another worker's lease.
func HeartbeatRunJob(ctx context.Context, q queryer, jobID string, ttlSeconds int64, owner string) error {
	const query = `
		UPDATE run_jobs
		SET leased_at = now(), lease_expires_at = now() + ($2 * interval '1 second'), updated_at = now()
		WHERE id = $1 AND status = 'claimed' AND lease_owner = $3;
	`
	_, err := q.Exec(ctx, query, jobID, ttlSeconds, owner)
	return err
}

func (s *Store) HeartbeatRunJob(ctx context.Context, jobID string, ttlSeconds int64, owner string) error {
	return HeartbeatRunJob(ctx, s.pool, jobID, ttlSeconds, owner)
}

// RecoverExpiredLeases resets claimed jobs whose lease has expired back to
// queued so they can be claimed and executed by another worker.
func RecoverExpiredLeases(ctx context.Context, q queryer) (int64, error) {
	const query = `
		UPDATE run_jobs
		SET status = 'queued', leased_at = NULL, lease_owner = NULL,
		    lease_expires_at = NULL, updated_at = now()
		WHERE status = 'claimed' AND lease_expires_at < now();
	`
	tag, err := q.Exec(ctx, query)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

func (s *Store) RecoverExpiredLeases(ctx context.Context) (int64, error) {
	return RecoverExpiredLeases(ctx, s.pool)
}

// ---- event attempts -----------------------------------------------------

// CreateEventAttempt records a single delivery attempt.
func CreateEventAttempt(
	ctx context.Context,
	q queryer,
	runID, eventID string,
	attemptNumber int32,
	deliveredAt time.Time,
	acknowledgedAt *time.Time,
	faultInjected *string,
) (*EventAttempt, error) {
	const query = `
		INSERT INTO event_attempts (run_id, event_id, attempt_number, delivered_at, acknowledged_at, fault_injected)
		VALUES ($1, $2, $3, $4, $5, $6)
		RETURNING id, run_id, event_id, attempt_number, delivered_at, acknowledged_at, fault_injected, created_at
	`
	row := q.QueryRow(ctx, query, runID, eventID, attemptNumber, deliveredAt, acknowledgedAt, faultInjected)
	return scanEventAttempt(row)
}

func (s *Store) CreateEventAttempt(
	ctx context.Context,
	runID, eventID string,
	attemptNumber int32,
	deliveredAt time.Time,
	acknowledgedAt *time.Time,
	faultInjected *string,
) (*EventAttempt, error) {
	return CreateEventAttempt(ctx, s.pool, runID, eventID, attemptNumber, deliveredAt, acknowledgedAt, faultInjected)
}

// ListEventAttemptsByRun returns event attempts for a run, oldest first.
func ListEventAttemptsByRun(ctx context.Context, q queryer, runID string) ([]*EventAttempt, error) {
	const query = `
		SELECT id, run_id, event_id, attempt_number, delivered_at, acknowledged_at, fault_injected, created_at
		FROM event_attempts
		WHERE run_id = $1
		ORDER BY created_at ASC
	`
	rows, err := q.Query(ctx, query, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*EventAttempt
	for rows.Next() {
		a, err := scanEventAttempt(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if out == nil {
		out = []*EventAttempt{}
	}
	return out, nil
}

func (s *Store) ListEventAttemptsByRun(ctx context.Context, runID string) ([]*EventAttempt, error) {
	return ListEventAttemptsByRun(ctx, s.pool, runID)
}

// CountEventAttemptsByRun returns the total number of event attempts recorded
// for a run. Used to enforce MaxEventsPerRun.
func CountEventAttemptsByRun(ctx context.Context, q queryer, runID string) (int, error) {
	const query = `
		SELECT COUNT(*) FROM event_attempts WHERE run_id = $1
	`
	var n int
	row := q.QueryRow(ctx, query, runID)
	if err := row.Scan(&n); err != nil {
		return 0, err
	}
	return n, nil
}

func (s *Store) CountEventAttemptsByRun(ctx context.Context, runID string) (int, error) {
	return CountEventAttemptsByRun(ctx, s.pool, runID)
}

// DeleteRunData removes all child-table evidence for a run and then the run
// itself. All deletes are scoped by run_id so one run's data can never leak
// into another. Returns the number of runs deleted (0 or 1).
func DeleteRunData(ctx context.Context, q queryer, runID string) (int64, error) {
	// Child tables reference runs(id) ON DELETE CASCADE, so deleting the run
	// row removes all associated evidence. We delete child rows explicitly for
	// clarity and to support stores without cascade.
	const deleteAttempts = `DELETE FROM event_attempts WHERE run_id = $1`
	const deleteEffects = `DELETE FROM side_effect_records WHERE run_id = $1`
	const deleteInvariants = `DELETE FROM invariant_results WHERE run_id = $1`
	const deleteJobs = `DELETE FROM run_jobs WHERE run_id = $1`
	const deleteTransitions = `DELETE FROM run_transitions WHERE run_id = $1`
	const deleteDiagnosis = `DELETE FROM diagnosis_reports WHERE run_id = $1`
	const deleteRejected = `DELETE FROM rejected_events WHERE run_id = $1`
	const deleteSnapshots = `DELETE FROM state_snapshots WHERE run_id = $1`
	const deleteRun = `DELETE FROM runs WHERE id = $1`

	if _, err := q.Exec(ctx, deleteAttempts, runID); err != nil {
		return 0, err
	}
	if _, err := q.Exec(ctx, deleteEffects, runID); err != nil {
		return 0, err
	}
	if _, err := q.Exec(ctx, deleteInvariants, runID); err != nil {
		return 0, err
	}
	if _, err := q.Exec(ctx, deleteJobs, runID); err != nil {
		return 0, err
	}
	if _, err := q.Exec(ctx, deleteTransitions, runID); err != nil {
		return 0, err
	}
	if _, err := q.Exec(ctx, deleteDiagnosis, runID); err != nil {
		return 0, err
	}
	if _, err := q.Exec(ctx, deleteRejected, runID); err != nil {
		return 0, err
	}
	if _, err := q.Exec(ctx, deleteSnapshots, runID); err != nil {
		return 0, err
	}
	tag, err := q.Exec(ctx, deleteRun, runID)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

func (s *Store) DeleteRunData(ctx context.Context, runID string) (int64, error) {
	return DeleteRunData(ctx, s.pool, runID)
}

// ListRunsOlderThan returns runs whose completed_at is before cutoff, ordered
// oldest first. Used for retention cleanup of terminal runs.
func ListRunsOlderThan(ctx context.Context, q queryer, cutoff time.Time) ([]*Run, error) {
	const query = `
		SELECT id, scenario_id, status, created_at, started_at, completed_at, updated_at,
		       failure_reason, cancellation_reason, attempt_count, version
		FROM runs
		WHERE completed_at IS NOT NULL AND completed_at < $1
		ORDER BY completed_at ASC
	`
	rows, err := q.Query(ctx, query, cutoff)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*Run
	for rows.Next() {
		r, err := scanRun(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if out == nil {
		out = []*Run{}
	}
	return out, nil
}

func (s *Store) ListRunsOlderThan(ctx context.Context, cutoff time.Time) ([]*Run, error) {
	return ListRunsOlderThan(ctx, s.pool, cutoff)
}

// ---- side effect records ------------------------------------------------

// CreateSideEffectRecord records a business effect written by the consumer.
func CreateSideEffectRecord(
	ctx context.Context,
	q queryer,
	runID, effectType, aggregateID string,
	idempotencyKey *string,
	amountCents *int64,
	currency string,
) (*SideEffectRecord, error) {
	const query = `
		INSERT INTO side_effect_records (run_id, effect_type, aggregate_id, idempotency_key, amount_cents, currency)
		VALUES ($1, $2, $3, $4, $5, $6)
		RETURNING id, run_id, effect_type, aggregate_id, idempotency_key, amount_cents, currency, recorded_at
	`
	row := q.QueryRow(ctx, query, runID, effectType, aggregateID, idempotencyKey, amountCents, currency)
	return scanSideEffectRecord(row)
}

func (s *Store) CreateSideEffectRecord(
	ctx context.Context,
	runID, effectType, aggregateID string,
	idempotencyKey *string,
	amountCents *int64,
	currency string,
) (*SideEffectRecord, error) {
	return CreateSideEffectRecord(ctx, s.pool, runID, effectType, aggregateID, idempotencyKey, amountCents, currency)
}

// ListSideEffectRecordsByRun returns side-effect records for a run.
func ListSideEffectRecordsByRun(ctx context.Context, q queryer, runID string) ([]*SideEffectRecord, error) {
	const query = `
		SELECT id, run_id, effect_type, aggregate_id, idempotency_key, amount_cents, currency, recorded_at
		FROM side_effect_records
		WHERE run_id = $1
		ORDER BY recorded_at ASC
	`
	rows, err := q.Query(ctx, query, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*SideEffectRecord
	for rows.Next() {
		r, err := scanSideEffectRecord(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if out == nil {
		out = []*SideEffectRecord{}
	}
	return out, nil
}

func (s *Store) ListSideEffectRecordsByRun(ctx context.Context, runID string) ([]*SideEffectRecord, error) {
	return ListSideEffectRecordsByRun(ctx, s.pool, runID)
}

// ---- state snapshots -----------------------------------------------------

// CreateStateSnapshot records a safe before/after state summary for an event
// delivery. Only non-sensitive summary data is stored.
func CreateStateSnapshot(
	ctx context.Context,
	q queryer,
	runID, stepID, eventID, snapshotType, aggregateID string,
	stateSummary []byte,
) (*StateSnapshot, error) {
	const query = `
		INSERT INTO state_snapshots (run_id, step_id, event_id, snapshot_type, aggregate_id, state_summary)
		VALUES ($1, $2, $3, $4, $5, $6)
		RETURNING id, run_id, step_id, event_id, snapshot_type, aggregate_id, state_summary, created_at
	`
	row := q.QueryRow(ctx, query, runID, stepID, eventID, snapshotType, aggregateID, stateSummary)
	return scanStateSnapshot(row)
}

func (s *Store) CreateStateSnapshot(
	ctx context.Context,
	runID, stepID, eventID, snapshotType, aggregateID string,
	stateSummary []byte,
) (*StateSnapshot, error) {
	return CreateStateSnapshot(ctx, s.pool, runID, stepID, eventID, snapshotType, aggregateID, stateSummary)
}

// ListStateSnapshotsByRun returns state snapshots for a run, oldest first.
func ListStateSnapshotsByRun(ctx context.Context, q queryer, runID string) ([]*StateSnapshot, error) {
	const query = `
		SELECT id, run_id, step_id, event_id, snapshot_type, aggregate_id, state_summary, created_at
		FROM state_snapshots
		WHERE run_id = $1
		ORDER BY created_at ASC
	`
	rows, err := q.Query(ctx, query, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*StateSnapshot
	for rows.Next() {
		s, err := scanStateSnapshot(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if out == nil {
		out = []*StateSnapshot{}
	}
	return out, nil
}

func (s *Store) ListStateSnapshotsByRun(ctx context.Context, runID string) ([]*StateSnapshot, error) {
	return ListStateSnapshotsByRun(ctx, s.pool, runID)
}

// ---- invariant results --------------------------------------------------

// CreateInvariantResult records a deterministic invariant verdict.
func CreateInvariantResult(
	ctx context.Context,
	q queryer,
	runID, invariantKey, verdict string,
	details []byte,
) (*InvariantResult, error) {
	const query = `
		INSERT INTO invariant_results (run_id, invariant_key, verdict, details)
		VALUES ($1, $2, $3, $4)
		RETURNING id, run_id, invariant_key, verdict, details, evaluated_at
	`
	row := q.QueryRow(ctx, query, runID, invariantKey, verdict, details)
	return scanInvariantResult(row)
}

func (s *Store) CreateInvariantResult(
	ctx context.Context,
	runID, invariantKey, verdict string,
	details []byte,
) (*InvariantResult, error) {
	return CreateInvariantResult(ctx, s.pool, runID, invariantKey, verdict, details)
}

// ListInvariantResultsByRun returns invariant verdicts for a run.
func ListInvariantResultsByRun(ctx context.Context, q queryer, runID string) ([]*InvariantResult, error) {
	const query = `
		SELECT id, run_id, invariant_key, verdict, details, evaluated_at
		FROM invariant_results
		WHERE run_id = $1
		ORDER BY evaluated_at ASC
	`
	rows, err := q.Query(ctx, query, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*InvariantResult
	for rows.Next() {
		r, err := scanInvariantResult(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if out == nil {
		out = []*InvariantResult{}
	}
	return out, nil
}

func (s *Store) ListInvariantResultsByRun(ctx context.Context, runID string) ([]*InvariantResult, error) {
	return ListInvariantResultsByRun(ctx, s.pool, runID)
}

// ---- diagnosis reports --------------------------------------------------

// CreateDiagnosisReport inserts an optional AI-assisted diagnosis report.
func CreateDiagnosisReport(
	ctx context.Context,
	q queryer,
	runID string,
	summary *string,
	suggestions []byte,
	model *string,
) (*DiagnosisReport, error) {
	const query = `
		INSERT INTO diagnosis_reports (run_id, summary, suggestions, model)
		VALUES ($1, $2, $3, $4)
		RETURNING id, run_id, summary, suggestions, model, created_at
	`
	row := q.QueryRow(ctx, query, runID, summary, suggestions, model)
	return scanDiagnosisReport(row)
}

func (s *Store) CreateDiagnosisReport(
	ctx context.Context,
	runID string,
	summary *string,
	suggestions []byte,
	model *string,
) (*DiagnosisReport, error) {
	return CreateDiagnosisReport(ctx, s.pool, runID, summary, suggestions, model)
}

// GetDiagnosisReportByRun returns the diagnosis report for a run.
func GetDiagnosisReportByRun(ctx context.Context, q queryer, runID string) (*DiagnosisReport, error) {
	const query = `
		SELECT id, run_id, summary, suggestions, model, created_at
		FROM diagnosis_reports
		WHERE run_id = $1
	`
	row := q.QueryRow(ctx, query, runID)
	return scanDiagnosisReport(row)
}

func (s *Store) GetDiagnosisReportByRun(ctx context.Context, runID string) (*DiagnosisReport, error) {
	return GetDiagnosisReportByRun(ctx, s.pool, runID)
}

// ---- transactions -------------------------------------------------------

// Begin starts a new transaction on the connection pool.
func (s *Store) Begin(ctx context.Context) (pgx.Tx, error) {
	return s.pool.Begin(ctx)
}

// WithTx runs fn within a transaction. On success the transaction is committed;
// on error (or panic) it is rolled back. The returned error is fn's error, or
// the commit/rollback error if fn succeeded.
func (s *Store) WithTx(ctx context.Context, fn func(pgx.Tx) error) (err error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() {
		if p := recover(); p != nil {
			_ = tx.Rollback(ctx)
			panic(p)
		}
		if err != nil {
			_ = tx.Rollback(ctx)
			return
		}
		err = tx.Commit(ctx)
	}()
	err = fn(tx)
	return err
}

// Ensure pgx.Tx satisfies queryer so WithTx callers can pass tx to CRUD helpers.
var _ queryer = (pgx.Tx)(nil)

// sql.Null is referenced only to keep the import when no nullable sql types are
// used directly; remove if unused. Kept intentionally minimal.
var _ = sql.NullString{}
var _ = errors.New
