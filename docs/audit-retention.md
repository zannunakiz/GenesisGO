# Audit hygiene and data retention

## Audit records

GenesisGo records security-relevant actions in the append-only `audit_logs`
table:

- `run.created` — a run is created for a scenario (records the scenario key
  and whether an idempotency key was provided; never the request body).
- `run.canceled` — a run is cancelled (records the resulting status).
- `report.accessed` — a run report is retrieved.
- `config.loaded` — recorded once at startup with a summary of effective,
  non-secret configuration.

Each record stores `action`, `actor_key_id` (the API key id of the
authenticated actor, never the secret), `detail` (a redacted summary), and
`created_at`. `run_id` links the record to a run when one is involved.

## What is never logged

API keys and secrets, credentials, authorization headers, passwords, tokens,
and complete request bodies are never written to audit logs or application
logs. Every detail string passes through `store.SanitizeDetail`, which
redacts values of secret-like keys (authorization, api_key, password, secret,
token, and similar) to `[REDACTED]` before persistence.

## Retention and cleanup

- Diagnostic artifacts (`diagnosis_reports`, `state_snapshots`) older than
  `RETENTION_PERIOD` (default 24h) are pruned.
- Terminal runs older than `RETENTION_PERIOD` beyond `MAX_RUNS` per scenario
  (default 1000) are deleted together with every child record (event
  attempts, side effects, invariant results, jobs, transitions).
- Active (non-terminal) runs and the latest run per scenario are never
  deleted, so the minimum records needed to explain a current run are always
  preserved.
- `audit_logs` are append-only and retained as the evidence trail; when a
  run is deleted its audit records keep their action history with `run_id`
  set to NULL.

Cleanup runs at startup, is scoped and repeatable, and never removes records
needed to explain an active or recent run.