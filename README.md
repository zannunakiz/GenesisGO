# GenesisGo

Event-driven reliability testing and controlled failure simulation for backend developers. Reproduce message-processing failure modes (duplicate delivery, retries, out-of-order delivery, consumer restart, crash after commit before ack) against an isolated sample consumer and get a deterministic invariant verdict per run. AI-assisted diagnosis is optional and advisory only — it never decides pass/fail.

## What it does

- Lets you choose a built-in fault scenario, run it against an isolated test database, and inspect the delivery timeline, side effects, and invariant verdicts.
- Every run is isolated by `run_id`; reports never mix events or state from different runs.
- Results are deterministic and reproducible. Verdicts come from recorded state and invariant evaluation, not from AI.
- Optional local AI (Ollama) can summarise evidence; when not configured, diagnosis is reported as unavailable — never fabricated.

## Supported scenarios

| Scenario | What it exercises | Expected verdict |
|----------|-------------------|------------------|
| `normal_payment` | Payment delivered and acknowledged exactly once (control/happy path) | pass |
| `duplicate_delivery` | Same payment delivered twice; consumer must dedupe by idempotency key | pass (safe dedupe) |
| `retry_after_timeout` | Delivery times out and is retried; at-least-once without duplicate effect | pass |
| `out_of_order` | Events delivered out of order; correct transition sequence enforced | pass |
| `crash_after_commit_before_ack` | Consumer commits then crashes before ack; redelivery must not duplicate | pass |
| `worker_restart` | Worker restarts mid-scenario; in-progress state recovered, no lost/duplicated effects | pass |

Fixtures live in `internal/scenarios/fixtures/*.json` (committed, source-controlled).

## Architecture (summary)

```
operator/API ──┬─► scheduler/worker ──► NATS JetStream ──► sample consumer ──► PostgreSQL (per run_id)
               │                    └─ invariant engine ──► report ──► REST + SSE
               └─ health (/health/live, /health/ready, /healthz), metrics (loopback)
```

- **API:** validates allowlisted scenario options, creates/exposes runs and reports, publishes bounded run work. Does not execute user-supplied workloads or decide invariant outcomes.
- **Worker:** claims run work, applies the fault recipe to the built-in consumer, records events/attempts/effects, runs deterministic invariant checks, conditionally stores terminal results. Bounded, cancellable, subject to a run deadline.
- **Sandbox state:** PostgreSQL stores test-run metadata/evidence; NATS JetStream transports work/events. All evidence is scoped by `run_id`. No production infra is touched.

See `docs/architecture.md` for the full component boundaries, safety limits, and production configuration notes.
## Test commands

| Command | What it does |
|---------|--------------|
| `make test` | `go test ./...` |
| `make race-test` | `go test -race ./...` |
| `make integration-test` | `go test -tags=integration ./...` (Testcontainers; needs Docker) |
| `make lint` | `go vet`, `staticcheck`, `npm lint` |
| `make format` | `go fmt`, `npm format` |
| `make build` | `go build ./...` |
| `make web-check` | frontend lint, typecheck, vitest, OpenAPI contract check |
| `make web-build` | `next build` |
| `make quality` | lint + test + build + web-check + web-build |

CI equivalent: `.github/workflows/ci.yml` runs Go formatting, vet/staticcheck, unit/race/integration tests, frontend checks/build, Trivy fs/image/config scans, Docker build, and CycloneDX SBOM — no paid keys, least-privilege permissions, pinned action SHAs.

## API reference

Endpoints (control-plane routes require an API key; health is unauthenticated):

| Method | Path | Scope |
|--------|------|-------|
| GET | `/api/v1/scenarios` | read |
| POST | `/api/v1/runs` | run (idempotent via `Idempotency-Key` header) |
| GET | `/api/v1/runs` | read |
| GET | `/api/v1/runs/{id}` | read |
| POST | `/api/v1/runs/{id}/cancel` | run |
| GET | `/api/v1/runs/{id}/report` | read |
| GET | `/api/v1/runs/{id}/export` | read (JSON download) |
| GET | `/api/v1/runs/{id}/export.csv` | read (CSV download) |
| GET | `/api/v1/runs/{id}/events` | read (SSE) |
| GET | `/health/live`, `/health/ready`, `/healthz` | none |

Interactive API reference: `web/public/openapi.yaml` (OpenAPI 3.1), validated by `make web-check`. There is no hosted API doc; serve `openapi.yaml` locally with any OpenAPI/Swagger UI tool.

Example (API key in `GENESISGO_API_KEY`):

```
curl -s http://127.0.0.1:8080/api/v1/scenarios \
  -H "Authorization: Bearer $GENESISGO_API_KEY"
```

## Migrations

Committed SQL migrations in `migrations/` (applied on API startup against the run-scoped test database):

- `0001_init_schema` — core schema
- `0002_run_lifecycle` — run states/transitions
- `0003_rejected_events` — rejected event envelopes
- `0004_run_job_leases` — bounded worker leases
- `0005_state_snapshots` — before/after side-effect state
- `0006_audit_logs` — append-only security-relevant actions

Migrations run automatically; no manual migration command is required for the local demo.

## Backups of local data

Local Compose data lives in Docker named volumes (`postgres-data`, etc.) outside the repository. Back up by dumping the PostgreSQL database or snapshotting the named volume:

```
docker compose exec postgres pg_dump -U genesisgo genesisgo > backup.sql
```

`.gitignore` excludes local DB files (`*.db`, `*.sqlite`, dumps) and volumes (`/volumes/`); do not commit database dumps.

## Worker recovery

The worker claims bounded run jobs with leases and checkpoints terminal state on shutdown. Interrupted runs are never marked `passed`; they remain `queued`/`running`/`cancelled`/`timed_out` and can be re-enqueued or inspected. Shutdown cancels active run work, closes HTTP/database/broker/telemetry in bounded order, then exits.

## Retention

- Diagnostic artifacts older than `RETENTION_PERIOD` (default 24h) are pruned.
- Terminal runs older than `RETENTION_PERIOD` beyond `MAX_RUNS` per scenario (default 1000) are deleted with child records.
- Active runs and the latest run per scenario are never deleted.
- `audit_logs` are append-only and retained as the evidence trail.

Cleanup runs at startup and is scoped/repeatable. See `docs/audit-retention.md`.

## Security boundaries

- API keys are stored only as SHA-256 digests in `API_KEYS_FILE` (default `.local/api-keys.json`); no default production secret.
- No secrets in images; runtime secrets come from gitignored `.env` only. `.gitignore` excludes `.env`, `.env.*`, `secrets/`, `credentials/`, `*.pem/*.key`, `.local/`, logs, coverage, binaries, `node_modules`, `.next`, Docker overrides, and IDE/OS files.
- No Docker socket is mounted into the API; no API-driven arbitrary container execution.
- All published Compose ports bind to `127.0.0.1` only; nothing is exposed publicly.
- Request logs and audit details are redacted (`store.SanitizeDetail` redacts secret-like keys); credentials, SQL, stack traces, and raw payment fields never appear in logs/reports.
- Production requires explicit TLS database/NATS/CORS settings and rejects debug logging.

## Known limitations

- Docker Desktop on Windows can occasionally fail to mark Testcontainers containers ready; integration tests skip gracefully there and run for real on Linux CI.
- `go test -race` requires cgo and runs on Linux CI (not on this Windows host).
- Trivy image scanning is not installed locally; it runs in CI (Step 42).
- AI diagnosis is unavailable without a local Ollama model; output is never fabricated.

## Troubleshooting

| Symptom | Likely cause / fix |
|---------|--------------------|
| `docker compose up` port conflict on 5432 | Set `POSTGRES_HOST_PORT=15432` (compose remaps host port via `${POSTGRES_HOST_PORT:-5432}`) |
| API `401 unauthorized` | Provide a valid API key via `Authorization: Bearer <key>`; bootstrap with `go run ./cmd/bootstrap-key` |
| Diagnosis missing | Ollama not configured (`OLLAMA_URL`/`OLLAMA_MODEL` empty) — expected; not an error |
| Health `not_ready` | `DATABASE_URL` or `NATS_URL` empty; check `.env` and that compose services are healthy |
| Frontend can't reach API | Set `GENESISGO_API_URL` (default `http://127.0.0.1:8080`); ensure API is running |

## Repository conventions

- Go: `go 1.27`, `gofmt`/`go vet`/staticcheck enforced by CI and `make lint`/`make format`.
- Frontend: Next.js + vitest + biome; contract check compares `web/public/openapi.yaml` against registered Go routes.
- Do not document future capabilities as existing; commands here match repository files (`Makefile`, `docker-compose.yml`, `.github/workflows/ci.yml`).
