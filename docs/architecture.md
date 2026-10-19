# GenesisGo v1 architecture and boundaries

## Product scope

GenesisGo is a local-first reliability test harness for event-driven consumers. V1 executes only its built-in Go payment/inventory sample consumer, with synthetic data and isolated test state. A scenario is a versioned, validated fault recipe with expected business invariants; its result comes from inspecting recorded state, not from whether message delivery alone succeeded.

Supported scenarios are normal processing as a control, duplicate delivery, retry/redelivery after a transient failure, out-of-order delivery, acknowledgement timeout, consumer restart, and crash after a database commit but before acknowledgement. The built-in consumer may expose explicitly defined safe and deliberately unsafe handling modes for reproducible invariant comparisons. Scenario and consumer options are allowlisted and validated.

## Run and result contract

`Run.status` is one of `queued`, `running`, `passed`, `failed`, `cancelled`, or `timed_out`. A new run is `queued`; a worker claims it and changes it to `running`; queued or running work can end as `failed`, `cancelled`, or `timed_out`, and successfully verified running work ends as `passed`. Cancellation is terminal only after work stops. A deadline reached before completion produces `timed_out`. State transitions are conditional so competing workers cannot complete a run twice. Setup, execution, and verification are phases within `running`, not additional public run states.

`passed` means execution and verification completed and all required invariants passed. `failed` means execution or verification failed, or at least one evaluated required invariant failed. A failed setup or infrastructure error is recorded as an execution error; it must not be fabricated as a failed business invariant. `cancelled` and `timed_out` are not passing results.

Each event envelope contains an event ID, event type, schema version, occurrence time, `run_id`, and a validated typed synthetic business payload. Delivery attempts record the event/run IDs, attempt number and time, acknowledgement outcome, and any failure category. Side effects record their run and event IDs and the synthetic business mutation. Events, attempts, side effects, invariant results, and reports are always associated with one run.

An invariant result identifies the rule, outcome (`passed`, `failed`, or `not_evaluated`), expected and actual values where available, and evidence references. The report contains the run ID and final status, overall verdict (`pass`, `fail`, or `not_evaluated`), scenario ID/version and immutable snapshot, timestamps and execution error when applicable, ordered delivery evidence, recorded side effects, invariant results, and an optional diagnosis with its availability status. The verdict is `pass` only when execution and verification completed and all required invariants passed; it is `fail` when any evaluated required invariant failed; otherwise, if verification did not establish a verdict, it is `not_evaluated`. Infrastructure errors must not be represented as business-invariant failures.

## Component boundaries

- **API:** validates requests and allowlisted scenario options, creates and exposes runs and reports, and publishes bounded run work. It does not execute user-supplied workloads or decide invariant outcomes.
- **Worker:** claims run work, applies the selected fault recipe to the built-in consumer, records events/attempts/effects, runs deterministic invariant checks, and conditionally stores the terminal result. Work is bounded, cancellable, and subject to a run deadline.
- **Sandbox state:** PostgreSQL stores test-run metadata and evidence; NATS JetStream transports backend-managed work/events. All persisted evidence and mutations are scoped by `run_id`. This is logical data isolation for the built-in synthetic workload, not a facility for executing arbitrary code. No production database, broker, or payment provider is replayed or mutated.
- **AI diagnosis:** an optional adapter receives only the evidence selected for that run and may return a clearly labelled, validated explanation. Missing AI configuration is reported as unavailable. AI output cannot set or change run status, invariant outcomes, or execute tools/code.
- **Observability:** optional local telemetry components receive operational logs, metrics, and traces. The core run path does not require hosted monitoring. Telemetry is not the source of truth for run reports and must not expose credentials or sensitive payloads.

## Non-goals and safety limits

V1 is not a production message broker, payment processor, production replay service, or arbitrary consumer test runner. It does not accept or execute uploaded source, binaries, scripts, shell commands, Docker images, or container configurations, and does not connect scenarios to user production infrastructure. It does not require paid AI, hosted databases/brokers, Arcjet, or hosted observability for local non-AI scenarios. AI is advisory only; deterministic state and invariant evaluation are authoritative.

## Development toolchain

The Go module targets Go 1.27.0. This toolchain was resolved through Go's automatic toolchain download because only Go 1.25.6 was preinstalled. The latest Arcjet Go SDK checked for this workspace, v1.2.0, declares Go 1.25.0 and is compatible with Go 1.27.0.

## Runtime configuration

`internal/config` validates environment-driven runtime settings. For local development it reads `.env` if present, with process environment values taking precedence. Copy `.env.example` to `.env` and replace placeholders locally; `.env` is ignored and must not be committed. Arcjet defaults to disabled and requires a structurally valid `ARCJET_KEY` only when `ARCJET_ENABLED=true`. Ollama and telemetry are optional, and the default non-AI configuration does not contact either service.

Control-plane authentication uses API keys stored only as SHA-256 digests in `API_KEYS_FILE` (default `.local/api-keys.json`). There is no default production secret: create a local key with `go run ./cmd/bootstrap-key`, copy the one-time `GENESISGO_API_KEY` value, and keep it out of tracked files, logs, and reports. Keys carry `read`, `run`, or `admin` scopes (`admin` implies both, `run` implies `read`); missing or unknown keys return `401 unauthorized`, while a valid key without the required scope returns `403 forbidden`. `DEMO_AUTH_DISABLED=true` is a local-only escape hatch for read-only demo calls and is rejected in production, as is `API_KEYS_ENABLED=false`. Production startup also refuses to run when the key file contains no usable keys.

Production mode requires explicit listener, database, NATS, and CORS settings. Production database connections must require TLS, NATS must use TLS, CORS must use public HTTPS origins, and debug logging is rejected. Configuration errors identify the setting to fix without including configured values.

## HTTP transport behaviour

`internal/httpapi` centralises the JSON error envelope (`code`, `message`, `request_id`, optional field errors), content-type/body-size/JSON-shape validation, string/enum/list bounds, read/write/idle/handler timeouts, liveness/readiness health endpoints, request-ID propagation, and panic recovery. Error responses use stable codes and safe messages; SQL errors, stack traces, and raw internal text are never serialised.

`internal/observability` provides JSON logging, request/run correlation helpers, and the central redaction policy. Request logs record method, path, status, duration, and correlation identifiers only; auth headers, API keys, cookies, secrets, connection strings, and raw payment fields are replaced with a fixed placeholder. API shutdown cancels active run work, checkpoints terminal state without ever marking interrupted runs as passed, then closes HTTP, database, broker, and telemetry resources in bounded order.
