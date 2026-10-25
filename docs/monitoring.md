# Local monitoring stack

GenesisGo ships an opt-in observability profile:

    docker compose --profile observability up -d

The core stack (`docker compose up -d`) runs without any monitoring
components, and the core profile works with monitoring disabled.

## Ports (all published to 127.0.0.1 only)

| Service | Host port | Notes |
|---------|-----------|-------|
| postgres | 5432 | Application database |
| nats | 4222 | Event broker |
| GenesisGo API | 8080 | REST API |
| API metrics | 9092 | Prometheus scrape target (container port 9090) |
| OTLP collector | 4318 | Traces/logs intake (observability profile) |
| Prometheus | 9091 | Metrics storage and UI (observability profile) |
| Grafana | 3001 | Dashboards; anonymous access, loopback only (observability profile) |
| Tempo | 3200 (HTTP), 4317 (OTLP gRPC) | Trace storage (observability profile) |
| Loki | 3100 | Log storage (observability profile) |

No monitoring or admin endpoint is published on a public interface. Inside the
Docker network the API metrics endpoint listens on 0.0.0.0:9090 so Prometheus
can scrape it; on the host it is published to 127.0.0.1:9092 only.

## Profiles

- Default: postgres, nats, api, worker, sample-consumer.
- `observability`: adds otel-collector, prometheus, tempo, loki, grafana.

Traces are exported to the OTLP collector (TELEMETRY_ENDPOINT is wired in the
compose file) and forwarded to Tempo. When the observability profile is off,
export failures are logged and the API keeps running. Application logs are
available via `docker compose logs`; forward them to Loki with the Loki
logging driver if desired.

## Grafana

The "GenesisGo" dashboard shows API requests, responses by status class,
active runs, run outcomes, delivery retries, and invariant failures. Data
sources are provisioned automatically: Prometheus (PROM), Tempo (TEMPO),
Loki (LOKI).

Persistent volumes are used only for postgres, Prometheus, Tempo, Loki, and
Grafana data, as Docker named volumes outside the repository.