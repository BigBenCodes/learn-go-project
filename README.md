# Go Fraud Pipeline

A deliberately small, production-shaped machine-learning inference pipeline for learning Go. It generates mock card transactions, scores them asynchronously with a transparent logistic-regression model, recommends an action, and later evaluates those predictions against delayed ground-truth labels.

The project emphasizes backend engineering rather than model training: concurrency, Kafka partitions, idempotency, database transactions, the outbox pattern, cancellation, retries, HTTP APIs, structured logs, and metrics.

## Architecture

```text
simulator ── transactions.v1 ──► Redpanda ──► fraud-service ──► Postgres
    │                                             │                 │
    └──── delayed fraud.labels.v1 ────────────────┘                 │
                                                  │                 │
                                                  └─ outbox ────────┘
                                                       │
                                              fraud.assessments.v1

fraudctl / curl ───────────────► HTTP API (:8080) ─────► Postgres
```

Transactions are keyed by account ID. Kafka partitions are processed concurrently, while records within a partition stay ordered. Each database transaction inserts the input, calculates durable history features, stores the assessment, and adds its output to an outbox. Kafka offsets are committed only afterwards. A replay therefore produces one logical assessment even if Kafka delivers an event more than once.

The detector reports a score and feature contributions. A separate policy maps that score to `no_action`, `review`, or `escalate`; because this is an asynchronous pipeline, these are recommendations rather than payment authorization responses.

## Run it

Requirements: Go 1.26+, Docker, and Docker Compose.

```sh
make infra-up
make service
```

In another terminal, generate 1,000 events (1% fraud, labels delayed by five seconds):

```sh
make simulate
```

Inspect the results:

```sh
go run ./cmd/fraudctl list --limit 5
go run ./cmd/fraudctl list --action escalate
go run ./cmd/fraudctl show tx-42-000000001
go run ./cmd/fraudctl stats
curl localhost:8080/metrics
```

Repeat the simulator seed, start time, rate, account count, and fraud rate to reproduce every event exactly. Use `--count 0` for a continuous stream. Run the service itself in Compose with `docker compose --profile app up --build`.

To stop the infrastructure without deleting its data, run `make infra-down`. Use `docker compose down -v` when you intentionally want a clean database and event log.

## API

| Endpoint | Purpose |
| --- | --- |
| `GET /healthz` | Process liveness |
| `GET /readyz` | Postgres readiness |
| `GET /v1/transactions?limit=20&action=review&cursor=...` | Cursor-paginated decisions |
| `GET /v1/transactions/{id}` | Transaction, signals, score, action, model version, and label |
| `GET /v1/model-metrics?model_version=...` | Confusion matrix, precision, recall, false-positive rate, and label coverage |
| `GET /metrics` | Prometheus-format system metrics |

## Model and features

[`configs/model.json`](configs/model.json) contains a versioned intercept and weights. The Go scorer calculates the logistic probability and records each feature's contribution, making every dummy decision explainable. Features include amount, foreign/card-not-present flags, merchant risk, time of day, recent transaction count, recent spend, and whether the merchant or country is new to the account.

The simulator deliberately creates fraud patterns that correlate with these signals. Labels are published on a separate topic so inference code cannot accidentally read the answer. This is useful for discussing leakage, delayed labels, class imbalance, threshold selection, and model/version monitoring.

## Development

```sh
make test        # unit tests with the race detector
make build       # compile every command
go vet ./...
```

Useful experiments:

- Stop Postgres while consuming, restart it, and watch the same offset retry safely.
- Rewind the `fraud-scorer-v1` consumer group and verify row counts do not change.
- Change policy thresholds without changing the model artifact.
- Add a second model artifact and compare metrics by model version.
- Replace the Go scorer behind its interface with a remote Python inference service.

This remains a learning system: it contains synthetic data, hand-authored model weights, a single local broker, no authentication, and no real payment intervention.
