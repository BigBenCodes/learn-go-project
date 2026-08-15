# Configuration and Topics

## `fraud-service` flags and environment variables

Every flag falls back to an environment variable, which falls back to a hardcoded default (`cmd/fraud-service/main.go`).

| Flag | Env variable | Default | Purpose |
| --- | --- | --- | --- |
| `--brokers` | `KAFKA_BROKERS` | `localhost:19092` | Comma-separated Kafka brokers |
| `--database-url` | `DATABASE_URL` | `postgres://fraud:fraud@localhost:5432/fraud?sslmode=disable` | Postgres connection string |
| `--http-address` | `HTTP_ADDRESS` | `:8080` | HTTP listen address |
| `--model` | `MODEL_PATH` | `configs/model.json` | Model artifact path |
| `--workers` | — | `6` | Bounded transaction-consumer worker pool size |
| `--review-threshold` | — | `0.65` | Score at/above which `review` is recommended |
| `--escalate-threshold` | — | `0.85` | Score at/above which `escalate` is recommended |

Thresholds must satisfy `0 <= review-threshold < escalate-threshold <= 1`; the service fails to start otherwise.

## `configs/model.json` schema

```json
{
  "version": "dummy-logistic-v1",
  "intercept": -6.0,
  "weights": {
    "log_amount": 0.55,
    "foreign": 1.25,
    "card_not_present": 1.1,
    "risky_merchant": 1.6,
    "nighttime": 0.45,
    "tx_count_10m": 1.3,
    "spend_1h": 1.5,
    "new_merchant": 0.5,
    "new_country": 0.75
  }
}
```

| Field | Type | Purpose |
| --- | --- | --- |
| `version` | string | Required. Identifies this artifact; stored on every `Assessment` and used to scope `/v1/model-metrics`. |
| `intercept` | number | Added to the weighted sum before the sigmoid. |
| `weights` | object | Required, non-empty. Maps feature name → coefficient; keys must match the feature names in [Scoring and Explainability](../explanation/scoring-and-explainability.md). |

## Kafka topics

Created by the `topics` bootstrap service in `compose.yaml` (6 partitions each):

| Topic | Producer | Consumer(s) |
| --- | --- | --- |
| `transactions.v1` | `simulator` | `fraud-service` (`fraud-scorer-v1` group) |
| `fraud.labels.v1` | `simulator` (delayed) | `fraud-service` (`fraud-label-evaluator-v1` group) |
| `fraud.assessments.v1` | `fraud-service` outbox | (none in this repo — external consumers would subscribe here) |
| `fraud.dead-letter.v1` | `fraud-service` consumer (on permanent decode/validation failure) | (none in this repo — inspect manually) |

## Consumer groups

| Group | Topic | Workers |
| --- | --- | --- |
| `fraud-scorer-v1` | `transactions.v1` | `--workers` (default 6), keyed by `partition % workers` |
| `fraud-label-evaluator-v1` | `fraud.labels.v1` | 2, fixed |

## Prometheus metrics

Registered in `internal/service/metrics.go`, exposed at `GET /metrics`:

| Metric | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `fraud_events_processed_total` | counter | `type` (`transaction`/`label`) | Successfully processed events |
| `fraud_duplicate_events_total` | counter | `type` | Redelivered events detected as no-ops |
| `fraud_processing_failures_total` | counter | `type` | Decode/validation/storage failures |
| `fraud_recommended_actions_total` | counter | `action`, `model_version` | Assessments by recommended action and model version |
| `fraud_assessment_duration_seconds` | histogram | — | End-to-end scoring latency per transaction |
| `fraud_outbox_published_total` | counter | — | Successfully published outbox events |
| `fraud_outbox_failures_total` | counter | — | Failed outbox publish attempts |

Plus the standard Go runtime and process collectors (`prometheus.NewGoCollector`, `prometheus.NewProcessCollector`).
