# HTTP API

Served by `internal/httpapi.Server` on the address set by `--http-address`/`HTTP_ADDRESS` (default `:8080`). All responses are `application/json`.

## Endpoints

| Method & Path | Purpose |
| --- | --- |
| `GET /healthz` | Process liveness. Always `200 {"status":"ok"}` if the process is running. |
| `GET /readyz` | Readiness. Pings Postgres with a 1s timeout; `200 {"status":"ready"}` or `503 {"error":"database unavailable"}`. |
| `GET /v1/transactions` | Cursor-paginated list of scored transactions. |
| `GET /v1/transactions/{id}` | A single transaction, its assessment, and its label if present. |
| `GET /v1/model-metrics` | Confusion-matrix-derived model quality metrics. |
| `GET /metrics` | Prometheus-format system metrics. |

## `GET /v1/transactions`

### Query parameters

| Parameter | Type | Default | Notes |
| --- | --- | --- | --- |
| `limit` | int | 20 | Must be 1–100; out of range returns `400`. |
| `action` | string | (none) | One of `no_action`, `review`, `escalate`. Invalid values return `400`. |
| `cursor` | string | (none) | Opaque token from a previous response's `next_cursor`. Invalid tokens return `400`. |

### Response

```json
{
  "transactions": [ { "transaction": { "...": "..." }, "assessment": { "...": "..." }, "label": null } ],
  "next_cursor": "base64-token-or-empty-string"
}
```

Each entry is a `TransactionRecord` (see [Configuration and Topics](configuration.md) for the field shapes). Pagination is cursor-based on `(occurred_at, transaction_id)` — pass `next_cursor` back as `cursor` to fetch the next page; an empty `next_cursor` means you're on the last page.

## `GET /v1/transactions/{id}`

Returns one `TransactionRecord`. `404 {"error":"transaction not found"}` if the ID doesn't exist.

## `GET /v1/model-metrics`

### Query parameters

| Parameter | Type | Default | Notes |
| --- | --- | --- | --- |
| `model_version` | string | (none) | Scopes the confusion matrix to one model version. Omit to aggregate across all versions seen. |

### Response

```json
{
  "model_version": "dummy-logistic-v1",
  "total_assessments": 1000,
  "labelled": 950,
  "true_positive": 8,
  "false_positive": 3,
  "true_negative": 935,
  "false_negative": 4,
  "precision": 0.727,
  "recall": 0.667,
  "false_positive_rate": 0.0032,
  "label_coverage": 0.95
}
```

Computed directly in SQL from whatever ground-truth labels have arrived so far — see [Delivery Guarantees and Idempotency](../explanation/delivery-guarantees-and-idempotency.md) for why labels can lag assessments.

## `GET /metrics`

Standard Prometheus exposition format. See [Configuration and Topics](configuration.md#prometheus-metrics) for the metric names this service registers.
