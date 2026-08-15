# Run the Suggested Experiments

These recipes exercise the parts of the pipeline that don't show up just from running it once — restart safety, idempotency, and swapping pieces behind their interfaces. Each assumes you've completed [Getting Started](../tutorials/getting-started.md) at least once.

## 1. Kill Postgres mid-stream and watch it recover

1. Start the service and a simulator run (`--count 0` so it keeps going).
2. While events are flowing, stop Postgres: `docker compose stop postgres`.
3. Watch the service logs — you should see `event processing failed; retrying` with growing backoff (100ms up to a 5s cap), and Kafka offsets are **not** committed while this happens.
4. Restart Postgres: `docker compose start postgres`.
5. The service picks up automatically and resumes from the same offset — no restart needed.

**What this demonstrates:** transient failures retry indefinitely rather than dropping data or committing a Kafka offset for an event that never got processed.

## 2. Rewind the consumer group and verify no duplicate rows

1. Note the current row count: `go run ./cmd/fraudctl stats`.
2. Stop the service.
3. Reset the `fraud-scorer-v1` consumer group's offsets to the start of `transactions.v1` (via `rpk group seek fraud-scorer-v1 --to start -X brokers=localhost:19092`, run against the Redpanda container, or an equivalent Kafka admin tool).
4. Restart the service and let it reprocess every transaction.
5. Compare `go run ./cmd/fraudctl stats` again — the assessment count should be unchanged.

**What this demonstrates:** `Postgres.Evaluate` inserts the raw transaction with `ON CONFLICT DO NOTHING`; a redelivered event that already exists is detected (zero rows affected) and treated as a no-op rather than a duplicate assessment. See [Delivery Guarantees and Idempotency](../explanation/delivery-guarantees-and-idempotency.md).

## 3. Change policy thresholds without touching the model

```sh
go run ./cmd/fraud-service --review-threshold 0.4 --escalate-threshold 0.7
```

Re-run the simulator and compare `fraudctl stats` / `list --action escalate` output against the defaults (`0.65` / `0.85`). The model's scores are identical — only the score-to-action mapping changed.

## 4. Add a second model artifact and compare metrics by version

1. Copy `configs/model.json` to `configs/model-v2.json` and adjust the `version` field and some `weights`.
2. Start a second instance of `fraud-service` pointed at it: `go run ./cmd/fraud-service --model configs/model-v2.json --http-address :8081`.
3. Run the simulator against each instance's Kafka consumer group in turn (or run both concurrently against the same topic — they're independent consumer groups if you also change `--brokers`/group naming, otherwise they'll compete for the same partitions).
4. Compare `go run ./cmd/fraudctl --url http://localhost:8080 stats --model-version dummy-logistic-v1` against the v2 instance's stats.

**What this demonstrates:** `internal/store.Postgres.ModelMetrics` scopes its confusion matrix to one `model_version`, so multiple model versions' assessments can coexist in the same database and be compared independently.

## 5. Replace the Go scorer with a remote service

`service.Processor` depends on a `Scorer` interface (`Version() string`, `Score(domain.FeatureVector) (float64, []domain.Signal)`), not the concrete `model.Logistic` type. Implement that interface with a client that calls out to an HTTP or gRPC inference service instead, and wire it in where `cmd/fraud-service/main.go` currently calls `model.Load`. Nothing else in `service`, `store`, or `httpapi` needs to change.

See [Design Decisions and Testability](../explanation/design-decisions-and-testability.md) for why the interfaces are shaped this way.
