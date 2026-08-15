# Delivery Guarantees and Idempotency

Kafka gives "at-least-once" delivery by default: a consumer restart, rebalance, or crash can cause the same record to be redelivered. This pipeline is built around that assumption rather than trying to avoid it — every layer is designed so redelivery is safe.

## One database transaction per event

`Postgres.Evaluate` (`internal/store/postgres.go`) does all of the following inside a single Postgres transaction:

1. Insert the raw transaction with `ON CONFLICT DO NOTHING` on its primary key.
2. If that insert affected zero rows, the transaction already exists — fetch and return its existing assessment instead of scoring again. This is the idempotency check: a redelivered Kafka record becomes a no-op read, not a duplicate write.
3. Otherwise, query durable history (10-minute transaction count, 1-hour spend, seen-merchant/seen-country flags) from prior rows in the same table.
4. Invoke the scoring callback (feature extraction → model score → policy decision) with that history.
5. Insert the resulting assessment and an outbox row.
6. Commit.

Everything commits together or nothing does — there's no window where a transaction row exists without its assessment, or vice versa.

## Kafka offsets commit only after success

`stream.Consumer.handleRecord` calls the handler and only commits the record's offset if the handler returns `nil`. If Postgres or Kafka is unreachable, the handler returns a (non-permanent) error, the offset is _not_ committed, and the same record will be redelivered — either on the next poll (transient network blip) or after a consumer restart. Combined with the idempotent insert above, redelivery after a partial failure never produces a duplicate assessment.

## Permanent vs. transient failures

`stream.Consumer.handleRecord` distinguishes two failure classes:

- **Permanent** — wrapped in `stream.Permanent(err)`, used for JSON decode failures and `domain` validation failures (malformed schema version, missing IDs, unsupported currency, etc.). These can never succeed on retry, so the record is published to `fraud.dead-letter.v1` with the original payload and error, then its offset is committed — the consumer moves past it rather than retrying forever.
- **Transient** — anything else (a down Postgres, a Kafka write failure). These retry with exponential backoff starting at 100ms and capping at 5s, without committing the offset, so the same record is retried indefinitely until it succeeds or the service is stopped.

This means a downed dependency stalls that consumer (with growing backoff and error logs) rather than dropping data, while a genuinely malformed event doesn't block the whole partition forever.

## The outbox decouples commit from publish

Because the assessment insert and the outbox row insert happen in the same Postgres transaction, "the assessment was persisted" and "an outbox row exists for it" are always true together. The separate `Outbox.Run` loop (`internal/service/outbox.go`) polls every 250ms, fetches up to 100 unpublished rows, publishes each to Kafka, and marks it published — one row at a time, so a crash mid-batch just leaves the remaining rows unpublished for the next tick, never double-published from Kafka's perspective in normal operation (a crash between "Kafka publish succeeded" and "mark published" would republish that one row — the trade-off is at-least-once _outbound_ delivery too, consistent with the rest of the pipeline).

## What this buys you

A transaction can be redelivered any number of times, from any point in the pipeline, and the outcome converges to exactly one assessment row and eventually one outbox publish — without a distributed transaction or exactly-once Kafka semantics.
