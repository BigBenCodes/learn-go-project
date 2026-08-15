# Code Review Log

A running record of code review findings and what was done about them. Each
entry keeps the defect, the reason it mattered, the fix, and the test that now
guards it, so the reasoning survives after the diff scrolls out of view.

Newest review first.

---

## 2026-08-15 — full-codebase review at `e4492f9`

Two independent passes over the whole Go tree plus the embedded dashboard.
Baseline before the review: `go vet` clean, `go build` clean,
`go test -race ./...` passing, no races. All of the defects below were latent —
none of them broke the build or an existing test, which is the main lesson from
this round.

### Summary

| # | Severity | Area | Defect |
|---|----------|------|--------|
| 1 | High | `httpapi/ui/dashboard.html` | Stored XSS: Kafka payloads concatenated into `innerHTML` |
| 2 | High | `store/postgres.go` | Unscoped `ON CONFLICT` turned a bad event into an infinite retry loop |
| 3 | High | `store/postgres.go` | Pagination cursor lost sub-microsecond precision and repeated a row |
| 4 | Medium | `store/postgres.go` | `StoreLabel` silently dropped labels on an `event_id` collision |
| 5 | Medium | `httpapi/server.go` | `GET /` was a catch-all, so nothing returned 404 |
| 6 | Medium | `store/postgres.go` | Outbox rows were not claimed, so replicas would double-publish |
| 7 | Medium | `stream/kafka.go` | `Consumer.Run` returned without joining its workers |
| 8 | Medium | `cmd/simulator` | Interrupt closed the producer under a live goroutine |
| 9 | Medium | `cmd/simulator` | `--rate 0` collapsed every event onto one timestamp |
| 10 | Medium | CI | No Go workflow — `go test` never ran on push |
| 11 | Low | `model/logistic.go` | Tied signal contributions ordered non-deterministically |
| 12 | Low | `cmd/fraud-service` | Log level hardcoded, so every `Debug` call was unreachable |
| 13 | Low | assorted | Deprecated collector, dead error check, NaN thresholds, unchecked writes |

---

### 1. Stored XSS from Kafka payloads into the dashboard (High)

**Where:** `internal/httpapi/ui/dashboard.html`, `renderTx`

`renderTx` built each table row by string concatenation into `innerHTML`:

```js
tr.innerHTML = "<td>" + t.transaction_id + "</td>" + ...
               "<td>" + t.merchant.id + " (" + t.merchant.category + ")</td>" + ...
```

**Why it mattered.** Every one of those values comes from `transactions.raw_event`
— the verbatim consumed Kafka message. `domain.TransactionCreated.Validate`
only checks `transaction_id`, `merchant.id` and `merchant.category` for
non-emptiness; `merchant.Country` is the only field with a format check. A
producer emitting a category of `<img src=x onerror=...>` therefore gets script
execution in every operator's browser, same-origin with the `/v1/*` API. This is
the only finding in this round that is exploitable from outside the process.

The other cells turned out not to be injectable: `currency` is pinned to `GBP`
by validation, and `fmtTime` routes through `new Date().toLocaleString()`.

**Fix.** Cells are built with `document.createElement` and `textContent`, never
`innerHTML`. The action badge maps through a known-values allowlist before it is
used as a CSS class.

**Guarded by.** `TestDashboardNeverConcatenatesIntoInnerHTML` asserts that every
remaining `innerHTML` assignment in the page is a single string literal — any
concatenation fails the test. `TestDashboardBuildsCellsWithTextContent` catches
deletion of the helpers.
`domain.TestTransactionValidateAcceptsHostileStrings` documents the other half
of the contract: validation deliberately accepts these strings, so escaping is
the display layer's job.

### 2. Unscoped `ON CONFLICT` wedged a partition forever (High)

**Where:** `internal/store/postgres.go`, `Postgres.Evaluate`

The `transactions` table has `PRIMARY KEY (transaction_id)` *and*
`event_id TEXT NOT NULL UNIQUE`. The insert used a bare
`ON CONFLICT DO NOTHING`, which catches **both** constraints.

**Why it mattered.** The `RowsAffected() == 0` branch reads a conflict as "this
is a redelivery" and looks up the existing assessment. For an event with a *new*
`transaction_id` but a reused `event_id`, no such assessment exists, so the
lookup returned `pgx.ErrNoRows`, which was wrapped as an ordinary error. Ordinary
errors are treated as transient by `stream.Consumer.handleRecord`, which retries
forever at a 5s backoff ceiling with no attempt cap and no dead-lettering.
Because a worker owns a whole partition (`partition % workers`), that partition
stops advancing permanently and its offsets never commit.

`delivery-guarantees-and-idempotency.md` already claimed the conflict was scoped
"on its primary key", so the code and the documented design had diverged.

**Fix.** Three parts:

- The conflict target is now explicit: `ON CONFLICT (transaction_id) DO NOTHING`.
  An `event_id` collision surfaces as a real error instead of being mistaken for
  a redelivery.
- `store.ErrUnprocessable` marks errors that can never succeed on retry — a
  Postgres unique violation (SQLSTATE `23505`) outside the idempotency key, or a
  duplicate transaction whose assessment is missing.
- `service.Processor` maps `store.IsUnprocessable(err)` to `stream.Permanent`,
  so those records go to `fraud.dead-letter.v1` and the partition keeps moving.

The distinction that matters: a *down database* must still retry forever, and it
still does. Only a genuine data conflict is dead-lettered.

**Guarded by.** `store.TestUnprocessableTagsUniqueViolations`, which also asserts
the negative case — a plain connection error must stay retryable.

### 3. Pagination cursor lost microseconds and repeated a row (High)

**Where:** `internal/store/postgres.go`, `ListTransactions` / `scanRecord`

The cursor was built from `record.Transaction.OccurredAt`, which is unmarshalled
from the `raw_event` JSONB at **nanosecond** precision. It was then compared
against the `t.occurred_at` **column**, which pgx stores as `timestamptz` —
**microsecond** precision.

**Why it mattered.** When a timestamp carries sub-microsecond digits, the cursor
is strictly greater than the value actually stored, so
`(occurred_at, transaction_id) < (cursor)` still matches the boundary row and it
reappears at the head of the next page. This is reachable with ordinary
simulator settings, because the pacing interval is `time.Second / rate`:

| `--rate` | JSON timestamp | Stored column | Cursor > stored? |
|----------|----------------|---------------|------------------|
| 100 | `…25.01Z` | `…25.01Z` | no |
| 60 | `…25.016666666Z` | `…25.016666Z` | **yes** |
| 7 | `…25.142857142Z` | `…25.142857Z` | **yes** |
| 3 | `…25.333333333Z` | `…25.333333Z` | **yes** |

**Fix.** Both list and get queries now select `t.occurred_at` and
`t.transaction_id` as their own columns, and `scanRecord` returns a `cursor`
struct read from those columns rather than from the decoded JSON. The ordering
key is now the same value on both sides of the comparison.

**Guarded by.** `store.TestScanRecordTakesCursorFromColumnNotRawEvent`, which
feeds `scanRecord` a column value at microsecond precision and a `raw_event`
at nanosecond precision and asserts the cursor follows the column.

### 4. `StoreLabel` silently dropped labels (Medium)

**Where:** `internal/store/postgres.go`, `Postgres.StoreLabel`

Same conflation as finding 2, on `fraud_labels`. A label whose `event_id`
collided returned zero rows affected, so the processor logged "duplicate label
ignored", incremented the `Duplicates` counter, and committed the offset. The
label was lost with no error and no dead-letter, and the transaction stayed
unlabelled forever — quietly skewing every number `ModelMetrics` reports.

**Fix.** `ON CONFLICT (transaction_id) DO NOTHING`, with unique violations
tagged `ErrUnprocessable` and dead-lettered by the processor as in finding 2.

### 5. `GET /` was a catch-all (Medium)

**Where:** `internal/httpapi/server.go`

Go 1.22's `ServeMux` treats `GET /` as the fallback for every unmatched GET, so
the dashboard answered **200 with HTML** for `/v1/bogus`,
`/v1/transaction/tx-1` (typo), and `/v1/transactions/x/y`.

**Why it mattered.** `fraudctl` failed with `decode response: invalid character
'<'` instead of a usable error; a monitor pointed at a mistyped path read as
healthy; and the `id == ""` guard in `getTransaction` was unreachable.

**Fix.** `GET /{$}`, which matches only the root path.

**Guarded by.** `httpapi.TestUnknownPathsReturn404`.

### 6. Outbox rows were not claimed (Medium)

**Where:** `internal/store/postgres.go`

`FetchOutbox` selected unpublished rows with no `FOR UPDATE SKIP LOCKED` and no
lease column, and `MarkOutboxPublished` was a separate call on the pool. Safe
with one instance; with two, both outbox loops fetch the same 100 rows every
250ms and publish each assessment repeatedly.

**Fix.** `FetchOutbox` and `MarkOutboxPublished` are replaced by
`PublishOutbox(ctx, limit, publish)`, which claims rows with
`FOR UPDATE SKIP LOCKED`, publishes each through the callback, and writes
`published_at` — all in one transaction. A second replica skips the locked rows
instead of republishing them.

Two deliberate trade-offs, both documented at the call site:

- A pool connection stays checked out for the duration of the batch's Kafka
  writes. That is the cost of keeping publishing **at-least-once** (publish,
  then mark) rather than at-most-once (mark, then publish).
- A publish failure stops the batch and returns the error, but rows published
  before it are still committed as published, so one undeliverable row no longer
  forces the rows ahead of it to be re-sent.

**Guarded by.** `service.TestOutboxFlushPublishesAndCountsRows` and
`service.TestOutboxFlushKeepsProgressBeforeAFailure`.

### 7. `Consumer.Run` did not join its workers (Medium)

**Where:** `internal/stream/kafka.go`

`Run` spawned `c.workers` goroutines and, on cancellation, returned `nil`
immediately. The deferred channel close fired, but nothing waited for the
workers to drain.

**Why it mattered.** In `cmd/fraud-service`, `group.Wait()` returns and the
deferred `Consumer.Close()`, `Producer.Close()` and `pool.Close()` then run while
handlers may still be inside `store.Evaluate`. A late `CommitRecords` lands on an
already-closed kgo client. This happened on every SIGTERM.

This was already listed as finding 13 in `deep-dive/fraud-pipeline-2026-08-15.md`
and dismissed as benign because "the `ctx.Err() != nil` guard short-circuits
immediately". That reasoning does not hold: the guard only runs at the top of
`handleRecord`'s retry loop, so a worker already inside `handler(ctx, ...)` keeps
going. It happened to survive for a different reason — `pgxpool.Close()` blocks
until connections are returned — and the kgo client is closed *before* the pool
(defers run LIFO), so the commit path was genuinely exposed.

**Fix.** A `sync.WaitGroup` joins the workers before `Run` returns.
`cancel()` is called first, inside the same deferred function: on the error-return
path the parent context is still live, so a worker retrying a transient failure
would otherwise never stop and `wg.Wait()` would deadlock.

### 8. Simulator closed the producer under a live goroutine (Medium)

**Where:** `cmd/simulator/main.go`

On `Ctrl-C`, `run` closed the label queue and returned immediately, so the
deferred `producer.Close()` could fire while `publishLabels` was still inside
`producer.Publish`. The normal completion path already waited; the two interrupt
paths did not.

**Fix.** A `stop` helper closes the queue and waits for the publisher goroutine
on every exit path. A companion `failed` helper covers the paths where the
goroutine's result has already been received, so `stop` is never called twice on
a spent channel.

### 9. `--rate 0` collapsed every event onto one timestamp (Medium)

**Where:** `cmd/simulator/main.go`

Event timestamps were derived from the pacing interval
(`start.Add(emitted * interval)`), and `interval` is zero when `--rate 0`
disables pacing. Every transaction in the run therefore carried the same
`occurred_at`.

**Why it mattered.** Every history feature in `Postgres.Evaluate` is computed
with `occurred_at < $2`, so nothing ever qualifies: `tx_count_10m` and `spend_1h`
stay 0 and `new_merchant` and `new_country` stay 1 for the entire run. The flag
is documented as the fast-load path, so it silently produced a dataset in which
three of the nine features can never fire.

**Fix.** Timestamps advance on their own step, `eventStep(interval)`, which falls
back to a 10ms default when pacing is off.

**Guarded by.** `TestEventStepNeverCollapsesTimestamps` and
`TestEventStepProducesDistinctTimestamps`.

### 10. No Go CI (Medium)

`.github/workflows/` contained only the docs deployment, so `make test` never
ran on push. Added `.github/workflows/go.yml`: gofmt check, `go mod tidy`
verification, `go vet`, `go build`, and `go test -race` on push and pull request.

The coverage picture at review time explains why this mattered — the defects
clustered precisely where the tests were not:

| Package | Coverage before | Findings |
|---------|-----------------|----------|
| `internal/store` | 0.0% | 4 (three of them high) |
| `internal/stream` | 0.0% | 1 |
| `internal/domain` | 0.0% | — (gatekeeper for findings 1 and 2) |
| `internal/httpapi` | 39.0% | 1 |
| `internal/model` | 52.2% | 1 |
| `internal/service` | 60.0% | 1 |
| `internal/policy` | 87.5% | 1 (low) |
| `internal/simulator` | 90.9% | — |
| `internal/features` | 100% | — |

Tests were added for `store`, `stream`, `domain` and `cmd/simulator`, which had
none at all.

### 11. Non-deterministic signal ordering (Low)

**Where:** `internal/model/logistic.go`

Signals are built by ranging `Artifact.Weights` (a map, so already randomly
ordered) and then sorted with the unstable `sort.Slice` on absolute
contribution. Ties — common, since a domestic daytime first transaction zeroes
several features at once — resolved differently on each call, so identical input
persisted different `assessments.signals` JSON and different outbox payloads
from run to run.

**Fix.** `sort.SliceStable` with a feature-name tiebreaker.

**Guarded by.** `model.TestScoreOrdersTiedSignalsDeterministically`.

### 12. Hardcoded log level (Low)

**Where:** `cmd/fraud-service/main.go`

The handler was built with a literal `slog.LevelInfo` and there was no flag or
environment variable, so every `Debug` call in the codebase was unreachable
without a rebuild — including the per-request log in `loggingMiddleware` and the
non-action branch of `Processor.HandleTransaction`.

**Fix.** `--log-level` flag, defaulting to `LOG_LEVEL` then `info`, parsed via
`slog.Level.UnmarshalText` so it accepts `debug`, `info`, `warn` and `error`.

### 13. Assorted low-severity items

- `prometheus.NewGoCollector` / `NewProcessCollector` are deprecated; switched to
  the `collectors` package.
- The `err != http.ErrServerClosed` check in `cmd/fraud-service` was dead —
  `api.Run` already maps that to `nil` — and used `!=` rather than `errors.Is`.
  Removed.
- `policy.New` accepted `NaN` thresholds, which compare false against every
  bound and produce a policy that never fires. Now rejected explicitly, with
  `policy.TestNewRejectsInvalidThresholds` covering it.
- The simulator split its broker list with a bare `strings.Split` while the
  service trimmed blanks. Both now use `stream.ParseBrokers`, covered by
  `stream.TestParseBrokers`.
- Unchecked `w.Write` in `httpapi/ui.go` and ignored `json.Marshal` errors in the
  simulator are now handled.

### Known trade-offs left alone

These were already recorded in `deep-dive/fraud-pipeline-2026-08-15.md` as
deliberate and remain so: no `OnPartitionsRevoked` handler, scoring inside the
open transaction, unvalidated feature names across the `model`/`features`
boundary, unbounded `log_amount`, the missing index behind `ListTransactions`,
the dashboard re-reading its embedded asset per request, `fraudctl stats`
measuring a tautology, the latency histogram observing only the created path,
and `time.Now()` bypassing the injected clock for that histogram.

Finding 7 above was on that list too, and was fixed rather than kept — the
justification recorded for it was wrong.
