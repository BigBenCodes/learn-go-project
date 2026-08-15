# Deep Dive: Go Fraud Pipeline (whole codebase)

**Generated**: 2026-08-15
**Mode**: full · **Level**: mid
**Scope**: all 24 Go files (2,192 LOC) + `internal/store/migrations/001_init.sql` + `configs/model.json`
**Verified**: `go vet ./...` clean · `go test -race ./...` all packages pass

---

## Overview

### What This Code Does

An asynchronous card-fraud scoring pipeline. A simulator publishes `TransactionCreated` events to Kafka (Redpanda); `fraud-service` consumes them, derives behavioural features from the account's own history in Postgres, scores them with a hand-authored logistic regression, maps the score to a recommended action, and persists everything atomically. Assessments are re-published to Kafka through a transactional outbox. Delayed ground-truth `FraudLabel` events arrive on a separate topic and separate consumer group, letting a SQL confusion matrix grade the model after the fact. An HTTP API + embedded dashboard + read-only CLI expose the results.

The interesting part is not the model — it is a fixed nine-weight linear function loaded from JSON. The interesting part is **everything that makes an at-least-once event stream produce exactly-one logical outcome**.

### Why This Approach Was Chosen

The design repeatedly picks *boring, verifiable correctness* over cleverness:

- **Postgres as the arbiter of truth, Kafka as transport.** Every "did I already do this?" question is answered by a unique constraint in the database, not by Kafka semantics. This is why redelivery is free.
- **One database transaction per event**, with the scoring function injected *into* it as a callback. History, assessment, and outbox row commit together or not at all.
- **Score and decision are separate packages.** `model` returns a probability; `policy` turns it into `no_action`/`review`/`escalate`. Re-tuning thresholds is a config change, not a model change.
- **Interfaces declared where they are consumed** (`service.Scorer`, `service.Repository`, `service.Publisher`), so the orchestration layer unit-tests against fakes with no Docker.
- **Every score is explainable.** `Score` returns per-feature contributions sorted by magnitude, persisted as JSONB. You can always answer "why was this flagged?".

### Context

This is the standard shape for any *decision-logging* pipeline where the decision is advisory and the input stream is unreliable: fraud, risk, moderation, alerting, lead scoring. The pattern set here (idempotent consume → transactional write → outbox publish → delayed label evaluation) transfers directly.

Note what it deliberately is **not**: it is not in the payment authorization path. Because the pipeline is asynchronous, an `escalate` is a recommendation produced *after* the transaction, not a decline. That single constraint is what permits the retry-forever error strategy — nothing here has a latency SLA that failing would violate.

---

## Architecture, annotated by guarantee

```text
                                   ┌─ guarantee established here ─┐
simulator ──transactions.v1──► Redpanda ──► stream.Consumer
   │                                          │  · manual offset commit AFTER handler success
   │                                          │  · partition % workers → per-partition order kept
   │                                          │  · Permanent(err) → DLQ; anything else → backoff retry
   │                                          ▼
   │                                     service.Processor  · decode → validate → delegate
   │                                          ▼
   │                                     store.Evaluate  ◄── ONE Postgres tx:
   │                                          │              INSERT tx ON CONFLICT DO NOTHING  ← idempotency
   │                                          │              SELECT history WHERE occurred_at < $now ← point-in-time
   │                                          │              callback: features → score → policy
   │                                          │              INSERT assessment + INSERT outbox
   │                                          ▼              COMMIT  ← atomicity
   │                                     service.Outbox  · 250ms poll → publish → mark published
   │                                          ▼
   └──delayed fraud.labels.v1──►         fraud.assessments.v1
        (separate consumer group —
         the model can never read it)

fraudctl / curl / dashboard ──► httpapi (:8080) ──► Postgres (keyset pagination, SQL confusion matrix)
```

---

## Key Components

| Package | Responsibility | Key symbols | Why it is its own package |
|---|---|---|---|
| `domain` | Event/record types + validation | `TransactionCreated.Validate`, `FraudLabel.Validate`, `History`, `Assessment`, `Signal` | Zero dependencies on anything else — every layer can import it without cycles |
| `features` | Transaction + history → numeric vector | `Extract` | Pure function; the seam where "banking facts" become "model inputs" |
| `model` | Load + apply the linear scorer | `Load`, `New`, `Logistic.Score` | Swappable behind `service.Scorer`; knows nothing about thresholds |
| `policy` | Score → recommended action | `New`, `Thresholds.Decide` | Business risk appetite, tuned independently of the model |
| `service` | Orchestration + metrics | `Processor`, `Outbox`, `Metrics`, `PipelineSnapshot` | The only place that knows the *sequence* of steps |
| `store` | Postgres, transactions, queries | `Evaluate`, `StoreLabel`, `ListTransactions`, `ModelMetrics`, `FetchOutbox` | All SQL lives here; the transaction boundary is owned here |
| `stream` | Kafka producer/consumer, DLQ, retry | `Producer`, `Consumer.Run`, `handleRecord`, `Permanent` | Delivery semantics isolated from business logic |
| `httpapi` | HTTP surface + embedded dashboard | `New`, `listTransactions`, `encodeCursor`, `dashboard` | Read path only — never writes |
| `simulator` | Deterministic mock event generator | `Generator.Next` | Seeded RNG makes whole runs reproducible |

Three binaries in `cmd/`: `fraud-service` (daemon), `simulator` (load generator), `fraudctl` (read-only CLI). Root `main.go` is a 10-line usage stub.

---

## Code Walkthrough

### `internal/domain/domain.go` — the vocabulary

**Purpose**: types every other package speaks, plus validation.

Notable choices:

```go
// domain.go:9
const SchemaVersion = 1
```
Every event carries `schema_version`, and both `Validate` methods reject anything else (`domain.go:47`, `domain.go:83`). This is *fail-closed* schema evolution: a v2 producer deployed early gets dead-lettered rather than silently misinterpreted. Cheap, and the DLQ makes it recoverable.

```go
// domain.go:40
AmountMinor   int64  `json:"amount_minor"`
```
Money as integer minor units (pence), never `float64`. Non-negotiable in financial code — `0.1 + 0.2 != 0.3` in binary floating point, and rounding errors accumulate across millions of rows.

`Validate` at `domain.go:46-71` uses a `switch` for the enum check with `return nil` in the valid branch — so the function's happy path exits through the enum switch. Slightly unusual shape, but it makes the "unknown entry mode" case impossible to forget.

`History` (`domain.go:95`) is a plain struct of four pre-aggregated values, not a slice of past transactions. That is a deliberate narrowing: the store computes aggregates in SQL and hands `features` only what it needs, so no unbounded row set crosses the layer boundary.

`FeatureVector` is `map[string]float64` (`domain.go:102`). Flexible — a new model artifact can reference new features without changing Go types — but string-keyed, so typos are silent. See Finding 4.

### `internal/features/features.go` — banking facts → numbers

**Purpose**: pure, deterministic feature extraction.

```go
// features.go:43
"log_amount": math.Log1p(float64(tx.AmountMinor) / 100),
```
`Log1p(x)` computes `ln(1+x)` accurately for small `x`. Log-scaling amounts is standard: it compresses a long-tailed distribution so a £10,000 transaction is not 100× more influential than a £100 one in a linear model.

```go
// features.go:48-49
"tx_count_10m": math.Min(float64(history.TransactionCount10m), 10) / 10,
"spend_1h":     math.Min(float64(history.SpendMinor1h)/100, 2000) / 2000,
```
Clamp-then-normalise into `[0,1]`. This caps the influence of any single feature and makes the weights in `model.json` directly comparable — a weight of `1.5` means "worth 1.5 logits at full saturation". Every other feature is a 0/1 flag, so the whole vector is bounded **except `log_amount`**. See Finding 5.

Everything else is a boolean flag materialised as `0.0`/`1.0`. Verbose in Go (no ternary), but explicit.

### `internal/model/logistic.go` — the scorer

**Purpose**: load a versioned artifact, produce a probability plus an explanation.

```go
// logistic.go:48-60
func (m *Logistic) Score(features domain.FeatureVector) (float64, []domain.Signal) {
	logit := m.artifact.Intercept
	for feature, weight := range m.artifact.Weights {
		value := features[feature]
		contribution := value * weight
		logit += contribution
		signals = append(signals, domain.Signal{...})
	}
	sort.Slice(signals, func(i, j int) bool {
		return math.Abs(signals[i].Contribution) > math.Abs(signals[j].Contribution)
	})
	return 1 / (1 + math.Exp(-logit)), signals
}
```

Three things worth internalising:

1. **Logistic regression is just a dot product plus a sigmoid.** `logit = intercept + Σ(wᵢ·xᵢ)`, then `σ(logit) = 1/(1+e^(-logit))` squashes ℝ → (0,1). There is no library here because there does not need to be one.
2. **Explainability is free in a linear model.** Each `contribution = value × weight` is exactly that feature's additive effect on the logit. Sorting by `|contribution|` gives you "the top reasons" with no approximation. This is why linear models survive in regulated domains where a gradient-boosted tree would need SHAP values to say the same thing.
3. **`sort.Slice` is not stable, and map iteration order is random.** Two features with identical `|contribution|` can swap order between runs. Harmless for display, but it is why the test asserts `signals[0].Feature` rather than the full slice.

Worked example — a fraud-shaped transaction (crypto merchant, LT, ecommerce, £1,000, unseen merchant/country, daytime, no recent activity) against `configs/model.json`:

| feature | value | weight | contribution |
|---|---|---|---|
| `log_amount` | ln(1001) ≈ 6.909 | 0.55 | **3.800** |
| `foreign` | 1 | 1.25 | 1.250 |
| `risky_merchant` | 1 | 1.60 | 1.600 |
| `card_not_present` | 1 | 1.10 | 1.100 |
| `new_country` | 1 | 0.75 | 0.750 |
| `new_merchant` | 1 | 0.50 | 0.500 |
| intercept | — | — | **−6.000** |
| | | **logit** | **3.000** |

σ(3.0) = 0.953 → ≥ 0.85 → `escalate`. The intercept of −6.0 is the base rate prior: with all flags at zero and a trivial amount, the score is near zero.

### `internal/policy/policy.go` — score → action

29 lines, and the shortest file that matters most.

```go
// policy.go:15
if review < 0 || escalate > 1 || review >= escalate {
```
One expression enforces `0 ≤ review < escalate ≤ 1`. The transitive chain covers the two constraints it does not state literally (`escalate ≥ 0`, `review ≤ 1`). Tight, and the test (`policy_test.go`) checks exactly the boundary values `0.649 / 0.65 / 0.849 / 0.85` — the only places a threshold bug can hide.

Keeping this separate from `model` means "we're seeing too many false positives" is answered by a flag change (`-review-threshold`), never by touching scoring code.

### `internal/store/postgres.go` — where correctness is actually enforced

**Purpose**: all SQL, and the single transaction boundary per event.

This is the most important file in the repo. `Evaluate` (`postgres.go:49-133`) does five things in one transaction:

**1. Idempotent claim (`postgres.go:60-80`)**
```go
INSERT INTO transactions (...) VALUES (...) ON CONFLICT DO NOTHING
...
if tag.RowsAffected() == 0 {
	assessment, err := getAssessment(ctx, tx, event.TransactionID)
	...
	return assessment, false, nil
}
```
`RowsAffected() == 0` *is* the duplicate detector. No `SELECT ... IF NOT EXISTS` race, because the uniqueness check and the insert are one atomic statement against the primary key. On a duplicate it returns the *original* assessment and `created=false`, so a replayed event is observably a no-op rather than a second decision.

**2. Point-in-time history (`postgres.go:83-97`)**
```sql
SELECT
  count(*) FILTER (WHERE occurred_at >= $2::timestamptz - interval '10 minutes' AND occurred_at < $2::timestamptz),
  COALESCE(sum((raw_event->>'amount_minor')::bigint) FILTER (...), 0),
  bool_or(merchant_id = $3 AND occurred_at < $2::timestamptz),
  bool_or(merchant_country = $4 AND occurred_at < $2::timestamptz)
FROM transactions
WHERE account_id = $1 AND occurred_at >= $2::timestamptz - interval '90 days'
```

Two subtleties here, and both are load-bearing:

- **Every window is `occurred_at < $2`, strictly.** The event's own row (already inserted) is excluded from its own history, and so is anything with a later event time. This makes features a pure function of `(event, event time)` — *independent of arrival order*. Reprocess the topic in a different order and you get byte-identical assessments. In ML terms this is point-in-time correctness: it prevents the label-adjacent leakage where a model at scoring time sees data that did not exist yet.
- **The insert-before-query ordering is required for NULL-safety, not just determinism.** `bool_or` over zero rows returns `NULL`, and `Scan` into a plain `bool` would error. Because the current transaction's own row was inserted first and always satisfies the outer `WHERE`, the aggregate always sees ≥ 1 row, so `bool_or` yields `false` rather than `NULL` for an account's very first transaction. Moving the `INSERT` after the `SELECT` — which looks like a harmless reorder — would break every account's first event. Worth a comment in the code.

`FILTER (WHERE ...)` is SQL-standard conditional aggregation: four different windows computed in a single pass over one index range, rather than four subqueries.

**3. Scoring inside the transaction (`postgres.go:99`)**
```go
assessment, err := evaluate(history)
```
The `Evaluator` callback is `func(domain.History) (domain.Assessment, error)`, supplied by `Processor` (`processor.go:57`). Inverting control this way lets the store own the transaction while the service owns the business logic — neither imports the other's concerns.

This is safe **because `Score` is pure CPU with no I/O**. If the scorer ever became a network call to a model server, this would hold a Postgres connection and row locks open across a remote round-trip. That constraint is currently implicit; it is the thing to remember before swapping the model.

**4–5. Assessment + outbox insert, then commit (`postgres.go:107-131`)** — both writes and the outbox row land in the same commit as the transaction row. Nothing can exist without its outbox event.

`defer func() { _ = tx.Rollback(ctx) }()` at `postgres.go:54` is the correct Go idiom: rollback after a successful commit is a no-op, so the deferred call safely covers every early-return path.

**Other queries:**

- `ListTransactions` (`postgres.go:218`) — keyset pagination via row-value comparison `(t.occurred_at, t.transaction_id) < ($2, $3)`, fetching `limit+1` to detect a next page. Correct implementation; see Finding 6 for its index problem.
- `ModelMetrics` (`postgres.go:310`) — the entire confusion matrix in one query using four `count(*) FILTER (...)` clauses, then precision/recall/FPR computed in Go via `ratio` (`postgres.go:338`), which guards divide-by-zero. Good separation: SQL counts, Go divides.
- `scanRecord` (`postgres.go:274`) — uses `sql.NullString`/`NullTime`/`NullBool` for the `LEFT JOIN fraud_labels` columns, and only builds `record.Label` when `labelEvent.Valid`. This is how you model "may not have arrived yet" without a sentinel value.

**Migration** (`001_init.sql`): applied via `go:embed` on startup (`postgres.go:17`, `postgres.go:40`) with `CREATE TABLE IF NOT EXISTS` — idempotent, no migration tool. Fine for one file; it does not survive a second migration that needs ordering or a down-path.

The comment `-- Labels deliberately have no transaction foreign key: real labels may arrive first` is exactly the kind of note worth writing. It preempts a "helpful" future FK that would break the pipeline.

### `internal/stream/kafka.go` — delivery semantics

**Purpose**: everything about *how many times* an event gets processed.

**Producer** (`kafka.go:25-35`):
```go
kgo.RequiredAcks(kgo.AllISRAcks()),
kgo.ProducerBatchCompression(kgo.SnappyCompression()),
```
`AllISRAcks` waits for every in-sync replica before acking — the durable setting. `ProduceSync` (`kafka.go:38`) then makes each publish blocking. Together: slow, and impossible to lose an acked write. Correct default for financial events; you would batch and go async only under measured throughput pressure.

**The permanent-error marker** (`kafka.go:51-61`):
```go
type permanentError struct{ err error }
func (e permanentError) Unwrap() error { return e.err }
func Permanent(err error) error { ... }
```
An unexported wrapper type with an exported constructor, recovered via `errors.As` at `kafka.go:160`. Implementing `Unwrap` keeps `errors.Is`/`As` working on the *inner* error too, so wrapping loses no information. This is the idiomatic Go way to add a *classification* to an error without an enum or sentinel comparison.

**Worker pool** (`kafka.go:91-145`):
```go
worker := record.Partition % int32(c.workers)
```
Partition affinity. Records from partition *p* always route to worker `p % workers`, so per-partition ordering survives concurrency. Since the simulator keys by `AccountID` (`cmd/simulator/main.go:92`) and Kafka hashes keys to partitions, this means **per-account ordering is preserved** — which is exactly what the velocity features need. Concurrency is bounded by `workers`, not by partition count.

Note this is coarser than "one goroutine per partition": with 6 workers and 12 partitions, partitions 0 and 6 share a worker and thus block each other. Deliberate — it bounds goroutines and DB connections at the cost of some head-of-line coupling.

**Retry ladder** (`kafka.go:147-185`):
```go
backoff = min(backoff*2, 5*time.Second)
```
`min` as a builtin (Go 1.21+). Exponential backoff 100ms → 5s cap, retried *forever* without committing the offset. A downed Postgres therefore stalls the pipeline rather than dropping or dead-lettering data. The right call here: this is a batch-shaped workload with no latency SLA, so stalling is strictly better than losing.

Permanent errors take the other branch (`kafka.go:160-170`): publish to `fraud.dead-letter.v1` with topic/partition/offset/error/payload envelope, *then* commit past the record. Ordering matters — commit only after the DLQ publish is acked, so a crash mid-way replays rather than skips.

### `internal/service/processor.go` — orchestration

**Purpose**: decode → validate → delegate, and record what happened.

```go
// processor.go:17-25
type Scorer interface {
	Version() string
	Score(domain.FeatureVector) (float64, []domain.Signal)
}
type Repository interface {
	Evaluate(context.Context, domain.TransactionCreated, store.Evaluator) (domain.Assessment, bool, error)
	StoreLabel(context.Context, domain.FraudLabel) (bool, error)
}
```
Interfaces declared **in the consuming package**, not next to their implementations. This is the Go convention and it is what makes `processor_test.go` able to run the full handler against a 12-line `fakeRepository` with no Postgres. Contrast with Java/C#, where the interface usually ships with the implementation.

```go
// processor.go:49, 54, 102, 107
return stream.Permanent(fmt.Errorf("decode transaction: %w", err))
```
Decode and validation failures are classified permanent — no amount of retrying fixes malformed JSON. Everything else (implicitly, `processor.go:74`) is transient. This one-line policy is the whole error taxonomy.

```go
// processor.go:57-70
assessment, created, err := p.repository.Evaluate(ctx, event, func(history domain.History) (domain.Assessment, error) {
	vector := features.Extract(event, history)
	score, signals := p.model.Score(vector)
	return domain.Assessment{... RecommendedAction: p.policy.Decide(score) ...}, nil
})
```
The closure captures `event` and the processor's collaborators, and runs inside the DB transaction. Read top-to-bottom it is the entire business logic of the system: features → score → policy.

```go
// processor.go:33, 39
now func() time.Time
...
now: time.Now,
```
Injected clock so `AssessedAt` is deterministic in tests (`processor_test.go` overrides it). Standard technique for testing time-dependent code without a clock library.

```go
// processor.go:84-87
log := p.logger.Debug
if assessment.RecommendedAction != domain.ActionNone {
	log = p.logger.Info
}
```
Method value assigned to a variable to pick a log level. Neat: the ~99% of transactions that are `no_action` log at Debug and vanish in production, while anything actionable logs at Info. Log volume control without an `if/else` around two near-identical call sites.

### `internal/service/outbox.go` — the publisher loop

```go
// outbox.go:32
ticker := time.NewTicker(250 * time.Millisecond)
for {
	if err := o.flush(ctx); err != nil { ...log, continue... }
	select {
	case <-ctx.Done(): return nil
	case <-ticker.C:
	}
}
```
Flush-first, then wait — so shutdown does not lose a pending flush and startup does not idle 250ms. `flush` (`outbox.go:47`) fetches up to 100 unpublished rows ordered by `id`, publishes each, marks each published.

Publish-then-mark is deliberately **at-least-once**: if the process dies between the two, the event republishes on restart. The alternative (mark-then-publish) would be at-most-once and could silently drop assessments. Since every payload carries a unique `event_id`, downstream consumers can dedupe — pushing the dedup cost to the reader is the correct trade.

### `internal/service/metrics.go` + `pipeline_metrics.go`

`NewMetrics` takes a `prometheus.Registerer` rather than using the global default registry (`metrics.go:15`). That is why every test can call `NewMetrics(prometheus.NewRegistry())` without `MustRegister` panicking on duplicate registration — a very common Go/Prometheus footgun, avoided by construction.

`PipelineSnapshot` (`pipeline_metrics.go:17`) reads back out of the registry via `Gatherer.Gather()` and reshapes the same numbers for the dashboard JSON. Single source of truth: `/metrics` and `/v1/pipeline-metrics` cannot disagree, because one is derived from the other.

`latencyQuantile` (`pipeline_metrics.go:89-124`) reimplements Prometheus' `histogram_quantile`: find the first cumulative bucket whose count ≥ `q × total`, then linearly interpolate between the previous and current bucket bounds. Two edge cases handled correctly — `+Inf` upper bound returns the highest finite bound (`:113`), and a zero-width count range returns the bound directly instead of dividing by zero (`:116`).

Understand the *inherent* limitation: histogram quantiles are estimates whose accuracy depends entirely on bucket layout. With `prometheus.DefBuckets` (5ms…10s), a p99 that lands between the 5s and 10s buckets is interpolated across a 5-second-wide gap. This is not a bug in the implementation; it is why summaries exist as an alternative.

### `internal/httpapi/server.go` + `ui.go`

Standard-library-only routing using Go 1.22+ method+pattern syntax:
```go
// server.go:36-43
mux.HandleFunc("GET /v1/transactions/{id}", s.getTransaction)
```
`r.PathValue("id")` (`server.go:122`) extracts the wildcard. No chi/gorilla/gin needed for a surface this size.

All four timeouts set explicitly (`server.go:46-49`). `ReadHeaderTimeout` in particular is the Slowloris defence, and its absence is one of the most common Go HTTP vulnerabilities.

```go
// server.go:63-67
case <-ctx.Done():
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return s.server.Shutdown(shutdownCtx)
```
The shutdown context derives from `context.Background()`, **not** from the already-cancelled `ctx`. Deriving it from `ctx` is the classic mistake — graceful shutdown would abort instantly. Correct here.

Cursor encoding (`server.go:160-176`) is `base64url(RFC3339Nano + "|" + id)`. Opaque to clients, which is the point: it can change shape without breaking the API contract. It is *encoded*, not signed or encrypted, so a client can trivially forge one — acceptable, since a forged cursor only reads data the endpoint already exposes.

`ui.go` embeds and serves the dashboard, re-reading from the embedded FS per request. See Finding 8.

### `internal/simulator/generator.go` + `cmd/simulator/main.go`

```go
// generator.go:47
rng: rand.New(rand.NewSource(seed)),
```
An explicitly seeded `*rand.Rand`, not the package-level `math/rand` functions. Two consequences: runs are reproducible from `--seed`, and the generator holds its own non-shared, non-locked source. Note this makes `Generator` **not** safe for concurrent use — fine, since `cmd/simulator` calls `Next` from one goroutine.

`generator_test.go` asserts determinism directly (two generators, same seed, 100 events, `reflect.DeepEqual`). That is the right test for this property.

Fraud events are generated with a distinct signature — risky merchant category, foreign country, ecommerce entry, £500–£2,000 amounts vs the normal £2.50–£152.50 — while normal events are GB/chip/contactless. This matters when reading `fraudctl stats`: see Finding 9.

`cmd/simulator/main.go` runs a producer goroutine for delayed labels (`main.go:68`) fed by a buffered channel, with every blocking send guarded by a three-way `select` on the channel, the error channel, and `ctx.Done()` (`main.go:96-103`). That is the disciplined form — a bare `labels <- x` would deadlock on shutdown once the buffer filled.

### `cmd/fraud-service/main.go` — the supervisor

```go
// main.go:45
ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
```
`signal.NotifyContext` turns SIGINT/SIGTERM into context cancellation, which then propagates through every layer that already takes a `ctx`. One mechanism, no global shutdown flag.

```go
// main.go:90-100
group, groupCtx := errgroup.WithContext(ctx)
group.Go(func() error { return transactionConsumer.Run(groupCtx, processor.HandleTransaction) })
group.Go(func() error { return labelConsumer.Run(groupCtx, processor.HandleLabel) })
group.Go(func() error { return outbox.Run(groupCtx) })
group.Go(func() error { ...api.Run(groupCtx)... })
return group.Wait()
```
`errgroup.WithContext` gives fail-fast supervision: the first goroutine to return a non-nil error cancels `groupCtx`, which unblocks the other three, and `Wait` returns that first error. This is ~10 lines replacing a `sync.WaitGroup` + error channel + manual cancel.

Note the deliberate asymmetry: `Consumer.Run` and `Outbox.Run` return `nil` on `ctx.Done()` (`kafka.go:119`, `outbox.go:41`), so a clean shutdown is not reported as an error.

Also note the ordering of `defer`s (`main.go:52, 69, 74, 79`) — LIFO, so consumers close before the producer, which closes before the DB pool. Right order.

Config is flags-with-env-fallback (`main.go:35-42` + `env` at `:103`). Flags win over env, env wins over the hardcoded default. Twelve-factor enough, zero dependencies.

### `cmd/fraudctl/main.go`

Sub-command CLI using nested `flag.FlagSet` — the standard-library way to get `fraudctl list --limit 5` without Cobra. `flag.ContinueOnError` (`main.go:23`) returns parse errors instead of calling `os.Exit`, so `run()` stays testable. Read-only by construction: it only issues `client.Get` (`main.go:70`).

---

## Concepts Explained

### Design Patterns Used

| Pattern | Where | Why |
|---|---|---|
| Transactional Outbox | `store.Evaluate` + `service.Outbox` | Atomically commit state + intent-to-publish, avoiding the dual-write problem |
| Polling Publisher | `outbox.Run` (250ms tick) | Simplest outbox drain; no CDC infrastructure required |
| Idempotent Consumer | `ON CONFLICT DO NOTHING` + `RowsAffected()==0` | Makes at-least-once delivery produce exactly-one outcome |
| Dead Letter Queue | `stream.Consumer.publishDeadLetter` | Quarantine unprocessable events without stalling the partition |
| Dependency Inversion (consumer-side interfaces) | `service.Scorer`, `service.Repository`, `service.Publisher` | Unit tests without Docker; swappable implementations |
| Strategy | `store.Evaluator` callback | Store owns the transaction, service owns the logic |
| Keyset (cursor) pagination | `store.ListTransactions` + `httpapi.encodeCursor` | Stable, O(1) paging over a mutating table |
| Repository | `store.Postgres` behind two interfaces | All SQL confined to one package |
| Circuit-free retry with backoff | `stream.handleRecord` | Stall-and-recover instead of drop-on-failure |

### Key Technical Concepts

#### 1. Idempotent consumption (at-least-once → effectively-once)

**What**: Kafka guarantees at-least-once delivery — a consumer that crashes after processing but before committing its offset will see the same record again. Idempotent consumption means processing the same event twice produces the same end state as processing it once.

**Why used here**: The natural key (`transaction_id`, a primary key) plus `ON CONFLICT DO NOTHING` turns the duplicate check into a single atomic statement. `RowsAffected()` reports which case occurred. Combined with committing the offset only *after* the handler succeeds (`kafka.go:155`), a redelivery is a cheap no-op that returns the original assessment.

**When to use**: Any Kafka/SQS/Pub-Sub consumer that writes to a database. This is the default correct architecture, not an optimisation.

**Trade-offs**:
- Pros: no distributed transaction; survives crash at any point; replaying a topic is safe.
- Cons: requires a stable natural key in the event; the duplicate path costs a round-trip to fetch the original.

**Alternatives**:
- *Kafka exactly-once semantics (transactional producer + `read_committed`)*: works within Kafka, but does not extend to your Postgres write. Solves a different half of the problem.
- *Separate `processed_events` table*: works when the payload has no natural key, but adds a table and a write.
- *Dedup cache (Redis) with TTL*: fast, but a probabilistic guarantee — an eviction reintroduces the duplicate.

**Prerequisites**: Kafka consumer groups and offsets · database primary keys and unique constraints · what "at-least-once" vs "exactly-once" delivery means.

#### 2. The transactional outbox and the dual-write problem

**What**: You need to update the database *and* publish an event. Doing both directly is a "dual write": if the DB commits and the publish fails, downstream never learns; if the publish succeeds and the DB rolls back, you have announced something that did not happen. There is no distributed transaction spanning Postgres and Kafka.

**Why used here**: `store.Evaluate` inserts the assessment **and** an `outbox` row in the same commit (`postgres.go:107-128`). The database is now the only thing that had to be atomic. A separate poller (`outbox.go`) publishes rows and marks them published. Neither the assessment nor its event can exist without the other.

**When to use**: Any time a state change must be reliably announced to another system. Fires constantly in event-driven architectures.

**Trade-offs**:
- Pros: single-resource atomicity; survives broker outage; no 2PC.
- Cons: adds publish latency (here ≤250ms); at-least-once publishing means consumers must dedupe; the outbox table needs pruning as it grows.

**Alternatives**:
- *Transaction log tailing / CDC (Debezium)*: no polling, lower latency, no outbox drain code — but a whole extra piece of infrastructure and a replication-slot dependency.
- *Two-phase commit*: theoretically correct, practically avoided — Kafka does not support XA, and 2PC couples availability of both systems.
- *Publish-then-write*: simpler, and wrong; the failure mode is a phantom event.

**Prerequisites**: database transactions and ACID · why 2PC is avoided in practice · at-least-once messaging.

#### 3. Point-in-time correct feature computation

**What**: A feature used at scoring time must only be computable from data that existed *at that event's time*. Violating this is **data leakage** — the model sees the future, scores brilliantly offline, and fails in production.

**Why used here**: Every window in the history query is bounded `occurred_at < $2` (the event's own time), never `now()`. The current row is excluded from its own aggregates. Result: `Extract(event, history)` is a pure function of the event and its timestamp, *independent of when or in what order it is processed*. Reprocess the topic backwards and every assessment is identical.

**When to use**: Any ML system where features are derived from historical events — fraud, churn, recommendation, credit. Also any system that needs replayable, auditable decisions.

**Trade-offs**:
- Pros: reproducible decisions; safe backfills; offline training and online serving compute the same thing (no training-serving skew).
- Cons: the query must scan/aggregate history per event, so it is more expensive than reading a maintained running counter; requires a trustworthy event timestamp.

**Alternatives**:
- *Feature store with point-in-time joins* (Feast, Tecton): the industrial version of exactly this idea.
- *Materialised running aggregates* (a `account_stats` row updated per transaction): O(1) reads, but the values reflect *processing* order, so they are not replayable.
- *Stream-windowed aggregation* (Kafka Streams/Flink): efficient, but out-of-order and late events become your problem to handle explicitly.

**Prerequisites**: SQL aggregates and `FILTER` clauses · the difference between event time and processing time · why train/serve skew happens.

#### 4. Permanent vs transient error classification

**What**: Failures split into two classes with opposite correct responses. *Transient* (DB down, broker unreachable) → retry, do not commit. *Permanent* (malformed JSON, failed validation) → retrying is futile; quarantine and move on.

**Why used here**: `stream.Permanent(err)` wraps an error in an unexported type recovered by `errors.As` (`kafka.go:160`). Only `processor.go` decides what is permanent — decode and validate failures. Everything else defaults to transient. Without this split you get one of two failure modes: a poison message blocking a partition forever, or a transient outage silently dead-lettering good data.

**When to use**: Every message consumer. It is the single highest-value piece of error handling in a stream processor.

**Trade-offs**:
- Pros: poison messages cannot stall the pipeline; outages stall rather than drop.
- Cons: misclassification is costly in both directions; the DLQ needs a monitoring and replay story (this project has neither yet).

**Alternatives**:
- *Retry N times then DLQ everything*: simpler, but dead-letters good data during a 10-minute database outage.
- *Retry topics with escalating delays* (the Uber/Confluent pattern): better isolation, more moving parts.

**Prerequisites**: Go's `errors.Is`/`errors.As` and error wrapping with `%w` · Kafka offset commit semantics.

#### 5. Partition-affinity worker pools

**What**: Concurrency without losing ordering. `worker := record.Partition % int32(c.workers)` guarantees every record from a given partition is handled by the same goroutine, so per-partition order is preserved while distinct partitions proceed in parallel.

**Why used here**: The velocity features (`tx_count_10m`, `spend_1h`) are meaningful only if an account's transactions are processed in order. Kafka guarantees order *within* a partition; the producer keys by `AccountID`, so one account maps to one partition; partition affinity extends that guarantee through the worker pool into the database.

**When to use**: Whenever per-key ordering matters and throughput needs more than one goroutine — which is most stateful stream processing.

**Trade-offs**:
- Pros: bounded goroutines and DB connections; strict per-key ordering; no locking.
- Cons: uneven partition load ⇒ uneven worker load; partitions sharing a worker block each other; a slow record stalls everything behind it on that worker.

**Alternatives**:
- *One goroutine per partition*: no sharing, but goroutine count follows partition count.
- *Unbounded goroutine per record*: maximum parallelism, zero ordering — invalid here.
- *Hash on the message key instead of the partition*: finer distribution, but only equivalent if the key→partition mapping is stable.

**Prerequisites**: Kafka partitioning and key hashing · Go channels and goroutines · why ordering guarantees are per-partition, never global.

#### 6. Consumer-side interfaces (Go's dependency inversion)

**What**: In Go, interfaces are defined by the package that *consumes* them, not the package that implements them, and satisfaction is structural — no `implements` keyword.

**Why used here**: `service` declares `Scorer`, `Repository`, `OutboxRepository`, `Publisher` describing exactly what it needs. `model.Logistic`, `store.Postgres`, and `stream.Producer` satisfy them without importing `service`. Dependencies point one way; tests substitute fakes trivially (`processor_test.go`'s `fakeRepository` is 12 lines).

**When to use**: Default Go practice. Define the interface at the point of use, keep it small (`Publisher` has one method).

**Trade-offs**:
- Pros: no import cycles; interfaces stay minimal; implementations need no knowledge of consumers.
- Cons: the same concrete type may satisfy several near-duplicate interfaces; "find all implementations" is harder without tooling.

**Alternatives**: *Interface next to implementation* (Java/C# convention) — tends toward fat interfaces exposing everything the implementation can do.

**Prerequisites**: Go interfaces and structural typing · Go package/import-cycle rules.

#### 7. Keyset (cursor) pagination

**What**: Instead of `OFFSET n`, remember the sort key of the last row and ask for rows strictly after it: `WHERE (occurred_at, transaction_id) < ($2, $3) ORDER BY ... DESC LIMIT n`.

**Why used here**: New transactions arrive continuously. With `OFFSET`, inserts between page requests shift rows and the client silently skips or repeats records. Keyset paging is stable under concurrent inserts, and the tuple `(occurred_at, transaction_id)` breaks timestamp ties deterministically.

**When to use**: Any list endpoint over a table that is actively written, and any deep pagination — `OFFSET 100000` makes the database count and discard 100,000 rows.

**Trade-offs**:
- Pros: stable under writes; constant-time per page *given the right index*; opaque cursor hides schema.
- Cons: no random access ("jump to page 50"); the sort key must be unique (hence the tuple); changing sort order invalidates cursors.

**Alternatives**:
- *`LIMIT`/`OFFSET`*: trivial and supports page numbers; unstable and slow at depth.
- *Snapshot/scroll cursors* (Elasticsearch-style): fully consistent view, but holds server state and expires.

**Prerequisites**: SQL `ORDER BY`/`LIMIT` · composite index ordering · SQL row-value comparison.

#### 8. Prometheus histograms and quantile estimation

**What**: A histogram records observations into pre-defined cumulative buckets, plus a total sum and count. Quantiles are *estimated* afterwards by finding the bucket containing the target rank and interpolating within it.

**Why used here**: `fraud_assessment_duration_seconds` uses `DefBuckets`; `PipelineSnapshot`/`latencyQuantile` reimplement `histogram_quantile` in Go so the dashboard can show p50/p95/p99 without a Prometheus server.

**When to use**: Any latency or size distribution. Histograms are aggregatable across instances (you can sum bucket counts); summaries are not.

**Trade-offs**:
- Pros: cheap to record; aggregatable across replicas; buckets are configurable per metric.
- Cons: accuracy is entirely a function of bucket layout — a p99 falling in a wide bucket is a wide guess; every bucket is a separate time series, so cardinality × labels adds up fast.

**Alternatives**:
- *Summaries*: accurate client-side quantiles, but cannot be aggregated across instances.
- *Native/exponential histograms* (newer Prometheus): far better resolution without manual bucket tuning.

**Prerequisites**: percentiles vs averages and why averages hide tail latency · Prometheus metric types · why `avg(p99)` across instances is meaningless.

#### 9. `errgroup` + context as process supervision

**What**: `errgroup.WithContext` runs N goroutines; the first non-nil error cancels the shared context and is returned by `Wait()`.

**Why used here**: Four long-lived components (two consumers, the outbox, the HTTP server) must live and die together. If the transaction consumer dies, continuing to serve HTTP against a stalled pipeline is worse than exiting and letting the orchestrator restart. Signal handling enters the same mechanism via `signal.NotifyContext`.

**When to use**: Any Go process running multiple long-lived workers, and any fan-out of parallel subtasks where the first failure should abort the rest.

**Trade-offs**:
- Pros: ~10 lines for fail-fast supervision; a single cancellation path; only the first error is surfaced (usually the causal one).
- Cons: subsequent errors are discarded; `Wait` does not force-kill a goroutine ignoring its context.

**Alternatives**:
- *`sync.WaitGroup` + error channel + manual `cancel()`*: what `errgroup` replaces; more code, same semantics.
- *Supervisor-tree libraries (suture)*: per-child restart policies; more machinery than a container restart already gives you.

**Prerequisites**: `context.Context` cancellation propagation · goroutines and channels · `defer` ordering.

#### 10. `go:embed`

**What**: A compiler directive that bakes files into the binary as `string`, `[]byte`, or `embed.FS`.

**Why used here**: `001_init.sql` (`postgres.go:17`) and `ui/dashboard.html` (`ui.go:9`) ship inside the binary. The container image needs no extra files and the migration cannot drift from the code that runs it — a single `go build` output is the whole deployment.

**When to use**: Migrations, templates, static assets, default configs — anything small and versioned with the code.

**Trade-offs**:
- Pros: single-artifact deploys; no runtime path resolution; asset and code versions cannot diverge.
- Cons: changing an asset requires a rebuild; binary size grows; large assets are better served from object storage.

**Prerequisites**: Go build directives (`//go:embed` must sit immediately above its var, with `_ "embed"` imported for the string/[]byte forms).

#### Already-known concepts (noted, not explained)

**REST APIs** — used conventionally in `internal/httpapi`: resource-shaped paths, `GET`-only, JSON bodies, status codes carrying meaning (400 invalid input, 404 missing, 503 dependency down).

---

## Findings & Trade-offs

Ordered by how much they would matter if this ran for real. Nothing here is currently causing a test failure — `go vet` is clean and `go test -race ./...` passes.

**1. No `OnPartitionsRevoked` handler with manual commits** — `stream/kafka.go:76-86`
The consumer sets `kgo.DisableAutoCommit()` and commits manually, but registers no revoke hook. franz-go's own guidance is explicit: with manual commits you must either commit in `OnPartitionsRevoked` or abandon in-flight work after a revoke. During a rebalance, worker goroutines can still be mid-`handleRecord` on partitions that have moved to another member, and their subsequent `CommitRecords` can rewind or clobber the new owner's offsets.
*Why it is invisible today*: a single service instance never rebalances. *Why it is survivable even then*: the `ON CONFLICT DO NOTHING` idempotency absorbs the resulting duplicate processing — data stays correct, offsets get messy. This is a good illustration of defence in depth, but it is the first thing to fix before running two replicas.

**2. `store.Evaluate` runs the scoring callback inside an open transaction** — `store/postgres.go:99`
Safe today because `Logistic.Score` is pure CPU with no I/O. If the scorer ever becomes a network call to a model server, this holds a pooled connection and row locks across a remote round-trip — the classic way to exhaust a connection pool under partial failure. The constraint is real but currently undocumented; worth a comment on the `Evaluator` type.

**3. Insert-before-query in `Evaluate` is load-bearing for NULL-safety** — `store/postgres.go:60` vs `:83`
`bool_or` over zero rows returns `NULL`, which cannot scan into a plain `bool`. The query only ever sees ≥1 row because the event's own row was inserted first. Reordering the two statements — which looks like a harmless cleanup — breaks the first transaction of every new account. Either add a comment or make it explicit with `COALESCE(bool_or(...), false)`.

**4. Feature names are unvalidated strings across a package boundary** — `model/logistic.go:52`, `features/features.go:42`
`value := features[feature]` returns `0.0` for a missing key. A typo in `configs/model.json` (`"loga_mount"`) silently zeroes that feature — no error, no log, a quietly degraded model that still scores and still passes every test. A `Load`-time check that the artifact's weight keys are a subset of a known feature set would catch it at startup.

**5. `log_amount` is the only unbounded feature** — `features/features.go:43`
Every other feature is clamped to `[0,1]`; `log_amount` is `ln(1 + amount)`, which reaches ~6.9 at £1,000 and ~9.2 at £10,000. At weight 0.55 that is a 3.8–5.1 logit contribution, dominating the other eight features combined. Whether this is intended is a modelling question, but it means the weights in `model.json` are *not* directly comparable in importance the way the clamped ones are.

**6. `ListTransactions` has no supporting index** — `store/postgres.go:223-233`, `migrations/001_init.sql`
The query orders by `t.occurred_at DESC, t.transaction_id DESC`, but the only relevant index is `(account_id, occurred_at DESC)` — wrong leading column. Every page is a full scan plus sort. Additionally `WHERE ($1 = '' OR a.recommended_action = $1)` is non-sargable: the `OR` on a parameter defeats index use even where `assessments_action_idx` would help. Fixes: add `transactions (occurred_at DESC, transaction_id DESC)`, and either branch to two queries or use two separate parameterised paths for the optional filter. (`assessments_action_idx` on a 3-value column is also low-selectivity and unlikely to be chosen regardless.)

**7. Outbox flush is head-of-line blocking with no DLQ** — `service/outbox.go:52-58`
`flush` returns on the first publish or mark error, so a single permanently-unpublishable row wedges the outbox forever, retried every 250ms. The consumer path has a permanent/transient split and a dead-letter topic; the outbox path has neither. Asymmetric — and the outbox is the one that silently stops delivering assessments downstream.

**8. `ui.go` re-reads the embedded dashboard on every request** — `httpapi/ui.go:14`
`embed.FS.ReadFile` allocates a fresh copy per call, so every dashboard hit re-allocates the whole HTML. The stated justification ("so unit tests against a zero-value Server still work") does not hold: a package-level `var dashboardBytes, _ = dashboardHTML.ReadFile(...)` is equally zero-value-safe and reads once. Also `w.Write` at `:21` is the only unchecked write in the file where siblings use `_ =`.

**9. `fraudctl stats` measures a tautology** — `configs/model.json` vs `simulator/generator.go:60-69`
The simulator draws fraud from a disjoint distribution (risky category + foreign + ecommerce + 3–20× the amount) and the weights are hand-set to separate exactly those signals. Precision and recall will look excellent because the model was authored against the generator, not fitted to data. Useful for exercising the plumbing; not evidence of model quality. Worth knowing before reading the metrics dashboard as if it meant something.

**10. Latency histogram only observes the created path** — `service/processor.go:83`
`Latency.Observe` sits after the `if !created { return }` early exit (`:76-80`) and after the error return (`:71-75`), so duplicates and failures contribute no samples. Excluding duplicates is arguably right; excluding failures means a slow-failing dependency is invisible in the latency percentiles.

**11. `started := time.Now()` bypasses the injected clock** — `service/processor.go:44` vs `:39`
The processor injects `now func() time.Time` for `AssessedAt` but measures latency with `time.Now()` directly. Minor inconsistency; it just means latency is not controllable in tests.

**12. DLQ publish failure retries forever** — `stream/kafka.go:162-164`
If the dead-letter topic is unavailable, a *permanent* error is returned up and retried with backoff — so a malformed message can still stall its partition when the DLQ is down. Correct in that it does not drop data, but it means "permanent errors never block" is not quite true.

**13. `Consumer.Run` does not wait for workers before returning** — `stream/kafka.go:110-114`
The deferred `close(ch)` lets workers drain, but `Run` returns without joining them, so `main`'s deferred `producer.Close()` / `repository.Close()` can fire while a worker is still inside `handleRecord`. In practice the `ctx.Err() != nil` guard at `:150` short-circuits immediately, so this is benign today — but a `sync.WaitGroup` would make it provably so.

**14. Ignored `json.Marshal` errors in the simulator** — `cmd/simulator/main.go:91`, `:126`
`payload, _ := json.Marshal(tx)`. Genuinely cannot fail for these struct types, so it is defensible — but it is the habit that bites later when someone adds a `map[any]any` or a channel field.

---

## Learning Resources

### Official Documentation

- [Pattern: Transactional outbox](https://microservices.io/patterns/data/transactional-outbox.html) — Chris Richardson's canonical write-up of the dual-write problem and its fix. The pattern this repo implements, stated in one page.
- [Pattern: Polling publisher](https://microservices.io/patterns/data/polling-publisher.html) — the specific outbox drain strategy `service/outbox.go` uses, with its trade-offs against the alternative below.
- [Pattern: Transaction log tailing](https://microservices.io/patterns/data/transaction-log-tailing.html) — the CDC alternative to polling; read this to understand what Debezium buys you and what it costs.
- [franz-go: producing and consuming](https://github.com/twmb/franz-go/blob/master/docs/producing-and-consuming.md) — the client's own guide, including the manual-commit rules that Finding 1 relies on.
- [franz-go: goroutine-per-partition manual commit example](https://github.com/twmb/franz-go/blob/master/examples/goroutine_per_partition_consuming/manual_commit/main.go) — a working reference for the rebalance-safe version of `stream.Consumer`.
- [`kgo` package reference](https://pkg.go.dev/github.com/twmb/franz-go/pkg/kgo) — option-by-option docs for `RequiredAcks`, `DisableAutoCommit`, `ConsumeResetOffset`.
- [`golang.org/x/sync/errgroup`](https://pkg.go.dev/golang.org/x/sync/errgroup) — short enough to read entirely; note `WithContext` vs plain `Group` and the `SetLimit` method.
- [`pgx` v5](https://pkg.go.dev/github.com/jackc/pgx/v5) and [`pgxpool`](https://pkg.go.dev/github.com/jackc/pgx/v5/pgxpool) — transaction API, `Scan` type mapping, pool configuration.
- [PostgreSQL aggregate expressions (`FILTER`)](https://www.postgresql.org/docs/current/sql-expressions.html#SYNTAX-AGGREGATES) — the `count(*) FILTER (WHERE ...)` syntax used in both the history query and the confusion matrix.
- [PostgreSQL: `INSERT ... ON CONFLICT`](https://www.postgresql.org/docs/current/sql-insert.html#SQL-ON-CONFLICT) — including the concurrency notes relevant to `DO NOTHING` against an in-flight insert.
- [Prometheus: histograms and summaries](https://prometheus.io/docs/practices/histograms/) — explains exactly what `latencyQuantile` reimplements, and why bucket choice bounds accuracy.

### Tutorials & Articles

- [Go blog: Routing enhancements for Go 1.22](https://go.dev/blog/routing-enhancements) — the `"GET /v1/transactions/{id}"` + `r.PathValue` syntax in `httpapi`, and why a third-party router is now optional.
- [Go blog: Structured logging with slog](https://go.dev/blog/slog) — the `log/slog` API used throughout, including handler choice and levels.
- [Go blog: Go concurrency patterns — pipelines and cancellation](https://go.dev/blog/pipelines) — the foundation for `Consumer.Run`'s fan-out and the simulator's channel discipline.
- [Go blog: Go concurrency patterns — context](https://go.dev/blog/context) — cancellation propagation, which is the backbone of shutdown here.
- [Go blog: Working with errors in Go 1.13](https://go.dev/blog/go1.13-errors) — `%w`, `errors.Is`/`As`; directly explains the `permanentError` design.
- [Use the index, Luke: "No offset"](https://use-the-index-luke.com/no-offset) — the definitive argument for keyset over `OFFSET` pagination, with the index requirements Finding 6 is about.
- [Go wiki: Code Review Comments](https://go.dev/wiki/CodeReviewComments) — the community style baseline this codebase already follows closely.

### Videos

- [GopherCon — "Rethinking Classical Concurrency Patterns" (Bryan Mills, ~50 min)](https://www.youtube.com/watch?v=5zXAHh5tJqQ) — when *not* to reach for worker pools and `sync` primitives; useful counterweight to Finding 13.
- [Kafka Summit 2018 — "Don't Repeat Yourself: Introducing Exactly-Once Semantics in Apache Kafka" (Matthias J. Sax, ~40 min)](https://www.youtube.com/watch?v=zm5A7z95pdE) — what Kafka's EOS does and does not cover, and why it does not remove the need for the outbox.

### Related Concepts (for deeper study)

- **Change Data Capture (CDC)** — the log-tailing alternative to the polling outbox.
- **Saga pattern** — what you reach for when a workflow spans several services' databases.
- **Feature stores and point-in-time joins** — the productionised form of Concept 3.
- **Model drift and delayed-label evaluation** — the `fraud_labels` topic exists to enable this; the natural next thing to build on it.
- **Backpressure and bounded queues** — the 32-deep worker channels are the implicit backpressure mechanism here.
- **Serializable vs Read Committed isolation** — `Evaluate` uses the default `pgx.TxOptions{}` (Read Committed); worth reasoning about what changes under concurrent same-account inserts.

---

## Related Code in This Project

| File | Relationship |
|---|---|
| `internal/domain/domain.go` | Imported by every other package; the dependency sink |
| `internal/features/features.go` | Called only from the `Evaluator` closure in `service/processor.go:58` |
| `internal/model/logistic.go` | Satisfies `service.Scorer`; loaded from `configs/model.json` by `cmd/fraud-service/main.go:56` |
| `internal/policy/policy.go` | Called at `service/processor.go:67`; configured by the `-review/-escalate-threshold` flags |
| `internal/store/postgres.go` | Satisfies `service.Repository`, `service.OutboxRepository`, and `httpapi.Repository` — one type, three consumer-side interfaces |
| `internal/store/migrations/001_init.sql` | Embedded at `postgres.go:17`, applied at `cmd/fraud-service/main.go:53` |
| `internal/stream/kafka.go` | `Producer` satisfies `service.Publisher`; `Permanent` is called from `service/processor.go` |
| `internal/service/metrics.go` | Written by `Processor` and `Outbox`; read back by `pipeline_metrics.go` and served at `/metrics` |
| `internal/service/pipeline_metrics.go` | Wired into `httpapi.New` as a closure at `cmd/fraud-service/main.go:87` |
| `internal/httpapi/ui/dashboard.html` | Embedded by `ui.go`; consumes `/v1/pipeline-metrics` and `/v1/model-metrics` |
| `cmd/simulator/main.go` | Produces to `stream.TransactionsTopic` and `stream.LabelsTopic` — the only writer besides the outbox |
| `cmd/fraudctl/main.go` | Pure HTTP client of `internal/httpapi`; shares no Go types with it (talks JSON) |
| `compose.yaml` / `Dockerfile` | Postgres + Redpanda + topic creation; `--profile app` runs the service itself |
| `AGENT.md` / `README.md` | Both already describe the invariants above accurately — unusually well-maintained |

---

## Next Steps

### 1. Try it yourself

- **Prove the idempotency claim.** Run `make infra-up && make service`, then `make simulate` with a fixed seed. Note `fraudctl stats`. Now re-run the *identical* simulator command. Transaction counts should not change and `fraud_duplicate_events_total` should climb by exactly the replayed count. Watch it in `curl localhost:8080/metrics | grep duplicate`.
- **Prove point-in-time correctness.** Run the simulator with `--seed 42 --count 200`, record a few `fraudctl show tx-42-...` risk scores. Wipe with `docker compose down -v`, restart, and replay with `--rate 0` (unpaced, so arrival order differs from the first run). The scores should be identical — that is Concept 3 in action.
- **Break the model quietly.** Rename one weight key in `configs/model.json` to a typo and restart. Everything starts, nothing errors, scores shift. That is Finding 4 — then fix it by adding validation to `model.Load`.
- **Watch the retry ladder.** With the service running, `docker compose stop postgres` for 30 seconds. Logs should show `event processing failed; retrying` with the backoff doubling to the 5s cap; on restart, nothing is lost. Then check that offsets did not advance during the outage.

### 2. Deeper dive

- **Make the consumer rebalance-safe** (Finding 1): add `kgo.OnPartitionsRevoked` that stops dispatch, drains in-flight work, and performs a blocking commit. Then actually run two `fraud-service` instances against >1 partition and confirm no offset rewind.
- **Add the missing index** (Finding 6) and measure. `EXPLAIN (ANALYZE, BUFFERS)` on the `ListTransactions` query before and after `CREATE INDEX ON transactions (occurred_at DESC, transaction_id DESC)` — with ~100k rows the difference is dramatic and worth seeing.
- **Give the outbox the same error taxonomy as the consumer** (Finding 7): permanent/transient split, a `failed_at`/`attempts` column, and a poison-row escape hatch.
- **Close the label loop.** Labels are stored and counted but never used. Build a small offline trainer that fits weights from `transactions ⋈ fraud_labels`, emits a `model.json` with a new `version`, and compare `fraudctl stats --model-version` across versions. That turns Finding 9's tautology into a real evaluation.
- **Test `store` and `stream`.** Both are currently untested (`[no test files]`). `store` is a strong fit for [testcontainers-go](https://golang.testcontainers.org/) — and the tests worth writing first are exactly the invariants above: duplicate insert returns the original assessment, first-ever transaction for an account does not error, history excludes the current row.

### 3. Common pitfalls

- **Do not "clean up" the insert-before-query order in `Evaluate`.** It is load-bearing (Finding 3).
- **Do not put I/O in the `Evaluator` callback.** It runs inside an open transaction (Finding 2).
- **Do not change `occurred_at < $2` to `<= $2` or to `now()`.** Either edit reintroduces leakage or destroys replay determinism.
- **Do not add a foreign key from `fraud_labels` to `transactions`.** The migration comment explains why: labels may legitimately arrive first.
- **Do not switch the outbox to mark-then-publish.** At-most-once silently drops assessments; the current order is the correct trade.
- **Do not trust the model metrics as model quality.** They measure the simulator agreeing with itself (Finding 9).
- **Do not use the global Prometheus registry** in new code — `NewMetrics(registerer)` takes an explicit one so tests do not collide.

---

*This deep dive was generated by AntiVibe — the anti-vibecoding learning framework.*
*Learn what AI writes, not just accept it.*
