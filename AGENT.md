# AGENT.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

Responses: concise, to the point. Favor bullets, diagrams, tables over verbose prose where appropriate.

## What this is

A small, production-shaped Go learning project: a mock card-transaction fraud pipeline. It emphasizes backend engineering (concurrency, Kafka partitioning, idempotency, DB transactions, the outbox pattern, cancellation, retries, HTTP APIs, structured logs, metrics) over ML — the model is a hand-authored logistic regression scored transparently, not trained.

## Commands

```sh
make infra-up     # start Postgres + Redpanda (Kafka) + create topics via docker compose
make service       # go run ./cmd/fraud-service
make simulate       # go run ./cmd/simulator --count 1000 --rate 100 --seed 42
make infra-down     # stop infra, keep volumes (docker compose down -v for a clean wipe)
make test         # go test -race ./...
make build         # go build ./...
go vet ./...
```

Run a single test: `go test -race ./internal/policy/ -run TestName`

Other useful commands:

```sh
go run ./cmd/fraudctl list --limit 5
go run ./cmd/fraudctl list --action escalate
go run ./cmd/fraudctl show tx-42-000000001
go run ./cmd/fraudctl stats
curl localhost:8080/metrics
docker compose --profile app up --build   # run fraud-service itself in Compose
```

The simulator's seed, start time, rate, account count, and fraud rate are all repeatable flags — reuse them to reproduce an event stream exactly. `--count 0` runs a continuous stream.

Requires Go 1.26+, Docker, and Docker Compose.

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

Three binaries in `cmd/`: `fraud-service` (the consumer/API daemon), `simulator` (mock event generator), `fraudctl` (read-only CLI against the HTTP API). `internal/` holds everything else, one package per concern — `domain` (event/record types + validation), `features`, `model` (logistic scorer), `policy` (score → action thresholds), `service` (processor + outbox, the orchestration layer), `store` (Postgres), `stream` (Kafka), `httpapi`.

**Data flow and invariants, in `internal/service/processor.go` + `internal/store/postgres.go`:**

- Transactions are keyed by account ID; Kafka partitions are processed concurrently by `stream.Consumer` (worker pool keyed on `partition % workers`), but records within one partition are handled and committed in order — ordering per account is preserved as long as the simulator/producer partitions consistently by account.
- Each transaction is handled inside one Postgres transaction (`Postgres.Evaluate`): insert the raw event with `ON CONFLICT DO NOTHING` for idempotency, compute durable history features from prior rows in the same tx, invoke the scoring callback, insert the assessment, and insert an outbox row — all committed atomically. Kafka offsets are only committed by `stream.Consumer` after the handler returns successfully, so a redelivered event is a no-op (detected via the `ON CONFLICT` producing zero rows affected) rather than a duplicate assessment.
- The outbox (`internal/service/outbox.go`) is a separate poll loop (250ms tick) that publishes unpublished rows to `fraud.assessments.v1` and marks them published — decoupling DB commit from Kafka publish so neither can be lost independently.
- `stream.Consumer.handleRecord` distinguishes permanent vs transient failures: wrap an error in `stream.Permanent(...)` (used for decode/validation failures) to route the record to `fraud.dead-letter.v1` and commit past it; anything else retries with exponential backoff (100ms → 5s cap) without committing, so a downed Postgres/Kafka just stalls and retries rather than dropping data.
- Labels arrive on a separate topic/consumer group (`fraud-label-evaluator-v1`) than the model can ever read, by design — this keeps scoring from leaking ground truth and lets label delay be simulated independently.

**Scoring:** `internal/features.Extract` turns a transaction + `domain.History` into a `FeatureVector` (log amount, foreign/card-not-present/risky-merchant/nighttime flags, recent tx count/spend, new-merchant/new-country flags). `internal/model.Logistic` loads `configs/model.json` (versioned intercept + weights) and returns a probability plus per-feature `Signal` contributions sorted by magnitude — every score is explainable back to its inputs. `internal/policy.Thresholds` then maps the score to `no_action` / `review` / `escalate`; these are recommendations, not authorization decisions, since the pipeline is asynchronous.

**Interfaces for testability:** `service.Processor` depends on `Scorer` and `Repository` interfaces (not concrete `model`/`store` types), and `service.Outbox` depends on `OutboxRepository`/`Publisher` — swap in fakes for unit tests rather than standing up Postgres/Kafka. `store.Postgres` and `stream.Producer`/`Consumer` are the only concrete implementations.

**HTTP API** (`internal/httpapi`): `/healthz`, `/readyz` (Postgres ping), `/v1/transactions` (cursor-paginated by `(occurred_at, transaction_id)`, filterable by `action`), `/v1/transactions/{id}`, `/v1/model-metrics?model_version=...` (confusion matrix computed in SQL in `Postgres.ModelMetrics`), `/metrics` (Prometheus).

Schema migration lives at `internal/store/migrations/001_init.sql`, embedded via `go:embed` and applied on service startup (`Postgres.Migrate`) — there's no separate migration tool/step.

## Behavioral Guidelines

Reduce common LLM coding mistakes. Merge with project-specific instructions above as needed.

**Tradeoff:** These guidelines bias toward caution over speed. For trivial tasks, use judgment.

## 1. Think Before Coding

**Don't assume. Don't hide confusion. Surface tradeoffs.**

Before implementing:

- State your assumptions explicitly. If uncertain, ask.
- If multiple interpretations exist, present them - don't pick silently.
- If a simpler approach exists, say so. Push back when warranted.
- If something is unclear, stop. Name what's confusing. Ask.

## 2. Simplicity First

**Minimum code that solves the problem. Nothing speculative.**

- No features beyond what was asked.
- No abstractions for single-use code.
- No "flexibility" or "configurability" that wasn't requested.
- No error handling for impossible scenarios.
- If you write 200 lines and it could be 50, rewrite it.

Ask yourself: "Would a senior engineer say this is overcomplicated?" If yes, simplify.

## 3. Surgical Changes

**Touch only what you must. Clean up only your own mess.**

When editing existing code:

- Don't "improve" adjacent code, comments, or formatting.
- Don't refactor things that aren't broken.
- Match existing style, even if you'd do it differently.
- If you notice unrelated dead code, mention it - don't delete it.

When your changes create orphans:

- Remove imports/variables/functions that YOUR changes made unused.
- Don't remove pre-existing dead code unless asked.

The test: Every changed line should trace directly to the user's request.

## 4. Goal-Driven Execution

**Define success criteria. Loop until verified.**

Transform tasks into verifiable goals:

- "Add validation" → "Write tests for invalid inputs, then make them pass"
- "Fix the bug" → "Write a test that reproduces it, then make it pass"
- "Refactor X" → "Ensure tests pass before and after"

For multi-step tasks, state a brief plan:

```
1. [Step] → verify: [check]
2. [Step] → verify: [check]
3. [Step] → verify: [check]
```

Strong success criteria let you loop independently. Weak criteria ("make it work") require constant clarification.

---

**These guidelines are working if:** fewer unnecessary changes in diffs, fewer rewrites due to overcomplication, and clarifying questions come before implementation rather than after mistakes.
