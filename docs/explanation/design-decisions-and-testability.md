# Design Decisions and Testability

## Interfaces separate orchestration from infrastructure

`service.Processor` depends on two interfaces it defines itself, not on concrete types:

```go
type Scorer interface {
    Version() string
    Score(domain.FeatureVector) (float64, []domain.Signal)
}

type Repository interface {
    Evaluate(context.Context, domain.TransactionCreated, store.Evaluator) (domain.Assessment, bool, error)
    StoreLabel(context.Context, domain.FraudLabel) (bool, error)
}
```

`service.Outbox` similarly depends on `OutboxRepository` and `Publisher` interfaces rather than on `store.Postgres` and `stream.Producer` directly. In each case there's exactly one production implementation (`model.Logistic` satisfies `Scorer`; `store.Postgres` satisfies `Repository`/`OutboxRepository`; `stream.Producer` satisfies `Publisher`) — the interfaces exist so unit tests can substitute fakes and exercise `Processor`/`Outbox` logic without standing up real Postgres or Kafka. Every internal package except `domain` has a test file built on exactly this pattern.

This is also the seam [Run the Suggested Experiments](../how-to/run-suggested-experiments.md#5-replace-the-go-scorer-with-a-remote-service) uses to swap the in-process logistic scorer for a remote inference call: implement `Scorer`, wire it in where `cmd/fraud-service/main.go` calls `model.Load`, and nothing downstream changes.

## Why `domain` has no dependencies

`internal/domain` defines the event and record types (`TransactionCreated`, `FraudLabel`, `Assessment`, `TransactionRecord`, `ModelMetrics`, etc.) plus their `Validate()` methods, and depends on no other internal package. Every other package imports `domain` for its shared vocabulary, which keeps the dependency graph acyclic and means the wire format (what a Kafka message or HTTP response actually looks like) is defined in exactly one place.

## Why the schema migration is embedded, not a separate tool

`internal/store/migrations/001_init.sql` is embedded into the binary via `go:embed` and applied automatically on startup (`Postgres.Migrate`). For a project this size, a separate migration tool/step would be more moving parts without a corresponding benefit — there's currently exactly one migration, and "start the service" is already the deployment action that needs the schema to exist.

## What this project is not

From the README's closing note, kept here as the canonical scope statement: this is a learning system. It contains synthetic data, hand-authored (not trained) model weights, a single local Kafka broker, no authentication, and no real payment intervention — the recommended actions are outputs for a human or downstream system to review, never authorization responses. Treat every architectural choice in these docs as "what a small production-shaped Go service looks like," not as production-hardened advice for a real fraud system (real fraud systems need auth, encryption at rest, PCI scope considerations, a trained and monitored model, and considerably more operational rigor than this repo demonstrates).
