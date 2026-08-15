# Scoring and Explainability

## The model is intentionally not machine-learned

`configs/model.json` is a hand-authored logistic regression artifact — an intercept and nine feature weights, chosen to make the simulator's synthetic fraud patterns score highly. Nothing in this repo trains a model; `internal/model.Logistic` only loads and scores an artifact. The point of the project is the backend engineering around inference, not the modeling itself.

## Features

`internal/features.Extract` (`internal/features/features.go`) turns a transaction plus its account's `domain.History` into a `FeatureVector` — nine numeric signals:

| Feature | Derivation |
| --- | --- |
| `log_amount` | `log1p(amount_minor / 100)` — dampens the effect of very large amounts |
| `foreign` | 1 if the merchant's country isn't `GB` |
| `card_not_present` | 1 if entry mode is `ecommerce` |
| `risky_merchant` | 1 if merchant category is `crypto`, `gambling`, or `money_transfer` |
| `nighttime` | 1 if the transaction's UTC hour is before 6 or at/after 23 |
| `tx_count_10m` | account's transaction count in the last 10 minutes, capped at 10 and normalized to `[0,1]` |
| `spend_1h` | account's spend in the last hour (major units), capped at 2000 and normalized to `[0,1]` |
| `new_merchant` | 1 if this is the account's first transaction with this merchant |
| `new_country` | 1 if this is the account's first transaction in this merchant's country |

`tx_count_10m` and `spend_1h` come from `domain.History`, which `Postgres.Evaluate` computes from prior rows in the same database transaction — they reflect durable state, not in-memory state that would be lost across restarts or inconsistent across service instances.

## Scoring and per-feature contributions

`internal/model.Logistic.Score` computes `intercept + Σ(weight × feature_value)`, passes that through a sigmoid to get a probability in `[0,1]`, and — critically — also returns a `[]domain.Signal` per feature: `{feature, value, contribution}`, sorted by `|contribution|` descending. Every score is traceable back to exactly which inputs drove it and by how much, which is what the HTTP API and `fraudctl show` expose. This is why the project calls the model "transparent" rather than "explainable" as an afterthought — explainability is a first-class return value of `Score`, not a separate post-hoc step.

## Policy: score to action

`internal/policy.Thresholds.Decide` maps the score to `domain.ActionEscalate` (score ≥ escalate threshold), `domain.ActionReview` (score ≥ review threshold), or `domain.ActionNone`. Thresholds are validated at startup (`0 <= review < escalate <= 1`) and are independent of the model artifact — see [Run the Suggested Experiments](../how-to/run-suggested-experiments.md#3-change-policy-thresholds-without-touching-the-model) for changing them without retraining or reloading the model.

Because the pipeline is asynchronous — a transaction is scored well after it's authorized, not in the payment path — these are **recommendations** for downstream review, not real-time authorization decisions.

## Why labels can't leak into scoring

`fraud.labels.v1` is consumed by a separate consumer group (`fraud-label-evaluator-v1`) from the one that scores transactions (`fraud-scorer-v1`), and `service.Processor.HandleTransaction` has no code path that reads a label. The simulator also publishes labels on a delay (`--label-delay`, default 5s) specifically to model the real-world gap between "a transaction happens" and "we find out whether it was fraud" — useful for discussing class imbalance, label delay, and monitoring model quality as labels trickle in (`GET /v1/model-metrics`) rather than assuming ground truth is available immediately.
