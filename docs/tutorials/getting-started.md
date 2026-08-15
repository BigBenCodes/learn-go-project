# Getting Started

This tutorial takes you from a clean checkout to seeing scored transactions and model metrics. Follow the steps in order — each one depends on the previous.

## What you'll need

- Go 1.26+
- Docker and Docker Compose

## 1. Start the infrastructure

```sh
make infra-up
```

This runs `docker compose up -d postgres redpanda topics`: a Postgres container, a Redpanda (Kafka-compatible) broker, and a one-shot `topics` container that creates `transactions.v1`, `fraud.assessments.v1`, `fraud.labels.v1`, and `fraud.dead-letter.v1` with 6 partitions each.

Wait a few seconds for the healthchecks to pass before continuing.

## 2. Start the fraud service

In your first terminal:

```sh
make service
```

This runs `go run ./cmd/fraud-service`. On startup it connects to Postgres, applies the embedded schema migration, loads `configs/model.json`, connects to Kafka, and starts serving HTTP on `:8080`. You should see a `starting fraud service` log line.

Leave this running.

## 3. Generate transactions

In a second terminal:

```sh
make simulate
```

This runs `go run ./cmd/simulator --count 1000 --rate 100 --seed 42`: it publishes 1,000 mock transactions (1% fraudulent) to `transactions.v1` at 100/second, then publishes each transaction's ground-truth fraud label to `fraud.labels.v1` five seconds later. Watch the first terminal — you'll see `transaction assessed` log lines as the service scores each event.

Wait for the simulator to print `published 1000 transactions and labels` before continuing.

## 4. Inspect the results

```sh
go run ./cmd/fraudctl list --limit 5
```

You should see JSON output listing the five most recent transactions, each with its risk score, recommended action, and feature signals.

Try filtering to just the flagged ones:

```sh
go run ./cmd/fraudctl list --action escalate
```

Look up a single transaction by ID (IDs follow the pattern `tx-<account>-<sequence>`):

```sh
go run ./cmd/fraudctl show tx-42-000000001
```

## 5. Check model metrics

Wait a few seconds after step 3 finishes so the delayed labels have arrived, then run:

```sh
go run ./cmd/fraudctl stats
```

This calls `GET /v1/model-metrics` and prints a confusion matrix (true/false positives and negatives), precision, recall, false-positive rate, and label coverage — computed from whatever labels have arrived so far.

## 6. Check the metrics endpoint

```sh
curl localhost:8080/metrics
```

This returns Prometheus-format metrics for the service itself (events processed, duplicates, failures, recommended actions, processing latency, outbox throughput).

## You're done

You've run the full pipeline: simulated transactions → Kafka → scored and persisted in Postgres → republished via the outbox → queried through the HTTP API and CLI.

Next steps:

- [Reproduce a Simulator Run](../how-to/reproduce-simulator-runs.md) to generate the exact same data again, or a different scenario.
- [Architecture Overview](../explanation/architecture-overview.md) to understand how the pieces fit together.
- [Run Tests and Build Binaries](../how-to/run-tests-and-build.md) before making any changes.
