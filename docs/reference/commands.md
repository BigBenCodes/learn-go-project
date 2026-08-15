# Commands

## Make targets

| Target | Runs | Purpose |
| --- | --- | --- |
| `make infra-up` | `docker compose up -d postgres redpanda topics` | Start Postgres, Redpanda, and create Kafka topics |
| `make infra-down` | `docker compose down` | Stop containers, keep volumes |
| `make service` | `go run ./cmd/fraud-service` | Run the fraud service locally |
| `make simulate` | `go run ./cmd/simulator --count 1000 --rate 100 --seed 42` | Generate 1,000 mock transactions |
| `make stats` | `go run ./cmd/fraudctl stats` | Print model metrics |
| `make test` | `go test -race ./...` | Run the test suite |
| `make build` | `go build ./...` | Build all binaries |
| `make docs-serve` | `mkdocs serve` | Live-reload local docs preview at `127.0.0.1:8000` |
| `make docs-build` | `mkdocs build --strict` | Build the docs site, failing on broken links/nav |

## `fraud-service` flags

See [Configuration and Topics](configuration.md) for the full flag/environment-variable table.

## `simulator` flags

| Flag | Default | Env fallback | Purpose |
| --- | --- | --- | --- |
| `--brokers` | `localhost:19092` | — | Comma-separated Kafka brokers |
| `--count` | `1000` | — | Transactions to emit; `0` runs until interrupted |
| `--rate` | `100` | — | Transactions per second; `0` emits without pacing |
| `--seed` | `42` | — | Deterministic random seed |
| `--accounts` | `200` | — | Number of simulated accounts |
| `--fraud-rate` | `0.01` | — | Fraction of transactions that are fraudulent |
| `--label-delay` | `5s` | — | Delay before publishing ground-truth labels (Go duration syntax) |
| `--start-time` | now, truncated to the second | — | Optional RFC3339 event start time |

## `fraudctl` commands

Global flag: `--url` (default `http://localhost:8080`) — must precede the subcommand.

| Command | Flags | Purpose |
| --- | --- | --- |
| `list` | `--limit` (default 20), `--action`, `--cursor` | List transactions |
| `show <transaction-id>` | — | Show one transaction |
| `stats` | `--model-version` | Print model metrics |
