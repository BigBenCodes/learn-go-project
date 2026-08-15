# Query Transactions with fraudctl

`fraudctl` is a read-only CLI that wraps the HTTP API — every subcommand is a thin `GET` request that pretty-prints the JSON response. It needs the service running and reachable (default `http://localhost:8080`, override with `--url`).

## List recent transactions

```sh
go run ./cmd/fraudctl list --limit 5
```

`--limit` defaults to 20 (the API caps it at 100).

## Filter by recommended action

```sh
go run ./cmd/fraudctl list --action escalate
```

`--action` accepts `no_action`, `review`, or `escalate`.

## Page through results

```sh
go run ./cmd/fraudctl list --limit 20 --cursor "<next_cursor from the previous response>"
```

Each `list` response includes a `next_cursor` field; pass it back as `--cursor` to fetch the next page. The cursor is an opaque, base64-encoded `(occurred_at, transaction_id)` pair — treat it as a token, not something to construct by hand.

## Look up a single transaction

```sh
go run ./cmd/fraudctl show tx-42-000000001
```

Returns the transaction, its assessment (score, action, feature signals), and its label if one has arrived yet.

## Check model metrics

```sh
go run ./cmd/fraudctl stats
```

```sh
go run ./cmd/fraudctl stats --model-version dummy-logistic-v1
```

Without `--model-version`, metrics aggregate across all model versions seen. With it, metrics are scoped to that one version — useful once you've [added a second model artifact](run-suggested-experiments.md) and want to compare them.

## Point at a different service instance

```sh
go run ./cmd/fraudctl --url http://localhost:8080 list
```

`--url` is a global flag and must come before the subcommand.

See [Commands Reference](../reference/commands.md) for the full flag table.
