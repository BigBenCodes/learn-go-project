# Reproduce a Simulator Run

The simulator (`cmd/simulator`) is fully deterministic: given the same flags, it produces the exact same transactions, labels, and timing every run.

## Reproduce the default run exactly

```sh
go run ./cmd/simulator --count 1000 --rate 100 --seed 42
```

Every value that affects output is an explicit flag — `--seed`, `--start-time`, `--rate`, `--accounts`, and `--fraud-rate` — so re-running with the same values reproduces the same event stream byte-for-byte.

## Pin the start time too

By default the simulator uses the current time (truncated to the second) as its first event's timestamp, which makes `occurred_at` values differ between runs even with the same seed. To make timestamps reproducible as well, pass `--start-time`:

```sh
go run ./cmd/simulator --count 1000 --rate 100 --seed 42 --start-time 2026-01-01T00:00:00Z
```

`--start-time` must be RFC3339.

## Change the scenario

- **More accounts:** `--accounts 500` spreads events across more distinct accounts, thinning out the "recent history" features (`tx_count_10m`, `spend_1h`).
- **Higher fraud rate:** `--fraud-rate 0.05` makes 5% of generated transactions fraudulent, useful for stress-testing the model/policy thresholds with more escalations.
- **Different label delay:** `--label-delay 30s` widens the gap between a transaction being scored and its ground-truth label arriving.

## Run a continuous stream

```sh
go run ./cmd/simulator --count 0 --rate 20
```

`--count 0` runs until interrupted (Ctrl-C), useful for soak-testing the service or watching metrics accumulate over a long period. `--rate 0` disables pacing entirely and emits as fast as possible.

## Point at different brokers

```sh
go run ./cmd/simulator --brokers redpanda:9092
```

Default is `localhost:19092` (Redpanda's external listener as configured in `compose.yaml`). Use the internal address if running the simulator itself inside the Compose network.

See [Commands Reference](../reference/commands.md) for the full simulator flag table.
