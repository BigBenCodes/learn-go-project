# Reset Local Infrastructure

Two different commands stop the local Postgres/Redpanda stack, with different consequences for your data.

## Stop, keep data

```sh
make infra-down
```

Runs `docker compose down`, which stops and removes the containers but leaves the named volumes (`postgres-data`, `redpanda-data`) intact. The next `make infra-up` picks up exactly where you left off — same transactions, same Kafka offsets, same consumer group positions.

Use this between work sessions when you want continuity.

## Stop, wipe everything

```sh
docker compose down -v
```

The `-v` flag additionally deletes the named volumes. Postgres comes back empty (the schema migration reapplies on next service startup) and Redpanda comes back with no topics or messages — `make infra-up` recreates the topics from scratch via the `topics` bootstrap job.

Use this when you want a genuinely clean slate: a fresh simulator run without old transactions mixed in, or to test that migrations and topic creation work from nothing.

!!! warning
    `-v` is destructive and cannot be undone. There's no confirmation prompt.
