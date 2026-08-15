# Run the Service in Docker Compose

By default `make infra-up` starts only Postgres, Redpanda, and the topic-bootstrap job — you run `fraud-service` yourself with `make service` (`go run ./cmd/fraud-service`) so you can iterate on it locally. The `fraud-service` container is defined behind an opt-in Compose profile, `app`, for cases where you want the service itself running in a container instead.

## Start everything, including the service, in containers

```sh
docker compose --profile app up --build
```

This builds the `fraud-service` image from the repo's `Dockerfile` and starts it alongside Postgres, Redpanda, and the topic-bootstrap job. `--build` ensures you pick up local code changes rather than reusing a stale image.

The containerized service is wired to talk to the other containers over the Compose network, not to `localhost`:

| Variable        | Value in Compose                                             | Purpose                                |
| --------------- | ------------------------------------------------------------ | -------------------------------------- |
| `DATABASE_URL`  | `postgres://fraud:fraud@postgres:5432/fraud?sslmode=disable` | Postgres inside the network            |
| `KAFKA_BROKERS` | `redpanda:9092`                                              | Redpanda's internal listener           |
| `HTTP_ADDRESS`  | `:8080`                                                      | published to host port 8080            |
| `MODEL_PATH`    | `/app/configs/model.json`                                    | baked into the image by the Dockerfile |

## Why this differs from `make service`

`make service` runs `go run ./cmd/fraud-service` on your host, which uses the flag defaults pointing at `localhost:19092` (Redpanda's external listener) and `localhost:5432`. The two modes are mutually exclusive ways of running the same binary — pick whichever matches how you're currently working: fast local iteration with `make service`, or a fully containerized stack with `docker compose --profile app up --build`.

## Stop the containerized service

```sh
docker compose --profile app down
```

Add `-v` to also remove the Postgres/Redpanda data volumes — see [Reset Local Infrastructure](reset-local-infrastructure.md).
