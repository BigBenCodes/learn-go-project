# Run Tests and Build Binaries

## Run the full test suite

```sh
make test
```

Runs `go test -race ./...` — every internal package except `domain` has a companion `_test.go` file, and the race detector is always on.

## Run a single test

```sh
go test -race ./internal/policy/ -run TestName
```

Swap the package path and `-run` pattern for whichever test you're isolating.

## Build every binary

```sh
make build
```

Runs `go build ./...`, compiling `cmd/fraud-service`, `cmd/simulator`, and `cmd/fraudctl` (and the root `main.go`).

## Vet the code

```sh
go vet ./...
```

## Before opening a change

There's no CI workflow for Go code in this repo yet (only the docs site has one — see [Architecture Overview](../explanation/architecture-overview.md) for what's automated). Run `make test`, `make build`, and `go vet ./...` locally before pushing.
