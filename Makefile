.PHONY: test build infra-up infra-down service simulate stats docs-serve docs-build

test:
	go test -race ./...

build:
	go build ./...

infra-up:
	docker compose up -d postgres redpanda topics

infra-down:
	docker compose down

service:
	go run ./cmd/fraud-service

simulate:
	go run ./cmd/simulator --count 1000 --rate 100 --seed 42

stats:
	go run ./cmd/fraudctl stats

docs-serve:
	python3 -m pip install -q -r requirements.txt
	mkdocs serve

docs-build:
	python3 -m pip install -q -r requirements.txt
	mkdocs build --strict
