.PHONY: up down migrate-up migrate-down run-api run-worker run-scheduler test build

# ── Docker ───────────────────────────────────────────────────
up:
	docker-compose up -d

down:
	docker-compose down

# ── Migrations ───────────────────────────────────────────────
# Requires: go install -tags 'postgres' github.com/golang-migrate/migrate/v4/cmd/migrate@latest
migrate-up:
	migrate -path migrations -database "$(DATABASE_URL)" up

migrate-down:
	migrate -path migrations -database "$(DATABASE_URL)" down

# ── Run services ─────────────────────────────────────────────
run-api:
	go run ./cmd/api/...

run-worker:
	go run ./cmd/worker/...

run-scheduler:
	go run ./cmd/scheduler/...

# ── Tests ─────────────────────────────────────────────────────
test:
	go test ./... -v -race

# ── Build ─────────────────────────────────────────────────────
build:
	go build -o bin/api       ./cmd/api/...
	go build -o bin/worker    ./cmd/worker/...
	go build -o bin/scheduler ./cmd/scheduler/...
