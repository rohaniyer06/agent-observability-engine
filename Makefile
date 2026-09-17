SHELL := /bin/bash

# Import the package from src/ explicitly rather than relying on the
# editable-install path hook: setuptools' .pth is not honoured reliably by
# every venv/Python build, and a stack that only starts on some machines is
# not a stack.
export PYTHONPATH := src
PY := python3
VENV := .venv
BIN := $(VENV)/bin

.DEFAULT_GOAL := help

.PHONY: help
help: ## Show this help
	@grep -hE '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

# ---- setup ------------------------------------------------------------------

$(VENV): pyproject.toml
	$(PY) -m venv $(VENV)
	$(BIN)/pip install -q --upgrade pip
	$(BIN)/pip install -q -e ".[dev]"
	@touch $(VENV)

.PHONY: install
install: $(VENV) ## Create venv and install the package (editable) + dev deps

.PHONY: up
up: ## Start Postgres + Redis
	docker compose up -d
	@echo "waiting for services..."
	@until docker compose exec -T postgres pg_isready -U aoe -d aoe >/dev/null 2>&1; do sleep 0.5; done
	@until docker compose exec -T redis redis-cli ping >/dev/null 2>&1; do sleep 0.5; done
	@echo "postgres + redis ready"

.PHONY: down
down: ## Stop Postgres + Redis
	docker compose down

.PHONY: nuke
nuke: ## Stop and delete all data volumes
	docker compose down -v

.PHONY: migrate
migrate: install ## Apply SQL migrations
	$(BIN)/aoe-migrate

# ---- run --------------------------------------------------------------------

.PHONY: ingest
ingest: install ## Run the ingestion service (:8000)
	$(BIN)/uvicorn aoe.ingest.app:app --host 0.0.0.0 --port 8000

.PHONY: api
api: install ## Run the query API + live WS (:8001)
	$(BIN)/uvicorn aoe.api.app:app --host 0.0.0.0 --port 8001

.PHONY: worker
worker: install ## Run one worker process
	$(BIN)/aoe-worker

.PHONY: harness
harness: install ## Run the 4-node LangGraph harness agent
	$(BIN)/aoe-harness --runs 20

.PHONY: dashboard
dashboard: ## Run the React dashboard dev server (:5173)
	cd dashboard && npm install && npm run dev

.PHONY: stack
stack: ## Run ingest + api + worker together (Ctrl-C stops all)
	@trap 'kill 0' EXIT INT TERM; \
	$(BIN)/uvicorn aoe.ingest.app:app --host 0.0.0.0 --port 8000 & \
	$(BIN)/uvicorn aoe.api.app:app --host 0.0.0.0 --port 8001 & \
	$(BIN)/aoe-worker & \
	wait

# ---- verify -----------------------------------------------------------------

.PHONY: test
test: install ## Run unit tests (no infra required)
	$(BIN)/pytest -q -m "not integration"

.PHONY: test-all
test-all: install ## Run all tests including integration (requires `make up`)
	$(BIN)/pytest -q

.PHONY: lint
lint: install ## Ruff check + format check
	$(BIN)/ruff check src loadgen tests
	$(BIN)/ruff format --check src loadgen tests

.PHONY: fmt
fmt: install ## Ruff autoformat
	$(BIN)/ruff format src loadgen tests
	$(BIN)/ruff check --fix src loadgen tests

.PHONY: smoke
smoke: install ## End-to-end check: ingest -> stream -> worker -> postgres
	$(BIN)/python -m tests.smoke

# ---- load test --------------------------------------------------------------

.PHONY: loadtest
loadtest: install ## Synthetic load run (override: RPS=2000 DURATION=60)
	$(BIN)/aoe-loadgen --rps $${RPS:-1000} --duration $${DURATION:-30} --label "$${LABEL:-default}"
