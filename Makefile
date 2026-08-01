# Thin wrapper over ci/gate.py and the commands in AGENTS.md.
#
# The gate is ci/gate.py, not these targets. This file exists so the same command
# works from a shell, from CI, and from an agent without anyone having to remember
# the venv path -- it must never grow logic of its own.

VENV    := .venv
PY      := $(VENV)/bin/python
PYTEST  := $(PY) -m pytest
RUFF    := $(VENV)/bin/ruff
ALEMBIC := $(VENV)/bin/alembic

.DEFAULT_GOAL := help
.PHONY: help gate gate-update test test-cov lint fmt types imports migrate run report install-hooks

help: ## Show this help
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) \
	  | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}'

gate: ## Run the full ratchet gate (pytest + pyright + ruff + import-linter)
	$(PY) ci/gate.py

gate-update: ## Rewrite ci/baseline.json from current counts -- review the diff
	$(PY) ci/gate.py --update

test: ## Run the test suite
	$(PYTEST) -q

test-cov: ## Run tests with a coverage report
	$(PYTEST) --cov=app --cov-report=term-missing -q

lint: ## Lint without modifying anything
	$(RUFF) check .

fmt: ## Format and auto-fix
	$(RUFF) check . --fix && $(RUFF) format .

types: ## Type-check (reads pyrightconfig.json)
	pyright --outputjson | jq '.summary'

imports: ## Check the architecture contracts in .importlinter
	$(VENV)/bin/lint-imports

migrate: ## Apply migrations to JOBAGENT_DATABASE_URL
	$(ALEMBIC) upgrade head

run: ## Start the app on 127.0.0.1:5001
	$(VENV)/bin/uvicorn app.fastapi_run:app --host 127.0.0.1 --port 5001 --reload

report: ## Generate a slice report: make report SLICE=slice-3 [BASE=main]
	@test -n "$(SLICE)" || { echo "usage: make report SLICE=<slice-id> [BASE=main]"; exit 1; }
	./scripts/slice_report.sh "$(SLICE)" "$(or $(BASE),main)"

install-hooks: ## Install the pre-commit hooks (config alone is inert)
	$(VENV)/bin/pre-commit install
