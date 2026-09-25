# Convenience wrappers around docker compose. Each target's recipe is also the
# plain command to run if `make` is not installed.

COMPOSE := docker compose
EXEC    := $(COMPOSE) exec airflow-scheduler

.PHONY: up down logs ps test test-live trigger dag-state bronze-ls bronze-sql bronze-shell bronze-init-sql bronze-ui

up:            ## build and start Airflow + Postgres + MinIO
	$(COMPOSE) up -d --build

down:          ## stop the stack (keeps volumes/data)
	$(COMPOSE) down

ps:
	$(COMPOSE) ps

logs:          ## follow scheduler logs
	$(COMPOSE) logs -f airflow-scheduler

test:          ## unit tests + DAG import test (no network)
	$(COMPOSE) --profile test run --rm --build airflow-tests

test-live:     ## opt-in smoke test against the real APIs (5 requests)
	RUN_LIVE_TESTS=1 $(COMPOSE) --profile test run --rm airflow-tests -m live -v

trigger:       ## trigger job_ingestion manually
	$(EXEC) airflow dags trigger job_ingestion

dag-state:     ## list recent runs
	$(EXEC) airflow dags list-runs job_ingestion

bronze-ls:     ## list Bronze objects
	$(EXEC) python -c "from include.config import load_settings; from include.storage.object_storage import storage_from_settings as s; print('\n'.join(s(load_settings().storage).list_keys('bronze/')))"

bronze-sql:    ## run SQL against Bronze:  make bronze-sql Q="SELECT count(*) FROM bronze_all"
	$(EXEC) python -m include.bronze_query "$(Q)"

bronze-shell:  ## interactive DuckDB prompt with Bronze views
	$(COMPOSE) exec -it airflow-scheduler python -m include.bronze_query

bronze-init-sql: ## view definitions for a host DuckDB CLI (MinIO on localhost:9000)
	mkdir -p .duckdb
	$(COMPOSE) exec -T airflow-scheduler python -m include.bronze_query --init-sql --s3-endpoint localhost:9000 > .duckdb/bronze_init.sql

bronze-ui: bronze-init-sql ## DuckDB web UI at http://localhost:4213 (needs the DuckDB CLI on the host)
	duckdb -init .duckdb/bronze_init.sql -ui
