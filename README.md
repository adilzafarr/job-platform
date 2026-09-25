# Job Platform

A personal job aggregation platform. The components:

| Area | Directory | Status |
|---|---|---|
| Ingestion (Airflow → Bronze) | `airflow/` | **Implemented**: see [docs/ingestion](docs/ingestion/README.md) |
| Transformations (Silver/Gold) | `dbt/` | Scaffold |
| API | `backend/` | Scaffold |
| Web app | `frontend/` | Scaffold |
| Resume generation | `resume-engine/` | Scaffold |

## Quick start (ingestion)

```bash
cp .env.example .env              # optional; all values have local defaults
docker compose up -d --build      # Airflow + Postgres + MinIO
# Airflow UI: http://localhost:8080 (airflow / airflow)   MinIO: http://localhost:9001
docker compose exec airflow-scheduler airflow dags trigger job_ingestion
docker compose exec airflow-scheduler python -m include.bronze_query "SELECT _source, count(*) FROM bronze_all GROUP BY 1"
./scripts/bronze-ui.ps1           # browser SQL UI over Bronze (needs: winget install DuckDB.cli)
docker compose --profile test run --rm --build airflow-tests   # unit tests
```

- [Ingestion design, operations and query guide](docs/ingestion/README.md)
- [Provider API research](docs/ingestion/api-research.md)
