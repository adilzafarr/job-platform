"""
### job_ingestion

Fetches job postings from **Himalayas, Remote OK, Remotive and We Work
Remotely** into the Bronze layer of the lake (MinIO/S3), incrementally and
idempotently.

```
start ─┬─ himalayas      : extract → validate → load_bronze → commit_state ─┐
       ├─ remoteok       : extract → validate → load_bronze → commit_state ─┤
       ├─ remotive       : extract → validate → load_bronze → commit_state ─┼─ record_run_metrics ─ end
       └─ weworkremotely : extract → validate → load_bronze → commit_state ─┘
```

* Sources are independent: one provider failing never touches another's
  Bronze data, and each group can be cleared/retried on its own.
* `record_run_metrics` always runs and writes a run summary. `end` fails the
  DAG run if any source failed, so a partial failure is never shown as green.
* Retrying a task is safe (deterministic object keys, manifest-based commit).
  Re-running an already committed source/run is a no-op.
* Param `full_refresh=true` ignores incremental state for a manual run
  (Himalayas re-sweeps its initial lookback window).
* Schedule: `JOB_INGESTION_SCHEDULE` (cron, default daily 06:00 UTC; `none` =
  manual only). `catchup=False`: starting the environment never replays
  historical intervals against the APIs.

Returned values (XCom) show request/record counts and the Bronze location
for each task. See `docs/ingestion/` for the design and query guide.
"""

from __future__ import annotations

import logging
import os
from datetime import timedelta

import pendulum
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import DAG, Param, TaskGroup, TriggerRule, get_current_context, task
from airflow.sdk.exceptions import AirflowFailException

from include.config import SOURCES

log = logging.getLogger(__name__)

DAG_ID = "job_ingestion"


def _schedule() -> str | None:
    value = os.environ.get("JOB_INGESTION_SCHEDULE", "0 6 * * *").strip()
    return None if value.lower() in ("", "none", "manual") else value


def _paused_on_creation() -> bool:
    return os.environ.get("JOB_INGESTION_PAUSED_ON_CREATION", "false").strip().lower() in ("1", "true", "yes")


# Per-stage retry policy. HTTP-level retries (3 quick attempts with backoff)
# live inside the extractor; Airflow retries are slower and fewer, so the two
# layers never multiply into a request storm. Stages that only touch the lake
# retry quickly; `validate` re-checks identical data, so it retries once (for
# storage hiccups) and permanent validation errors skip retries entirely.
STAGE_POLICY = {
    "extract": {"retries": 2, "retry_delay": timedelta(minutes=5), "execution_timeout": timedelta(minutes=45)},
    "validate": {"retries": 1, "retry_delay": timedelta(minutes=1), "execution_timeout": timedelta(minutes=10)},
    "load_bronze": {"retries": 2, "retry_delay": timedelta(minutes=1), "execution_timeout": timedelta(minutes=15)},
    "commit_state": {"retries": 2, "retry_delay": timedelta(minutes=1), "execution_timeout": timedelta(minutes=5)},
}

STAGE_DOCS = {
    "extract": "Call the provider API using its incremental state; archive raw responses to Bronze.",
    "validate": "Re-parse the archived responses and run lightweight quality checks.",
    "load_bronze": "Write records.parquet, read it back, then write the _manifest.json commit marker.",
    "commit_state": "Advance the source's incremental state from the committed manifest.",
}


def _run_context(context, source: str):
    from include.pipeline import RunContext

    dag_run = context["dag_run"]
    return RunContext(
        source=source,
        run_id=context["run_id"],
        # run_after is set for scheduled *and* manual runs and is stable across retries.
        ingestion_date=pendulum.instance(dag_run.run_after).in_timezone("UTC").date(),
        try_number=context["ti"].try_number,
        full_refresh=bool(context["params"].get("full_refresh", False)),
    )


def _stage_callback(stage: str, source: str, *, will_retry: bool):
    def callback(context) -> None:
        from include.pipeline import IngestionPipeline

        exc = context.get("exception")
        if isinstance(exc, AirflowFailException) and exc.__cause__ is not None:
            exc = exc.__cause__
        try:
            IngestionPipeline().record_failure(_run_context(context, source), stage, exc, will_retry=will_retry)
        except Exception:  # never let bookkeeping mask the real failure
            log.exception("Could not record %s/%s failure metrics", source, stage)

    return callback


@task
def run_stage(stage: str, source: str) -> dict:
    from include.pipeline import IngestionPipeline, is_retryable

    ctx = _run_context(get_current_context(), source)
    try:
        return getattr(IngestionPipeline(), stage)(ctx)
    except Exception as exc:
        if not is_retryable(exc):
            # Permanent problem (4xx, validation failure): retrying would only repeat it.
            raise AirflowFailException(f"{type(exc).__name__}: {exc}") from exc
        raise


@task(trigger_rule=TriggerRule.ALL_DONE, retries=1, retry_delay=timedelta(minutes=1))
def record_run_metrics() -> dict:
    from include.pipeline import IngestionPipeline

    context = get_current_context()
    run = _run_context(context, SOURCES[0])
    summary = IngestionPipeline().summarize_run(run.run_id, run.ingestion_date, SOURCES)

    log.info("Ingestion run %s: %s", summary["pipeline_run_id"], summary["status"].upper())
    log.info("%-15s %-13s %8s %8s %8s %6s %10s", "source", "status", "requests", "received", "written", "new", "bytes")
    for source, m in summary["sources"].items():
        log.info(
            "%-15s %-13s %8s %8s %8s %6s %10s",
            source, m.get("status"), m.get("requests_made", "-"), m.get("records_received", "-"),
            m.get("records_written", "-"), m.get("new_records", "-"), m.get("bytes_written", "-"),
        )
        if m.get("error"):
            log.info("    error in %s: %s", m["error"].get("stage"), m["error"].get("message"))
    return {k: v for k, v in summary.items() if k != "sources"}


@task(trigger_rule=TriggerRule.ALL_DONE, retries=0)
def end(summary: dict | None) -> str:
    if not summary:
        raise AirflowFailException("Run summary unavailable — record_run_metrics failed")
    if summary["failed_sources"]:
        raise AirflowFailException(
            f"Ingestion incomplete; failed sources: {', '.join(summary['failed_sources'])}. "
            "Successful sources are committed; clear the failed group(s) to retry them."
        )
    return summary["status"]


with DAG(
    dag_id=DAG_ID,
    description="Ingest raw job postings from 4 public job APIs into the Bronze layer",
    schedule=_schedule(),
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=3),
    is_paused_upon_creation=_paused_on_creation(),
    default_args={
        "owner": "data-platform",
        "retry_exponential_backoff": True,
        "max_retry_delay": timedelta(minutes=30),
    },
    params={
        "full_refresh": Param(
            False,
            type="boolean",
            description="Ignore incremental state (conditional headers, hashes, Himalayas watermark) for this run.",
        )
    },
    tags=["ingestion", "bronze"],
    doc_md=__doc__,
) as dag:
    start = EmptyOperator(task_id="start")
    committed = []

    for source in SOURCES:
        with TaskGroup(group_id=source, tooltip=f"{source}: extract → validate → load_bronze → commit_state"):
            previous = start
            for stage, policy in STAGE_POLICY.items():
                current = run_stage.override(
                    task_id=stage,
                    doc_md=STAGE_DOCS[stage],
                    on_failure_callback=_stage_callback(stage, source, will_retry=False),
                    on_retry_callback=_stage_callback(stage, source, will_retry=True),
                    **policy,
                )(stage, source)
                previous >> current
                previous = current
            committed.append(previous)

    summary = record_run_metrics()
    committed >> summary
    end(summary)
