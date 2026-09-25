"""The DAG must import cleanly and have the intended shape."""

from __future__ import annotations

import importlib
import os
from pathlib import Path

import pytest

DAGS_FOLDER = Path(__file__).resolve().parents[1] / "dags"
SOURCES = ("himalayas", "remoteok", "remotive", "weworkremotely")
STAGES = ("extract", "validate", "load_bronze", "commit_state")


def _dagbag():
    try:
        from airflow.dag_processing.dagbag import DagBag
    except ImportError:  # older 3.x layout
        from airflow.models.dagbag import DagBag
    return DagBag(dag_folder=str(DAGS_FOLDER))  # examples are off via AIRFLOW__CORE__LOAD_EXAMPLES


@pytest.fixture(scope="module")
def dag():
    bag = _dagbag()
    assert bag.import_errors == {}, bag.import_errors
    assert "job_ingestion" in bag.dags
    return bag.dags["job_ingestion"]


def test_dag_imports_without_errors(dag):
    assert dag.dag_id == "job_ingestion"


def test_scheduling_defaults(dag):
    assert dag.catchup is False
    assert dag.max_active_runs == 1
    assert "full_refresh" in dag.params
    assert dag.params["full_refresh"] is False


def test_each_source_is_an_independent_chain(dag):
    task_ids = set(dag.task_ids)
    for source in SOURCES:
        chain = [dag.get_task(f"{source}.{stage}") for stage in STAGES]
        for upstream, downstream in zip(chain, chain[1:]):
            assert downstream.upstream_task_ids == {upstream.task_id}
        assert chain[0].upstream_task_ids == {"start"}
        assert chain[-1].downstream_task_ids == {"record_run_metrics"}
    assert task_ids == {"start", "record_run_metrics", "end"} | {f"{s}.{t}" for s in SOURCES for t in STAGES}


def test_metrics_and_end_always_run(dag):
    assert dag.get_task("record_run_metrics").trigger_rule == "all_done"
    end = dag.get_task("end")
    assert end.trigger_rule == "all_done"
    assert end.upstream_task_ids == {"record_run_metrics"}


def test_retry_policy_is_bounded(dag):
    extract = dag.get_task("remoteok.extract")
    assert extract.retries == 2
    assert extract.retry_exponential_backoff
    assert extract.execution_timeout is not None
    assert dag.get_task("remoteok.validate").retries == 1
    assert extract.on_failure_callback and extract.on_retry_callback


@pytest.mark.parametrize("value, expected", [("0 */12 * * *", "0 */12 * * *"), ("none", None)])
def test_schedule_is_configurable(monkeypatch, value, expected):
    import sys

    monkeypatch.setenv("JOB_INGESTION_SCHEDULE", value)
    sys.path.insert(0, str(DAGS_FOLDER))
    try:
        module = importlib.import_module("job_ingestion")
        module = importlib.reload(module)
        assert module._schedule() == expected
    finally:
        sys.path.remove(str(DAGS_FOLDER))
        os.environ.pop("JOB_INGESTION_SCHEDULE", None)
