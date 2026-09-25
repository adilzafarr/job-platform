"""End-to-end stage tests: mocked HTTP, local object storage, real Parquet + DuckDB."""

from __future__ import annotations

import json
from datetime import date

import pytest
import responses
from responses import matchers

from include.bronze_query import connect
from include.errors import PermanentIngestionError, TransientHttpError, ValidationError
from include.pipeline import RunContext
from include.storage.bronze import decode_responses
from tests.conftest import FIXED_NOW, run_all_stages, run_ctx
from tests.payloads import (
    HIMALAYAS_URL,
    REMOTEOK_URL,
    REMOTIVE_URL,
    himalayas_job,
    himalayas_page,
    remoteok_body,
    remoteok_job,
    remotive_body,
    remotive_job,
)

NOW_TS = int(FIXED_NOW.timestamp())


def _remoteok(mocked, jobs):
    # upsert: replace any earlier registration so each run sees the new payload
    mocked.upsert(responses.GET, REMOTEOK_URL, body=remoteok_body(jobs), content_type="application/json")


def _keys(storage, prefix=""):
    return storage.list_keys(prefix)


def test_successful_run_writes_committed_bronze_partition(pipeline, storage, mocked):
    _remoteok(mocked, [remoteok_job(i) for i in range(3)])
    ctx = run_ctx("remoteok")

    results = run_all_stages(pipeline, ctx)

    assert results["extract"]["records_received"] == 3
    assert results["load_bronze"]["status"] == "committed"
    assert results["load_bronze"]["records_written"] == 3
    assert results["load_bronze"]["new_records"] == 3 and results["load_bronze"]["previously_seen_records"] == 0
    assert results["commit_state"] == {"status": "succeeded", "state_commit": "applied"}

    key = ctx.key
    assert set(_keys(storage, key.partition_prefix)) == {key.records_key, key.responses_key, key.manifest_key}
    manifest = json.loads(storage.get_bytes(key.manifest_key))
    assert manifest["records_written"] == 3 and manifest["state_version_before"] == 0

    state = pipeline.state_store.load("remoteok")
    assert state.version == 1 and state.status == "succeeded"
    assert state.last_successful_run_id == ctx.run_id == state.last_bronze_run_id
    assert set(state.incremental) == {"content_sha256", "last_updated"}

    metrics = json.loads(storage.get_bytes(key.metrics_key))
    assert metrics["status"] == "succeeded"
    assert metrics["requests_made"] == 1 and metrics["pages_fetched"] == 1
    assert metrics["records_received"] == metrics["records_written"] == 3
    assert metrics["bytes_written"] > 0 and metrics["bronze_uri"].endswith(key.partition_prefix)
    assert [a["stage"] for a in metrics["attempts"] if a["status"] == "success"] == [
        "extract", "validate", "load_bronze", "commit_state"
    ]


def test_bronze_is_queryable_with_original_field_names(pipeline, storage, settings, mocked):
    _remoteok(mocked, [remoteok_job(i) for i in range(3)])
    run_all_stages(pipeline, run_ctx("remoteok"))
    mocked.get(REMOTIVE_URL, body=remotive_body([remotive_job(1)]), content_type="application/json")
    run_all_stages(pipeline, run_ctx("remotive"))

    con = connect(settings, storage=storage)

    assert con.sql("SELECT count(*) FROM bronze_remoteok").fetchone() == (3,)
    assert con.sql("SELECT position FROM bronze_remoteok ORDER BY position LIMIT 1").fetchone() == ("Developer 0",)
    assert con.sql("SELECT title, company_name FROM bronze_remotive").fetchone() == ("Backend Engineer 1", "Acme")
    by_source = dict(con.sql("SELECT _source, count(*) FROM bronze_all GROUP BY 1").fetchall())
    assert by_source == {"remoteok": 3, "remotive": 1}
    assert con.sql("SELECT count(*) FROM bronze_himalayas").fetchone() == (0,)  # no data yet: empty view, no error
    assert con.sql("SELECT count(*) FROM ingestion_metrics WHERE status = 'succeeded'").fetchone() == (2,)
    assert con.sql("SELECT count(*) FROM ingestion_state").fetchone() == (2,)
    assert con.sql(
        "SELECT sum(records_written) FROM bronze_manifests WHERE ingestion_date = '2026-09-25'"
    ).fetchone() == (4,)


def test_retrying_extract_overwrites_instead_of_duplicating(pipeline, storage, mocked):
    _remoteok(mocked, [remoteok_job(1)])
    ctx = run_ctx("remoteok")

    pipeline.extract(ctx)
    first_keys = _keys(storage, "bronze/")
    pipeline.extract(RunContext(**{**ctx.__dict__, "try_number": 2}))
    second_keys = _keys(storage, "bronze/")

    # Same deterministic key, overwritten in place: one archive, not two. (The
    # archive itself records the retry's own fetched_at, so bytes may differ.)
    assert first_keys == second_keys == [ctx.key.responses_key]
    pages = decode_responses(storage.get_bytes(ctx.key.responses_key))
    assert len(pages) == 1 and pages[0].response.body == remoteok_body([remoteok_job(1)]).encode()
    run_all_stages_after_extract(pipeline, ctx)
    assert len([k for k in _keys(storage, "bronze/") if k.endswith(".parquet")]) == 1


def run_all_stages_after_extract(pipeline, ctx):
    pipeline.validate(ctx)
    pipeline.load_bronze(ctx)
    pipeline.commit_state(ctx)


def test_rerunning_a_committed_run_is_a_noop_without_requests(pipeline, storage, mocked):
    _remoteok(mocked, [remoteok_job(1)])
    ctx = run_ctx("remoteok")
    run_all_stages(pipeline, ctx)
    calls_before = len(mocked.calls)
    records_before = storage.get_bytes(ctx.key.records_key)

    again = run_all_stages(pipeline, RunContext(**{**ctx.__dict__, "try_number": 3}))

    assert len(mocked.calls) == calls_before  # no API traffic
    assert again["extract"]["status"] == "already_committed"
    assert again["load_bronze"]["status"] == "already_committed"
    assert again["commit_state"]["state_commit"] == "already_applied"
    assert storage.get_bytes(ctx.key.records_key) == records_before
    assert pipeline.state_store.load("remoteok").version == 1


def test_crash_between_manifest_and_state_rolls_forward(pipeline, storage, mocked):
    _remoteok(mocked, [remoteok_job(1)])
    ctx = run_ctx("remoteok")
    pipeline.extract(ctx)
    pipeline.validate(ctx)
    pipeline.load_bronze(ctx)
    # worker dies here: manifest written, state not yet advanced
    assert pipeline.state_store.load("remoteok").version == 0

    # Airflow clears/retries the whole group
    retry = run_all_stages(pipeline, RunContext(**{**ctx.__dict__, "try_number": 2}))

    assert retry["extract"]["status"] == "already_committed"
    assert retry["commit_state"]["state_commit"] == "applied"
    state = pipeline.state_store.load("remoteok")
    assert state.version == 1 and state.incremental["content_sha256"]
    assert len(mocked.calls) == 1


def test_crash_mid_load_leaves_no_committed_data_and_is_cleaned_on_retry(pipeline, storage, mocked):
    _remoteok(mocked, [remoteok_job(1)])
    ctx = run_ctx("remoteok")
    pipeline.extract(ctx)
    storage.put_bytes(ctx.key.records_key, b"partial garbage")  # simulated torn write, no manifest

    con = connect(pipeline.settings, storage=storage)
    assert con.sql("SELECT count(*) FROM bronze_remoteok").fetchone() == (0,)  # uncommitted: invisible

    _remoteok(mocked, [remoteok_job(1)])
    run_all_stages(pipeline, RunContext(**{**ctx.__dict__, "try_number": 2}))
    con = connect(pipeline.settings, storage=storage)
    assert con.sql("SELECT count(*) FROM bronze_remoteok").fetchone() == (1,)


def test_validation_failure_does_not_advance_state(pipeline, storage, mocked):
    jobs = [{**remoteok_job(i), "id": None} for i in range(5)]  # identity field vanished
    _remoteok(mocked, jobs)
    ctx = run_ctx("remoteok")
    pipeline.extract(ctx)

    with pytest.raises(ValidationError, match="identity field 'id' missing on 5/5"):
        pipeline.validate(ctx)
    pipeline.record_failure(ctx, "validate", ValidationError("boom"), will_retry=False)

    with pytest.raises(PermanentIngestionError):
        pipeline.commit_state(ctx)  # nothing committed → refuses to move state
    state = pipeline.state_store.load("remoteok")
    assert state.version == 0 and state.incremental == {}
    assert state.status == "failed" and state.last_error["stage"] == "validate"
    assert not storage.exists(ctx.key.manifest_key)


def test_extract_failure_is_recorded_and_isolated_from_other_sources(pipeline, storage, mocked):
    for _ in range(3):
        mocked.get(REMOTIVE_URL, status=503)
    _remoteok(mocked, [remoteok_job(1)])
    remotive_ctx, remoteok_ctx = run_ctx("remotive"), run_ctx("remoteok")

    with pytest.raises(TransientHttpError) as exc_info:
        pipeline.extract(remotive_ctx)
    pipeline.record_failure(remotive_ctx, "extract", exc_info.value, will_retry=False)
    run_all_stages(pipeline, remoteok_ctx)

    summary = pipeline.summarize_run(remoteok_ctx.run_id, remoteok_ctx.ingestion_date, ("remoteok", "remotive"))
    assert summary["status"] == "partially_failed"
    assert summary["failed_sources"] == ["remotive"]
    assert summary["source_statuses"] == {"remoteok": "succeeded", "remotive": "failed"}
    assert summary["sources"]["remotive"]["error"]["type"] == "TransientHttpError"
    assert storage.exists(remoteok_ctx.key.manifest_key)  # the healthy source is committed
    assert pipeline.state_store.load("remotive").version == 0


def test_retry_callback_marks_attempt_without_failing_state(pipeline, storage):
    ctx = run_ctx("remotive")
    pipeline.record_failure(ctx, "extract", TransientHttpError("503", url="x", status=503), will_retry=True)
    metrics = json.loads(storage.get_bytes(ctx.key.metrics_key))
    assert metrics["status"] == "up_for_retry"
    assert pipeline.state_store.load("remotive").status == "never_run"


def test_not_modified_run_writes_no_bronze(pipeline, storage, mocked):
    mocked.get(REMOTIVE_URL, body=remotive_body([remotive_job(1)]), content_type="application/json",
               headers={"Last-Modified": "Thu, 24 Sep 2026 11:43:49 GMT"})
    run_all_stages(pipeline, run_ctx("remotive", run_id="run-1"))

    mocked.upsert(responses.GET, REMOTIVE_URL, status=304,
               match=[matchers.header_matcher({"If-Modified-Since": "Thu, 24 Sep 2026 11:43:49 GMT"})])
    second = run_ctx("remotive", run_id="run-2", ingestion_date=date(2026, 9, 26))
    results = run_all_stages(pipeline, second)

    assert results["extract"]["status"] == "not_modified"
    assert results["load_bronze"] == {"status": "not_modified", "records_written": 0}
    assert results["commit_state"]["status"] == "not_modified"
    assert _keys(storage, second.key.partition_prefix) == []
    state = pipeline.state_store.load("remotive")
    assert state.status == "not_modified" and state.last_bronze_run_id == "run-1"
    assert state.last_successful_run_id == "run-2"
    assert json.loads(storage.get_bytes(second.key.metrics_key))["not_modified_reason"] == "http_304_not_modified"


def test_new_vs_previously_seen_across_runs(pipeline, storage, mocked):
    _remoteok(mocked, [remoteok_job(1), remoteok_job(2)])
    run_all_stages(pipeline, run_ctx("remoteok", run_id="run-1"))
    _remoteok(mocked, [remoteok_job(2), remoteok_job(3), remoteok_job(4)])

    results = run_all_stages(pipeline, run_ctx("remoteok", run_id="run-2", ingestion_date=date(2026, 9, 26)))

    assert results["load_bronze"]["new_records"] == 2
    assert results["load_bronze"]["previously_seen_records"] == 1


def test_stale_run_cannot_regress_state(pipeline, storage, mocked):
    _remoteok(mocked, [remoteok_job(1)])
    old = run_ctx("remoteok", run_id="run-old")
    pipeline.extract(old)  # old run extracts, then stalls

    _remoteok(mocked, [remoteok_job(1), remoteok_job(2)])
    run_all_stages(pipeline, run_ctx("remoteok", run_id="run-new", ingestion_date=date(2026, 9, 26)))
    newer_state = pipeline.state_store.load("remoteok")

    pipeline.validate(old)
    pipeline.load_bronze(old)  # its raw data is still preserved in Bronze
    assert pipeline.commit_state(old)["state_commit"] == "stale_skipped"
    assert pipeline.state_store.load("remoteok").incremental == newer_state.incremental


def test_himalayas_incremental_state_flows_between_runs(pipeline, storage, mocked):
    last_old = NOW_TS - 48 * 3600 - 3601
    mocked.get(HIMALAYAS_URL, body=himalayas_page([himalayas_job(1, NOW_TS), himalayas_job(2, last_old)], "c1"),
               content_type="application/json", match=[matchers.query_param_matcher({"limit": "20"})])
    run_all_stages(pipeline, run_ctx("himalayas", run_id="run-1"))
    assert pipeline.state_store.load("himalayas").incremental["last_seen_published_at"] == NOW_TS

    mocked.upsert(responses.GET, HIMALAYAS_URL, body=himalayas_page([himalayas_job(3, NOW_TS + 60), himalayas_job(1, NOW_TS - 7200)], "c1"),
               content_type="application/json", match=[matchers.query_param_matcher({"limit": "20"})])
    second = run_ctx("himalayas", run_id="run-2", ingestion_date=date(2026, 9, 26))
    results = run_all_stages(pipeline, second)

    assert results["extract"]["notes"]["stop_reason"] == "watermark_reached"
    assert results["load_bronze"]["new_records"] == 1 and results["load_bronze"]["previously_seen_records"] == 1
    con = connect(pipeline.settings, storage=storage)
    rows = con.sql(
        "SELECT _pipeline_run_id, count(*) FROM bronze_himalayas GROUP BY 1 ORDER BY 1"
    ).fetchall()
    assert rows == [("run-1", 2), ("run-2", 2)]
    assert con.sql("SELECT DISTINCT guid IS NOT NULL, typeof(pubDate) FROM bronze_himalayas").fetchall() == [
        (True, "BIGINT")
    ]


def test_extract_clears_uncommitted_leftovers(pipeline, storage, mocked):
    ctx = run_ctx("remoteok")
    storage.put_bytes(ctx.key.partition_prefix + "stray-from-older-code.bin", b"x")
    _remoteok(mocked, [remoteok_job(1)])
    pipeline.extract(ctx)
    assert _keys(storage, ctx.key.partition_prefix) == [ctx.key.responses_key]


def test_later_stages_require_extraction(pipeline):
    with pytest.raises(PermanentIngestionError, match="extract task must succeed"):
        pipeline.validate(run_ctx("remoteok"))


def test_summary_marks_unstarted_sources(pipeline):
    summary = pipeline.summarize_run("run-x", date(2026, 9, 25), ("remoteok",))
    assert summary["status"] == "failed"
    assert summary["source_statuses"] == {"remoteok": "not_started"}
