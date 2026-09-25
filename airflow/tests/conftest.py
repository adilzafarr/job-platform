from __future__ import annotations

import dataclasses
from datetime import UTC, date, datetime

import pytest
import responses as responses_lib

from include.config import load_settings
from include.extractors.http import HttpClient
from include.pipeline import IngestionPipeline, RunContext
from include.storage.object_storage import LocalObjectStorage

FIXED_NOW = datetime(2026, 9, 25, 6, 0, 0, tzinfo=UTC)


@pytest.fixture
def settings(tmp_path, monkeypatch):
    for var in ("LAKE_S3_ENDPOINT_URL", "LAKE_S3_ACCESS_KEY_ID", "LAKE_S3_SECRET_ACCESS_KEY", "WWR_FEEDS"):
        monkeypatch.delenv(var, raising=False)
    monkeypatch.setenv("LAKE_STORAGE_BACKEND", "local")
    monkeypatch.setenv("LAKE_LOCAL_ROOT", str(tmp_path / "lake"))
    base = load_settings()
    return dataclasses.replace(
        base,
        himalayas=dataclasses.replace(base.himalayas, max_pages=5, request_interval_seconds=0),
        weworkremotely=dataclasses.replace(
            base.weworkremotely, feeds=("remote-design-jobs", "remote-product-jobs"), request_interval_seconds=0
        ),
    )


@pytest.fixture
def sleeps():
    return []


@pytest.fixture
def http_factory(settings, sleeps):
    def factory() -> HttpClient:
        return HttpClient(settings.http, sleep=sleeps.append, jitter=lambda: 1.0)

    return factory


@pytest.fixture
def http(http_factory):
    client = http_factory()
    yield client
    client.close()


@pytest.fixture
def storage(tmp_path):
    return LocalObjectStorage(tmp_path / "lake", bucket="test-lake")


@pytest.fixture
def pipeline(settings, storage, http_factory):
    return IngestionPipeline(settings, storage, http_factory=http_factory, now=lambda: FIXED_NOW)


@pytest.fixture
def mocked():
    """HTTP mock; any request without a registered response raises ConnectionError."""
    with responses_lib.RequestsMock(assert_all_requests_are_fired=False) as rsps:
        yield rsps


def run_ctx(source: str, run_id: str = "scheduled__2026-09-25T06:00:00+00:00", **kwargs) -> RunContext:
    return RunContext(source=source, run_id=run_id, ingestion_date=kwargs.pop("ingestion_date", date(2026, 9, 25)), **kwargs)


def run_all_stages(pipeline: IngestionPipeline, ctx: RunContext) -> dict:
    results = {}
    for stage in ("extract", "validate", "load_bronze", "commit_state"):
        results[stage] = getattr(pipeline, stage)(ctx)
    return results
