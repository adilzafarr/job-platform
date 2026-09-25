"""Opt-in smoke test against the real provider APIs.

Skipped by default. Run explicitly (5 polite requests: 1 Himalayas page,
Remote OK, Remotive, 2 WWR feeds; writes only to a temp dir):

    RUN_LIVE_TESTS=1 docker compose --profile test run --rm airflow-tests -m live
"""

from __future__ import annotations

import dataclasses
import os

import pytest

from include.extractors import EXTRACTORS
from include.extractors.http import HttpClient

pytestmark = [
    pytest.mark.live,
    pytest.mark.skipif(os.environ.get("RUN_LIVE_TESTS") != "1", reason="set RUN_LIVE_TESTS=1 to call real APIs"),
]


@pytest.mark.parametrize("source", list(EXTRACTORS))
def test_live_extraction(settings, source):
    settings = dataclasses.replace(
        settings,
        himalayas=dataclasses.replace(settings.himalayas, max_pages=1, request_interval_seconds=1.0),
        weworkremotely=dataclasses.replace(settings.weworkremotely, request_interval_seconds=2.0),
    )
    with HttpClient(settings.http) as http:
        extractor = EXTRACTORS[source](settings, http)
        result = extractor.extract({}, full_refresh=True)

    assert result.status == "fetched"
    assert result.records_received > 0
    records = [r for p in result.pages for r in extractor.parse_page(p.response, p.context).records]
    assert all(extractor.source_job_id(r) for r in records)
    for field in extractor.required_fields:
        assert sum(1 for r in records if r.get(field) in (None, "")) / len(records) < 0.05, field
