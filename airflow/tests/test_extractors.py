from __future__ import annotations

import json

import pytest
from responses import matchers

from include.errors import EmptyResponseError, ResponseFormatError, SchemaError, TransientHttpError
from include.extractors import EXTRACTORS
from include.config import SOURCES
from include.extractors.himalayas import HimalayasExtractor
from include.extractors.remoteok import RemoteOKExtractor
from include.extractors.remotive import RemotiveExtractor
from include.extractors.weworkremotely import WeWorkRemotelyExtractor
from tests.conftest import FIXED_NOW
from tests.payloads import (
    HIMALAYAS_URL,
    REMOTEOK_URL,
    REMOTIVE_URL,
    WWR_BASE,
    himalayas_job,
    himalayas_page,
    remoteok_body,
    remoteok_job,
    remotive_body,
    remotive_job,
    wwr_feed,
    wwr_item,
)

NOW_TS = int(FIXED_NOW.timestamp())


def test_registry_matches_configured_sources():
    assert tuple(EXTRACTORS) == SOURCES


# ------------------------------------------------------------------ Himalayas


def _him(settings, http):
    return HimalayasExtractor(settings, http, now=lambda: FIXED_NOW)


def _him_get(mocked, body, cursor=None, **kw):
    params = {"limit": "20", **({"cursor": cursor} if cursor else {})}
    mocked.get(HIMALAYAS_URL, body=body, content_type="application/json",
               match=[matchers.query_param_matcher(params)], **kw)


def test_himalayas_first_run_paginates_back_to_initial_lookback(settings, http, mocked):
    watermark = NOW_TS - 48 * 3600
    _him_get(mocked, himalayas_page([himalayas_job(i, NOW_TS - i * 60) for i in range(20)], "c1"))
    # Page 2 crosses watermark - overlap (1h) at i=10.
    _him_get(mocked, himalayas_page([himalayas_job(20 + i, watermark - i * 400) for i in range(20)], "c2"), cursor="c1")
    _him_get(mocked, himalayas_page([], None), cursor="c2")  # must never be requested

    result = _him(settings, http).extract({})

    assert len(mocked.calls) == 2
    assert result.status == "fetched"
    assert len(result.pages) == 2 and result.records_received == 40
    assert result.notes["stop_reason"] == "watermark_reached"
    assert result.state_after == {"last_seen_published_at": NOW_TS, "last_cursor": None, "sweep_high_watermark": None}
    assert result.pages[1].context == {"page": 2, "cursor": "c1", "resumed_sweep": False}


def test_himalayas_incremental_run_stops_at_watermark(settings, http, mocked):
    last_seen = NOW_TS - 3 * 3600
    overlap = settings.himalayas.overlap_seconds
    jobs = [himalayas_job(i, NOW_TS - i * 600) for i in range(19)] + [himalayas_job(99, last_seen - overlap - 1)]
    _him_get(mocked, himalayas_page(jobs, "c1"))

    result = _him(settings, http).extract({"last_seen_published_at": last_seen})

    assert len(mocked.calls) == 1
    assert result.state_after["last_seen_published_at"] == NOW_TS


def test_himalayas_end_of_feed(settings, http, mocked):
    _him_get(mocked, himalayas_page([himalayas_job(1, NOW_TS)], None, total=1))
    result = _him(settings, http).extract({"last_seen_published_at": NOW_TS - 10})
    assert result.notes["stop_reason"] == "end_of_feed"
    assert result.state_after["last_seen_published_at"] == NOW_TS


def test_himalayas_max_pages_saves_cursor_and_next_run_resumes(settings, http, mocked):
    last_seen = NOW_TS - 10 * 86400
    for page in range(5):  # max_pages=5 in the test settings, all newer than the watermark
        jobs = [himalayas_job(page * 20 + i, NOW_TS - page * 100 - i) for i in range(20)]
        _him_get(mocked, himalayas_page(jobs, f"c{page + 1}"), cursor=f"c{page}" if page else None)

    first = _him(settings, http).extract({"last_seen_published_at": last_seen})

    assert first.notes["stop_reason"] == "max_pages" and not first.notes["sweep_complete"]
    assert first.state_after == {"last_seen_published_at": last_seen, "last_cursor": "c5", "sweep_high_watermark": NOW_TS}
    assert any("resumes" in w for w in first.warnings)

    # Next run: resumes from c5 (never re-reads the newest pages) and completes the sweep.
    _him_get(mocked, himalayas_page([himalayas_job(500, last_seen - 99999)], "c6"), cursor="c5")
    second = _him(settings, http).extract(first.state_after)

    assert second.pages[0].context["cursor"] == "c5" and second.pages[0].context["resumed_sweep"]
    assert second.notes["stop_reason"] == "watermark_reached"
    # The watermark jumps to the newest job of the *whole* sweep, not of this run.
    assert second.state_after == {"last_seen_published_at": NOW_TS, "last_cursor": None, "sweep_high_watermark": None}


def test_himalayas_rejected_cursor_restarts_sweep(settings, http, mocked):
    _him_get(mocked, '{"ok":false,"errors":"Invalid cursor."}', cursor="stale", status=400)
    _him_get(mocked, himalayas_page([himalayas_job(1, NOW_TS)], None))

    result = _him(settings, http).extract(
        {"last_seen_published_at": NOW_TS - 100, "last_cursor": "stale", "sweep_high_watermark": NOW_TS - 50}
    )

    assert any("rejected" in w for w in result.warnings)
    assert result.state_after["last_seen_published_at"] == NOW_TS
    assert result.state_after["last_cursor"] is None


def test_himalayas_full_refresh_ignores_state(settings, http, mocked):
    _him_get(mocked, himalayas_page([himalayas_job(1, NOW_TS - 50 * 3600)], "c1"))
    result = _him(settings, http).extract(
        {"last_seen_published_at": NOW_TS, "last_cursor": "resume-me"}, full_refresh=True
    )
    assert result.pages[0].context["cursor"] is None
    assert result.notes["watermark"] == NOW_TS - 48 * 3600


def test_himalayas_empty_first_page_is_an_error(settings, http, mocked):
    _him_get(mocked, himalayas_page([], None, total=0))
    with pytest.raises(EmptyResponseError):
        _him(settings, http).extract({})


@pytest.mark.parametrize(
    "body, content_type, error",
    [
        ('{"data": []}', "application/json", SchemaError),
        ('{"jobs": ["x"]}', "application/json", SchemaError),
        ("<html>Just a moment...</html>", "text/html", ResponseFormatError),
        ('{"jobs": [', "application/json", ResponseFormatError),
    ],
)
def test_himalayas_malformed_responses(settings, http, mocked, body, content_type, error):
    mocked.get(HIMALAYAS_URL, body=body, content_type=content_type)
    with pytest.raises(error):
        _him(settings, http).extract({})


# ------------------------------------------------------------------ Remote OK


def test_remoteok_strips_legal_notice_and_keeps_fields(settings, http, mocked):
    mocked.get(REMOTEOK_URL, body=remoteok_body([remoteok_job(1), remoteok_job(2)]), content_type="application/json")
    extractor = RemoteOKExtractor(settings, http)

    result = extractor.extract({})

    assert result.status == "fetched" and result.records_received == 2
    assert set(result.state_after) == {"content_sha256", "last_updated"}
    records = extractor.parse_page(result.pages[0].response, {}).records
    assert records[0]["position"] == "Developer 1"  # provider field names preserved
    assert extractor.source_job_id(records[0]) == "1137401"


def test_remoteok_unchanged_payload_is_not_modified(settings, http, mocked):
    body = remoteok_body([remoteok_job(1)])
    mocked.get(REMOTEOK_URL, body=body, content_type="application/json")
    mocked.get(REMOTEOK_URL, body=body, content_type="application/json")
    extractor = RemoteOKExtractor(settings, http)

    first = extractor.extract({})
    second = extractor.extract(first.state_after)

    assert second.status == "not_modified"
    assert second.notes["reason"] == "content_sha256_unchanged"
    assert second.state_after == first.state_after


def test_remoteok_only_legal_notice_is_suspicious_empty(settings, http, mocked):
    mocked.get(REMOTEOK_URL, body=remoteok_body([]), content_type="application/json")
    with pytest.raises(EmptyResponseError):
        RemoteOKExtractor(settings, http).extract({})


def test_remoteok_object_instead_of_array(settings, http, mocked):
    mocked.get(REMOTEOK_URL, body='{"error": "rate limited"}', content_type="application/json")
    with pytest.raises(SchemaError):
        RemoteOKExtractor(settings, http).extract({})


def test_remoteok_missing_legal_notice_warns(settings, http, mocked):
    mocked.get(REMOTEOK_URL, body=remoteok_body([remoteok_job(1)], legal=False), content_type="application/json")
    result = RemoteOKExtractor(settings, http).extract({})
    assert result.records_received == 1
    assert any("legal notice" in w for w in result.warnings)


# ------------------------------------------------------------------ Remotive


def test_remotive_sends_conditional_request_and_handles_304(settings, http, mocked):
    mocked.get(REMOTIVE_URL, status=304)
    state = {"last_modified": "Thu, 24 Sep 2026 11:43:49 GMT", "content_sha256": "abc"}

    result = RemotiveExtractor(settings, http).extract(state)

    assert mocked.calls[0].request.headers["If-Modified-Since"] == "Thu, 24 Sep 2026 11:43:49 GMT"
    assert result.status == "not_modified" and result.notes["reason"] == "http_304_not_modified"
    assert result.state_after == state


def test_remotive_200_records_validators(settings, http, mocked):
    mocked.get(REMOTIVE_URL, body=remotive_body([remotive_job(1), remotive_job(2)]),
               content_type="application/json", headers={"Last-Modified": "Fri, 25 Sep 2026 05:00:00 GMT"})
    result = RemotiveExtractor(settings, http).extract({})
    assert "If-Modified-Since" not in mocked.calls[0].request.headers
    assert result.records_received == 2
    assert result.state_after["last_modified"] == "Fri, 25 Sep 2026 05:00:00 GMT"


def test_remotive_full_refresh_skips_conditional_headers(settings, http, mocked):
    mocked.get(REMOTIVE_URL, body=remotive_body([remotive_job(1)]), content_type="application/json")
    RemotiveExtractor(settings, http).extract({"last_modified": "x"}, full_refresh=True)
    assert "If-Modified-Since" not in mocked.calls[0].request.headers


def test_remotive_zero_jobs_is_an_error(settings, http, mocked):
    mocked.get(REMOTIVE_URL, body=remotive_body([]), content_type="application/json")
    with pytest.raises(EmptyResponseError):
        RemotiveExtractor(settings, http).extract({})


def test_remotive_rate_limit_then_success(settings, http, mocked, sleeps):
    mocked.get(REMOTIVE_URL, status=429, headers={"Retry-After": "30"})
    mocked.get(REMOTIVE_URL, body=remotive_body([remotive_job(1)]), content_type="application/json")
    result = RemotiveExtractor(settings, http).extract({})
    assert result.records_received == 1 and sleeps == [30.0]


def test_remotive_persistent_server_error_propagates(settings, http, mocked):
    for _ in range(3):
        mocked.get(REMOTIVE_URL, status=503)
    with pytest.raises(TransientHttpError):
        RemotiveExtractor(settings, http).extract({})


def test_remotive_count_mismatch_warns(settings, http, mocked):
    payload = json.loads(remotive_body([remotive_job(1)]))
    payload["job-count"] = 5
    mocked.get(REMOTIVE_URL, body=json.dumps(payload), content_type="application/json")
    result = RemotiveExtractor(settings, http).extract({})
    assert any("job-count=5" in w for w in result.warnings)


# ------------------------------------------------------------ We Work Remotely


def _wwr_url(feed):
    return f"{WWR_BASE}/{feed}.rss"


def test_wwr_parses_items_verbatim(settings, http, mocked):
    mocked.get(_wwr_url("remote-design-jobs"), body=wwr_feed([wwr_item("a", media=True), wwr_item("b")]),
               content_type="application/rss+xml")
    mocked.get(_wwr_url("remote-product-jobs"), body=wwr_feed([]), content_type="application/rss+xml")
    extractor = WeWorkRemotelyExtractor(settings, http)

    result = extractor.extract({})

    assert result.status == "fetched" and result.records_received == 2
    assert any("remote-product-jobs" in w and "empty" in w for w in result.warnings)
    item = extractor.parse_page(result.pages[0].response, result.pages[0].context).records[0]
    assert item["title"] == "Acme: a"
    assert item["pubDate"] == "Fri, 25 Sep 2026 00:33:21 +0000"
    assert item["expires_at"] == "Sun, 25 Oct 2026 00:33:21 +0000"
    assert item["state"] == ""
    assert item["country"] == "🇺🇸 United States"
    assert item["description"] == "<p>Job a</p>"
    assert item["media:content"] == {"@url": "https://wwr.example/logo.png", "@medium": "image"}
    assert extractor.source_job_id(item) == "https://weworkremotely.com/remote-jobs/a"
    assert set(result.state_after["feed_sha256"]) == {"remote-design-jobs", "remote-product-jobs"}


def test_wwr_not_modified_only_when_every_feed_is_unchanged(settings, http, mocked):
    design, product = wwr_feed([wwr_item("a")]), wwr_feed([wwr_item("b")])
    for body_design in (design, design, wwr_feed([wwr_item("a"), wwr_item("c")])):
        mocked.get(_wwr_url("remote-design-jobs"), body=body_design, content_type="application/rss+xml")
        mocked.get(_wwr_url("remote-product-jobs"), body=product, content_type="application/rss+xml")
    extractor = WeWorkRemotelyExtractor(settings, http)

    first = extractor.extract({})
    second = extractor.extract(first.state_after)
    third = extractor.extract(second.state_after)

    assert second.status == "not_modified"
    assert third.status == "fetched" and third.notes["changed_feeds"] == ["remote-design-jobs"]
    assert third.records_received == 3  # complete snapshot of all feeds, not just the changed one


def test_wwr_all_feeds_empty_is_an_error(settings, http, mocked):
    for feed in settings.weworkremotely.feeds:
        mocked.get(_wwr_url(feed), body=wwr_feed([]), content_type="application/rss+xml")
    with pytest.raises(EmptyResponseError):
        WeWorkRemotelyExtractor(settings, http).extract({})


@pytest.mark.parametrize(
    "body, error",
    [
        ("<rss><channel><item>", ResponseFormatError),
        ("<feed xmlns='http://www.w3.org/2005/Atom'></feed>", SchemaError),
        (
            '<?xml version="1.0"?><!DOCTYPE r [<!ENTITY a "aaaa"><!ENTITY b "&a;&a;&a;">]>'
            "<rss><channel><item><title>&b;</title></item></channel></rss>",
            ResponseFormatError,
        ),
    ],
    ids=["truncated", "not-rss", "entity-expansion"],
)
def test_wwr_rejects_bad_xml(settings, http, mocked, body, error):
    mocked.get(_wwr_url("remote-design-jobs"), body=body, content_type="application/rss+xml")
    with pytest.raises(error):
        WeWorkRemotelyExtractor(settings, http).extract({})
