from __future__ import annotations

from datetime import UTC, datetime, timedelta
from email.utils import format_datetime

import pytest
import requests
import responses as responses_lib

from include.errors import PermanentHttpError, RateLimitedError, TransientHttpError
from include.extractors.http import HttpClient, parse_retry_after

URL = "https://api.example.test/jobs"


def test_success_sends_user_agent_and_keeps_useful_headers(http, mocked, settings):
    mocked.get(URL, body=b'{"ok":true}', headers={"ETag": '"abc"', "Set-Cookie": "x=1"}, content_type="application/json")
    response = http.get(URL, params={"limit": 20})

    assert response.status == 200
    assert response.body == b'{"ok":true}'
    assert response.attempts == 1
    assert response.headers["etag"] == '"abc"'
    assert "set-cookie" not in response.headers
    assert mocked.calls[0].request.headers["User-Agent"] == settings.http.user_agent
    assert "limit=20" in mocked.calls[0].request.url
    assert http.stats.requests_made == 1 and http.stats.retries == 0


def test_transient_5xx_is_retried_with_exponential_backoff(http, mocked, sleeps):
    mocked.get(URL, status=503)
    mocked.get(URL, status=502)
    mocked.get(URL, body="{}", status=200)

    response = http.get(URL)

    assert response.status == 200 and response.attempts == 3
    assert http.stats.requests_made == 3 and http.stats.retries == 2
    assert sleeps == [2.0, 4.0]  # base 2s doubling (jitter pinned to its maximum)


@pytest.mark.parametrize("status", [500, 501, 520, 522, 524, 599])
def test_every_5xx_including_cdn_codes_is_transient(http, mocked, status):
    # Regression: Cloudflare 522 from WWR was once classified as permanent.
    mocked.get(URL, status=status)
    mocked.get(URL, body="{}", status=200)
    assert http.get(URL).attempts == 2


def test_gives_up_after_max_attempts(http, mocked, sleeps):
    for _ in range(3):
        mocked.get(URL, status=500)
    with pytest.raises(TransientHttpError) as exc_info:
        http.get(URL)
    assert exc_info.value.status == 500
    assert exc_info.value.retryable is True
    assert http.stats.requests_made == 3


def test_429_honours_retry_after(http, mocked, sleeps):
    mocked.get(URL, status=429, headers={"Retry-After": "7"})
    mocked.get(URL, body="{}", status=200)
    http.get(URL)
    assert sleeps == [7.0]


def test_429_with_long_retry_after_defers_to_airflow_retry(http, mocked, sleeps):
    mocked.get(URL, status=429, headers={"Retry-After": "3600"})
    with pytest.raises(RateLimitedError):
        http.get(URL)
    assert http.stats.requests_made == 1  # no hammering while rate limited
    assert sleeps == []


def test_repeated_429_raises_rate_limited(http, mocked):
    for _ in range(3):
        mocked.get(URL, status=429)
    with pytest.raises(RateLimitedError) as exc_info:
        http.get(URL)
    assert exc_info.value.retryable is True


@pytest.mark.parametrize("status", [400, 401, 403, 404, 410])
def test_permanent_4xx_is_not_retried(http, mocked, sleeps, status):
    mocked.get(URL, status=status, body="nope")
    with pytest.raises(PermanentHttpError) as exc_info:
        http.get(URL)
    assert exc_info.value.status == status
    assert exc_info.value.retryable is False
    assert http.stats.requests_made == 1
    assert sleeps == []


def test_timeout_is_retried_then_raised(http, mocked):
    for _ in range(3):
        mocked.get(URL, body=requests.exceptions.ReadTimeout("read timed out"))
    with pytest.raises(TransientHttpError, match="ReadTimeout"):
        http.get(URL)
    assert http.stats.requests_made == 3


def test_connection_error_recovers(http, mocked):
    mocked.get(URL, body=requests.exceptions.ConnectionError("reset"))
    mocked.get(URL, body="{}", status=200)
    assert http.get(URL).attempts == 2


def test_304_accepted_only_when_requested(http, mocked):
    mocked.get(URL, status=304)
    assert http.get(URL, accept_statuses=(200, 304)).status == 304

    mocked.get(URL, status=304)
    with pytest.raises(PermanentHttpError):
        http.get(URL)


def test_min_interval_spaces_requests(settings, mocked):
    clock = {"t": 100.0}
    slept = []

    def sleep(seconds):
        slept.append(seconds)
        clock["t"] += seconds

    client = HttpClient(settings.http, sleep=sleep, monotonic=lambda: clock["t"], jitter=lambda: 1.0)
    mocked.get(URL, body="{}")
    mocked.get(URL, body="{}")
    client.get(URL, min_interval=1.5)
    clock["t"] += 0.5
    client.get(URL, min_interval=1.5)
    assert slept == [pytest.approx(1.0)]


def test_session_is_reused_across_requests(http, mocked):
    mocked.get(URL, body="{}")
    mocked.get(URL, body="{}")
    session = http._session
    http.get(URL)
    http.get(URL)
    assert http._session is session


def test_parse_retry_after_formats():
    now = datetime(2026, 9, 25, tzinfo=UTC)
    assert parse_retry_after("12") == 12.0
    assert parse_retry_after(format_datetime(now + timedelta(seconds=30)), now=now) == pytest.approx(30)
    assert parse_retry_after("garbage") is None
    assert parse_retry_after(None) is None
