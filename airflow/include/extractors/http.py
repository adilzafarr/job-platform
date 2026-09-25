"""A small, polite HTTP client shared by all extractors.

Responsibilities:
- one pooled `requests.Session` per extraction (connection reuse)
- descriptive User-Agent, explicit connect/read timeouts
- minimum spacing between consecutive requests (per-source politeness)
- a *small* number of in-task retries for transient failures (timeouts,
  connection errors, 5xx, 429) with exponential backoff + jitter, honouring
  `Retry-After`. Airflow task retries sit on top of this, so the in-task
  budget is deliberately low.
- classification of failures into the `include.errors` taxonomy
- request metrics
"""

from __future__ import annotations

import email.utils
import logging
import random
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Callable, Iterable, Mapping

import requests
from requests.adapters import HTTPAdapter

from include.config import HttpSettings
from include.errors import (
    PermanentHttpError,
    RateLimitedError,
    ResponseFormatError,
    TransientHttpError,
)

log = logging.getLogger(__name__)

RETRYABLE_4XX = frozenset({408, 425, 429})


def is_retryable_status(status: int) -> bool:
    """429/408/425 and every 5xx, including CDN codes such as Cloudflare's 520-527
    (522 "origin timed out" was observed from weworkremotely.com)."""
    return status in RETRYABLE_4XX or 500 <= status <= 599

# Response headers worth archiving with the raw body. Everything else
# (cookies, CSP, tracking headers) is noise for debugging ingestion.
_KEPT_HEADERS = frozenset(
    {
        "age",
        "cache-control",
        "cf-cache-status",
        "content-encoding",
        "content-length",
        "content-type",
        "date",
        "etag",
        "expires",
        "last-modified",
        "retry-after",
    }
)


@dataclass(frozen=True)
class HttpResponse:
    """One completed HTTP exchange, as archived in the Bronze raw layer."""

    method: str
    url: str
    status: int
    headers: dict[str, str]
    body: bytes
    fetched_at: datetime
    elapsed_ms: int
    attempts: int
    request_headers: dict[str, str] = field(default_factory=dict)

    @property
    def content_type(self) -> str:
        return self.headers.get("content-type", "")

    def text(self) -> str:
        try:
            return self.body.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise ResponseFormatError(f"Response from {self.url} is not valid UTF-8: {exc}") from exc


@dataclass
class HttpStats:
    requests_made: int = 0
    retries: int = 0
    bytes_received: int = 0
    status_counts: dict[str, int] = field(default_factory=dict)

    def record(self, status: int | str, size: int) -> None:
        self.requests_made += 1
        self.bytes_received += size
        key = str(status)
        self.status_counts[key] = self.status_counts.get(key, 0) + 1


def parse_retry_after(value: str | None, *, now: datetime | None = None) -> float | None:
    """Parse a Retry-After header (delta-seconds or HTTP-date) into seconds."""
    if not value:
        return None
    value = value.strip()
    if value.isdigit():
        return float(value)
    try:
        when = email.utils.parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return None
    if when.tzinfo is None:
        when = when.replace(tzinfo=UTC)
    now = now or datetime.now(UTC)
    return max(0.0, (when - now).total_seconds())


class HttpClient:
    def __init__(
        self,
        settings: HttpSettings,
        *,
        session: requests.Session | None = None,
        sleep: Callable[[float], None] = time.sleep,
        monotonic: Callable[[], float] = time.monotonic,
        jitter: Callable[[], float] = random.random,
    ) -> None:
        self._settings = settings
        self._session = session or self._build_session(settings)
        self._sleep = sleep
        self._monotonic = monotonic
        self._jitter = jitter
        self._last_request_at: float | None = None
        self.stats = HttpStats()

    @staticmethod
    def _build_session(settings: HttpSettings) -> requests.Session:
        session = requests.Session()
        # Retries are handled explicitly below; the adapter only pools connections.
        adapter = HTTPAdapter(pool_connections=4, pool_maxsize=4, max_retries=0)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        session.headers.update({"User-Agent": settings.user_agent})
        return session

    def close(self) -> None:
        self._session.close()

    def __enter__(self) -> "HttpClient":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    # ------------------------------------------------------------------ public

    def get(
        self,
        url: str,
        *,
        params: Mapping[str, str | int] | None = None,
        headers: Mapping[str, str] | None = None,
        accept_statuses: Iterable[int] = (200,),
        min_interval: float = 0.0,
    ) -> HttpResponse:
        """GET with bounded retries. Returns only for a status in `accept_statuses`."""
        accept = frozenset(accept_statuses)
        request_headers = dict(headers or {})
        max_attempts = max(1, self._settings.max_attempts)
        last_error: Exception | None = None

        for attempt in range(1, max_attempts + 1):
            self._pace(min_interval)
            started = self._monotonic()
            fetched_at = datetime.now(UTC)
            try:
                resp = self._session.get(
                    url,
                    params=params,
                    headers=request_headers,
                    timeout=(self._settings.connect_timeout, self._settings.read_timeout),
                )
            except (requests.Timeout, requests.ConnectionError) as exc:
                self.stats.record(type(exc).__name__, 0)
                last_error = TransientHttpError(
                    f"{type(exc).__name__} requesting {url}: {exc}", url=url
                )
                delay = self._backoff(attempt)
            else:
                body = resp.content
                self.stats.record(resp.status_code, len(body))
                elapsed_ms = int((self._monotonic() - started) * 1000)

                if resp.status_code in accept:
                    return HttpResponse(
                        method="GET",
                        url=resp.url,
                        status=resp.status_code,
                        headers=_select_headers(resp.headers),
                        body=body,
                        fetched_at=fetched_at,
                        elapsed_ms=elapsed_ms,
                        attempts=attempt,
                        request_headers=request_headers,
                    )

                snippet = body[:300].decode("utf-8", errors="replace")
                if resp.status_code == 429:
                    retry_after = parse_retry_after(resp.headers.get("Retry-After"))
                    if retry_after is not None and retry_after > self._settings.max_retry_after_seconds:
                        raise RateLimitedError(
                            f"Rate limited by {url}; Retry-After {retry_after:.0f}s exceeds "
                            f"in-task limit — deferring to task retry",
                            url=url,
                            status=429,
                        )
                    last_error = RateLimitedError(f"429 Too Many Requests from {url}", url=url, status=429)
                    delay = max(retry_after or 0.0, self._backoff(attempt))
                elif is_retryable_status(resp.status_code):
                    last_error = TransientHttpError(
                        f"HTTP {resp.status_code} from {url}: {snippet}", url=url, status=resp.status_code
                    )
                    delay = self._backoff(attempt)
                else:
                    raise PermanentHttpError(
                        f"HTTP {resp.status_code} from {resp.url}: {snippet}",
                        url=resp.url,
                        status=resp.status_code,
                    )

            if attempt < max_attempts:
                self.stats.retries += 1
                log.warning(
                    "Attempt %d/%d for %s failed (%s); retrying in %.1fs",
                    attempt, max_attempts, url, last_error, delay,
                )
                self._sleep(delay)

        assert last_error is not None
        raise last_error

    # ----------------------------------------------------------------- helpers

    def _pace(self, min_interval: float) -> None:
        if min_interval > 0 and self._last_request_at is not None:
            wait = min_interval - (self._monotonic() - self._last_request_at)
            if wait > 0:
                self._sleep(wait)
        self._last_request_at = self._monotonic()

    def _backoff(self, attempt: int) -> float:
        """Exponential backoff with 'equal jitter': half fixed, half random."""
        ceiling = min(
            self._settings.backoff_max_seconds,
            self._settings.backoff_base_seconds * (2 ** (attempt - 1)),
        )
        return ceiling / 2 + (ceiling / 2) * self._jitter()


def _select_headers(headers: Mapping[str, str]) -> dict[str, str]:
    kept = {}
    for name, value in headers.items():
        lname = name.lower()
        if lname in _KEPT_HEADERS or lname.startswith(("x-ratelimit", "ratelimit")):
            kept[lname] = value
    return kept
