"""Runtime configuration for the ingestion pipeline, read from environment variables.

Everything that may differ between environments (endpoints, storage location,
schedule, politeness settings) is configurable here. Defaults point at the
public provider endpoints and the local Docker Compose stack.
Secrets (object-storage credentials) have no defaults.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field

#: Source names, in DAG display order. Kept here (dependency-free) so the DAG
#: file can import it cheaply at parse time; the extractor registry must match.
SOURCES: tuple[str, ...] = ("himalayas", "remoteok", "remotive", "weworkremotely")

DEFAULT_WWR_FEEDS = (
    # The 10 leaf category feeds. The main feed (~10 items per category) and
    # remote-programming-jobs (union of the three programming feeds) are
    # redundant. See docs/ingestion/api-research.md.
    "all-other-remote-jobs",
    "remote-back-end-programming-jobs",
    "remote-customer-support-jobs",
    "remote-design-jobs",
    "remote-devops-sysadmin-jobs",
    "remote-front-end-programming-jobs",
    "remote-full-stack-programming-jobs",
    "remote-management-and-finance-jobs",
    "remote-product-jobs",
    "remote-sales-and-marketing-jobs",
)


def _env(name: str, default: str | None = None) -> str | None:
    value = os.environ.get(name)
    if value is None or value.strip() == "":
        return default
    return value.strip()


def _env_int(name: str, default: int) -> int:
    return int(_env(name, str(default)))


def _env_float(name: str, default: float) -> float:
    return float(_env(name, str(default)))


def _env_list(name: str, default: tuple[str, ...]) -> tuple[str, ...]:
    raw = _env(name)
    if raw is None:
        return default
    return tuple(item.strip() for item in raw.split(",") if item.strip())


@dataclass(frozen=True)
class HttpSettings:
    user_agent: str
    connect_timeout: float = 10.0
    read_timeout: float = 90.0
    max_attempts: int = 3
    backoff_base_seconds: float = 2.0
    backoff_max_seconds: float = 60.0
    max_retry_after_seconds: float = 120.0


@dataclass(frozen=True)
class StorageSettings:
    backend: str  # "s3" or "local"
    bucket: str
    endpoint_url: str | None = None
    region: str = "us-east-1"
    access_key_id: str | None = field(default=None, repr=False)
    secret_access_key: str | None = field(default=None, repr=False)
    local_root: str = "/opt/airflow/data/lake"


@dataclass(frozen=True)
class HimalayasSettings:
    url: str = "https://himalayas.app/jobs/api"
    page_limit: int = 20
    max_pages: int = 200
    initial_lookback_hours: int = 48
    overlap_seconds: int = 3600
    request_interval_seconds: float = 1.0


@dataclass(frozen=True)
class RemoteOKSettings:
    url: str = "https://remoteok.com/api"


@dataclass(frozen=True)
class RemotiveSettings:
    url: str = "https://remotive.com/api/remote-jobs"


@dataclass(frozen=True)
class WeWorkRemotelySettings:
    base_url: str = "https://weworkremotely.com/categories"
    feeds: tuple[str, ...] = DEFAULT_WWR_FEEDS
    request_interval_seconds: float = 2.0


@dataclass(frozen=True)
class Settings:
    http: HttpSettings
    storage: StorageSettings
    himalayas: HimalayasSettings
    remoteok: RemoteOKSettings
    remotive: RemotiveSettings
    weworkremotely: WeWorkRemotelySettings


def load_settings() -> Settings:
    """Build settings from the current environment (evaluated at call time)."""
    return Settings(
        http=HttpSettings(
            user_agent=_env(
                "JOB_INGESTION_USER_AGENT",
                "job-platform-ingestion/0.1 (personal job aggregator)",
            ),
            connect_timeout=_env_float("JOB_INGESTION_HTTP_CONNECT_TIMEOUT", 10.0),
            read_timeout=_env_float("JOB_INGESTION_HTTP_READ_TIMEOUT", 90.0),
            max_attempts=_env_int("JOB_INGESTION_HTTP_MAX_ATTEMPTS", 3),
            backoff_base_seconds=_env_float("JOB_INGESTION_HTTP_BACKOFF_BASE", 2.0),
            backoff_max_seconds=_env_float("JOB_INGESTION_HTTP_BACKOFF_MAX", 60.0),
            max_retry_after_seconds=_env_float("JOB_INGESTION_HTTP_MAX_RETRY_AFTER", 120.0),
        ),
        storage=StorageSettings(
            backend=_env("LAKE_STORAGE_BACKEND", "s3"),
            bucket=_env("LAKE_BUCKET", "job-platform-lake"),
            endpoint_url=_env("LAKE_S3_ENDPOINT_URL"),
            region=_env("LAKE_S3_REGION", "us-east-1"),
            access_key_id=_env("LAKE_S3_ACCESS_KEY_ID"),
            secret_access_key=_env("LAKE_S3_SECRET_ACCESS_KEY"),
            local_root=_env("LAKE_LOCAL_ROOT", "/opt/airflow/data/lake"),
        ),
        himalayas=HimalayasSettings(
            url=_env("HIMALAYAS_API_URL", HimalayasSettings.url),
            page_limit=_env_int("HIMALAYAS_PAGE_LIMIT", 20),
            max_pages=_env_int("HIMALAYAS_MAX_PAGES", 200),
            initial_lookback_hours=_env_int("HIMALAYAS_INITIAL_LOOKBACK_HOURS", 48),
            overlap_seconds=_env_int("HIMALAYAS_OVERLAP_SECONDS", 3600),
            request_interval_seconds=_env_float("HIMALAYAS_REQUEST_INTERVAL_SECONDS", 1.0),
        ),
        remoteok=RemoteOKSettings(url=_env("REMOTEOK_API_URL", RemoteOKSettings.url)),
        remotive=RemotiveSettings(url=_env("REMOTIVE_API_URL", RemotiveSettings.url)),
        weworkremotely=WeWorkRemotelySettings(
            base_url=_env("WWR_FEED_BASE_URL", WeWorkRemotelySettings.base_url),
            feeds=_env_list("WWR_FEEDS", DEFAULT_WWR_FEEDS),
            request_interval_seconds=_env_float("WWR_REQUEST_INTERVAL_SECONDS", 2.0),
        ),
    )
