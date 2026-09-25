"""The common extractor contract.

Every provider implements `JobSource`. The contract standardises *how* an
extraction is described — pages fetched, request context, proposed
incremental state, status — but deliberately not *what a job looks like*:
records are returned exactly as the provider shaped them. Canonicalisation is
a Silver-layer concern.

Two operations matter:

- `extract()` performs HTTP requests (via the shared `HttpClient`), decides
  pagination / stop conditions from the provider's incremental state, and
  returns an `ExtractionResult` holding the raw responses plus the *proposed*
  next state. It never persists anything and never commits state.
- `parse_page()` is a pure function from one raw response to provider
  records. It runs during extraction (to paginate) and again in the
  validate/load stages from the archived raw response, so Bronze is always
  derivable from the raw archive.
"""

from __future__ import annotations

import hashlib
import json
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, Callable, ClassVar, Literal

from include.config import Settings
from include.errors import ResponseFormatError
from include.extractors.http import HttpClient, HttpResponse

log = logging.getLogger(__name__)

ExtractionStatus = Literal["fetched", "not_modified"]
IngestMode = Literal["snapshot", "incremental"]


@dataclass
class ParsedPage:
    records: list[dict[str, Any]]
    meta: dict[str, Any] = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)


@dataclass
class FetchedPage:
    """A raw response plus the request context that produced it."""

    response: HttpResponse
    context: dict[str, Any]
    record_count: int


@dataclass
class ExtractionResult:
    status: ExtractionStatus
    pages: list[FetchedPage]
    state_after: dict[str, Any]
    notes: dict[str, Any] = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)

    @property
    def records_received(self) -> int:
        return sum(p.record_count for p in self.pages)


class JobSource(ABC):
    #: Stable machine name, used in storage paths, state and Airflow task ids.
    name: ClassVar[str]
    #: "snapshot": every committed run is a complete copy of the provider feed.
    #: "incremental": every committed run holds only newly published jobs.
    mode: ClassVar[IngestMode]
    #: The provider's own field(s) that identify a job, for documentation and validation.
    id_field: ClassVar[str]
    #: Provider-native fields expected on every record. Used by validation to
    #: detect a changed schema — not to enforce a canonical model.
    required_fields: ClassVar[tuple[str, ...]]
    #: File extension of the raw payload, for humans browsing the archive.
    payload_format: ClassVar[Literal["json", "xml"]] = "json"

    def __init__(
        self,
        settings: Settings,
        http: HttpClient,
        *,
        now: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self.settings = settings
        self.http = http
        self._now = now

    @abstractmethod
    def extract(self, incremental_state: dict[str, Any], *, full_refresh: bool = False) -> ExtractionResult:
        """Fetch whatever the incremental state says is needed."""

    @abstractmethod
    def parse_page(self, response: HttpResponse, context: dict[str, Any]) -> ParsedPage:
        """Turn one raw response into provider-native records (pure; no I/O)."""

    def source_job_id(self, record: dict[str, Any]) -> str | None:
        """The provider's own identifier for a record, as a string."""
        value = record.get(self.id_field)
        if value is None or (isinstance(value, str) and not value.strip()):
            return None
        return str(value)

    # ------------------------------------------------------------ helpers

    @staticmethod
    def body_sha256(response: HttpResponse) -> str:
        return hashlib.sha256(response.body).hexdigest()

    @staticmethod
    def parse_json(response: HttpResponse) -> Any:
        """Decode a JSON body, rejecting HTML error/challenge pages served with 200."""
        ctype = response.content_type.lower()
        if "html" in ctype:
            raise ResponseFormatError(
                f"Expected JSON from {response.url} but got {ctype!r} "
                "(likely an error, block or challenge page)"
            )
        try:
            return json.loads(response.text())
        except json.JSONDecodeError as exc:
            raise ResponseFormatError(f"Invalid JSON from {response.url}: {exc}") from exc

    def __repr__(self) -> str:
        return f"<{type(self).__name__} {self.name}>"
