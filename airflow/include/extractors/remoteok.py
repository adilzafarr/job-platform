"""Remote OK — https://remoteok.com/api

A single JSON array: element 0 is a terms/legal notice carrying
`last_updated`, the rest are the ~100 newest jobs. No pagination, no query
parameters, and no ETag/Last-Modified. The request itself can't be avoided,
but an unchanged body (SHA-256 equal to the last committed snapshot) is
recorded as `not_modified` and nothing is written.
"""

from __future__ import annotations

from typing import Any

from include.errors import EmptyResponseError, SchemaError
from include.extractors.base import ExtractionResult, FetchedPage, JobSource, ParsedPage
from include.extractors.http import HttpResponse


class RemoteOKExtractor(JobSource):
    name = "remoteok"
    mode = "snapshot"
    id_field = "id"
    required_fields = ("id", "epoch", "position")

    def parse_page(self, response: HttpResponse, context: dict[str, Any]) -> ParsedPage:
        payload = self.parse_json(response)
        if not isinstance(payload, list):
            raise SchemaError(f"Remote OK response is a {type(payload).__name__}, expected a JSON array")
        if any(not isinstance(item, dict) for item in payload):
            raise SchemaError("Remote OK array contains non-object entries")

        meta: dict[str, Any] = {}
        warnings: list[str] = []
        jobs = payload
        if payload and "legal" in payload[0]:
            meta["last_updated"] = payload[0].get("last_updated")
            meta["legal_notice_present"] = True
            jobs = payload[1:]
        else:
            warnings.append("Remote OK legal notice element missing; response layout may have changed")
            meta["legal_notice_present"] = False
        return ParsedPage(records=jobs, meta=meta, warnings=warnings)

    def extract(self, incremental_state: dict[str, Any], *, full_refresh: bool = False) -> ExtractionResult:
        response = self.http.get(self.settings.remoteok.url, headers={"Accept": "application/json"})
        context = {"page": 1}
        parsed = self.parse_page(response, context)
        digest = self.body_sha256(response)
        page = FetchedPage(response=response, context=context, record_count=len(parsed.records))
        notes = {"content_sha256": digest, "last_updated": parsed.meta.get("last_updated"), "full_refresh": full_refresh}

        if not full_refresh and digest == incremental_state.get("content_sha256"):
            return ExtractionResult(
                status="not_modified",
                pages=[page],
                state_after=dict(incremental_state),
                notes={**notes, "reason": "content_sha256_unchanged"},
                warnings=parsed.warnings,
            )
        if not parsed.records:
            raise EmptyResponseError("Remote OK returned no jobs (only the legal notice, or an empty array)")

        return ExtractionResult(
            status="fetched",
            pages=[page],
            state_after={"content_sha256": digest, "last_updated": parsed.meta.get("last_updated")},
            notes=notes,
            warnings=parsed.warnings,
        )
