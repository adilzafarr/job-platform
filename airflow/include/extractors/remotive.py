"""Remotive — https://github.com/remotive-com/remote-jobs-api

A single JSON document containing every publicly exposed job (delayed by
24h). No pagination. The endpoint honours `If-Modified-Since` with a 304, so
the stored `Last-Modified` is replayed on every run; a body hash guards the
case where the server answers 200 with unchanged content. Documented limits:
max ~4 requests/day, >2 requests/minute are blocked.
"""

from __future__ import annotations

from typing import Any

from include.errors import EmptyResponseError, SchemaError
from include.extractors.base import ExtractionResult, FetchedPage, JobSource, ParsedPage
from include.extractors.http import HttpResponse


class RemotiveExtractor(JobSource):
    name = "remotive"
    mode = "snapshot"
    id_field = "id"
    required_fields = ("id", "publication_date", "title")

    def parse_page(self, response: HttpResponse, context: dict[str, Any]) -> ParsedPage:
        payload = self.parse_json(response)
        if not isinstance(payload, dict) or not isinstance(payload.get("jobs"), list):
            raise SchemaError(
                f"Remotive response missing 'jobs' list (top-level keys: "
                f"{sorted(payload) if isinstance(payload, dict) else type(payload).__name__})"
            )
        jobs = payload["jobs"]
        if any(not isinstance(j, dict) for j in jobs):
            raise SchemaError("Remotive 'jobs' contains non-object entries")
        warnings = []
        job_count = payload.get("job-count")
        if isinstance(job_count, int) and job_count != len(jobs):
            warnings.append(f"Remotive job-count={job_count} but {len(jobs)} jobs were returned")
        return ParsedPage(
            records=jobs,
            meta={"job_count": job_count, "total_job_count": payload.get("total-job-count")},
            warnings=warnings,
        )

    def extract(self, incremental_state: dict[str, Any], *, full_refresh: bool = False) -> ExtractionResult:
        headers = {"Accept": "application/json"}
        if not full_refresh:
            if incremental_state.get("last_modified"):
                headers["If-Modified-Since"] = incremental_state["last_modified"]
            if incremental_state.get("etag"):
                headers["If-None-Match"] = incremental_state["etag"]

        response = self.http.get(self.settings.remotive.url, headers=headers, accept_statuses=(200, 304))
        context = {"page": 1, "conditional": {k: v for k, v in headers.items() if k.startswith("If-")}}

        if response.status == 304:
            return ExtractionResult(
                status="not_modified",
                pages=[FetchedPage(response=response, context=context, record_count=0)],
                state_after=dict(incremental_state),
                notes={"reason": "http_304_not_modified"},
            )

        parsed = self.parse_page(response, context)
        digest = self.body_sha256(response)
        page = FetchedPage(response=response, context=context, record_count=len(parsed.records))
        state_after = {
            "last_modified": response.headers.get("last-modified"),
            "etag": response.headers.get("etag"),
            "content_sha256": digest,
        }
        state_after = {k: v for k, v in state_after.items() if v is not None}

        if not full_refresh and digest == incremental_state.get("content_sha256"):
            return ExtractionResult(
                status="not_modified",
                pages=[page],
                # Keep the fresher validators even though the content is unchanged.
                state_after={**incremental_state, **state_after},
                notes={"reason": "content_sha256_unchanged"},
                warnings=parsed.warnings,
            )
        if not parsed.records:
            raise EmptyResponseError(
                f"Remotive returned zero jobs (job-count={parsed.meta.get('job_count')!r})"
            )
        return ExtractionResult(
            status="fetched",
            pages=[page],
            state_after=state_after,
            notes={"content_sha256": digest, "full_refresh": full_refresh, **parsed.meta},
            warnings=parsed.warnings,
        )
