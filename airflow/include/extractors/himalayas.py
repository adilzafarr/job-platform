"""Himalayas — https://himalayas.app/api

Cursor-paginated browse feed, newest first, 20 jobs per page, ~98k live jobs.
The API has no date filter, no updated-at and ignores conditional requests,
so incremental extraction is a *resumable newest-first sweep* down to a
watermark:

    state.last_seen_published_at   stop target of the current sweep
    state.last_cursor              set only while a sweep is unfinished
    state.sweep_high_watermark     newest pubDate seen by the unfinished sweep

A sweep that hits `max_pages` before the watermark saves its next cursor, and
the next run resumes there. The watermark only advances when a sweep
completes, so a backlog never leaves a gap. See docs/ingestion/api-research.md.
"""

from __future__ import annotations

import logging
from typing import Any

from include.errors import EmptyResponseError, InvalidCursorError, PermanentHttpError, SchemaError
from include.extractors.base import ExtractionResult, FetchedPage, JobSource, ParsedPage
from include.extractors.http import HttpResponse

log = logging.getLogger(__name__)


class HimalayasExtractor(JobSource):
    name = "himalayas"
    mode = "incremental"
    id_field = "guid"
    required_fields = ("guid", "pubDate", "title")

    def parse_page(self, response: HttpResponse, context: dict[str, Any]) -> ParsedPage:
        payload = self.parse_json(response)
        if not isinstance(payload, dict) or not isinstance(payload.get("jobs"), list):
            raise SchemaError(
                f"Himalayas response missing 'jobs' list (top-level keys: "
                f"{sorted(payload) if isinstance(payload, dict) else type(payload).__name__})"
            )
        jobs = payload["jobs"]
        non_dicts = [j for j in jobs if not isinstance(j, dict)]
        if non_dicts:
            raise SchemaError(f"Himalayas 'jobs' contains {len(non_dicts)} non-object entries")
        meta = {
            "next_cursor": payload.get("nextCursor") or None,
            "total_count": payload.get("totalCount"),
            "updated_at": payload.get("updatedAt"),
        }
        return ParsedPage(records=jobs, meta=meta)

    def extract(self, incremental_state: dict[str, Any], *, full_refresh: bool = False) -> ExtractionResult:
        cfg = self.settings.himalayas
        now_ts = int(self._now().timestamp())
        warnings: list[str] = []

        previous_watermark = incremental_state.get("last_seen_published_at")
        if full_refresh or previous_watermark is None:
            watermark = now_ts - cfg.initial_lookback_hours * 3600
            resume_cursor = None
            sweep_high = None
        else:
            watermark = int(previous_watermark)
            resume_cursor = incremental_state.get("last_cursor")
            sweep_high = incremental_state.get("sweep_high_watermark")

        stop_below = watermark - cfg.overlap_seconds
        cursor = resume_cursor
        pages: list[FetchedPage] = []
        stop_reason = "max_pages"
        total_count = None

        while len(pages) < cfg.max_pages:
            params: dict[str, str | int] = {"limit": cfg.page_limit}
            if cursor:
                params["cursor"] = cursor
            try:
                response = self.http.get(
                    cfg.url,
                    params=params,
                    headers={"Accept": "application/json"},
                    min_interval=cfg.request_interval_seconds,
                )
            except PermanentHttpError as exc:
                if exc.status == 400 and cursor and cursor == resume_cursor and not pages:
                    # Stored cursor no longer valid (expired / format change):
                    # restart the sweep from the newest job with the same watermark.
                    message = f"Stored cursor rejected by Himalayas ({exc}); restarting sweep from newest"
                    log.warning(message)
                    warnings.append(message)
                    cursor = resume_cursor = sweep_high = None
                    continue
                if exc.status == 400 and cursor:
                    raise InvalidCursorError(str(exc), url=exc.url, status=400) from exc
                raise

            context = {"page": len(pages) + 1, "cursor": cursor, "resumed_sweep": resume_cursor is not None}
            parsed = self.parse_page(response, context)
            pages.append(FetchedPage(response=response, context=context, record_count=len(parsed.records)))
            total_count = parsed.meta.get("total_count", total_count)

            if context["page"] == 1 and not parsed.records and resume_cursor is None:
                raise EmptyResponseError(
                    f"Himalayas returned no jobs on the first page (totalCount={total_count!r}); "
                    "the feed is never legitimately empty"
                )

            published = [r["pubDate"] for r in parsed.records if isinstance(r.get("pubDate"), (int, float))]
            if context["page"] % 10 == 0 or context["page"] == 1:
                log.info(
                    "himalayas: page %d, %d jobs, oldest pubDate %s (stop below %s)",
                    context["page"], len(parsed.records), min(published) if published else None, stop_below,
                )
            if published and sweep_high is None:
                sweep_high = int(max(published))  # newest job of a fresh sweep
            if published and min(published) < stop_below:
                stop_reason = "watermark_reached"
                break
            cursor = parsed.meta["next_cursor"]
            if not cursor:
                stop_reason = "end_of_feed"
                break

        complete = stop_reason != "max_pages"
        if complete:
            state_after = {
                "last_seen_published_at": max(watermark, sweep_high or watermark),
                "last_cursor": None,
                "sweep_high_watermark": None,
            }
        else:
            message = (
                f"Reached HIMALAYAS_MAX_PAGES={cfg.max_pages} before the watermark; "
                "the next run resumes this sweep from the saved cursor"
            )
            log.warning(message)
            warnings.append(message)
            state_after = {
                "last_seen_published_at": watermark,
                "last_cursor": cursor,
                "sweep_high_watermark": sweep_high,
            }

        return ExtractionResult(
            status="fetched",
            pages=pages,
            state_after=state_after,
            notes={
                "stop_reason": stop_reason,
                "sweep_complete": complete,
                "watermark": watermark,
                "stop_below": stop_below,
                "resumed_from_cursor": resume_cursor is not None,
                "total_count": total_count,
                "full_refresh": full_refresh,
            },
            warnings=warnings,
        )
