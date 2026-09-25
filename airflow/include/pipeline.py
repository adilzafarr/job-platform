"""Stage functions for one source's ingestion, called by the Airflow DAG.

Each source runs four stages, each an Airflow task that can be retried on its own:

    extract      -> HTTP requests; raw responses archived to Bronze; hand-off doc written
    validate     -> archived responses re-parsed and checked; nothing written to Bronze
    load_bronze  -> records.parquet written + read back, then _manifest.json (the commit)
    commit_state -> incremental state advanced from the manifest

Idempotency protocol
--------------------
* Every object key is a pure function of (source, Airflow run_id), so a retry
  overwrites instead of duplicating.
* `_manifest.json` is the commit marker. Once it exists, the partition is
  final: every stage becomes a no-op for that (source, run), and a re-run can
  never replace committed data with a smaller incremental fetch.
* Before extracting, an uncommitted partition left by a crashed attempt is
  cleared.
* State is advanced only by `commit_state`, only from a committed manifest (or
  a `not_modified` extraction), and only if the state version still equals
  the version the extraction started from. A crash between the manifest and the
  state write is therefore rolled forward on retry, and a late retry of an
  old run can't regress state that a newer run already advanced.

Validation and loading re-read the raw archive instead of re-calling the
API, so fixing a parser and clearing `validate` never costs a request.

No Airflow imports here: the stages are plain Python and are unit-tested
without a scheduler.
"""

from __future__ import annotations

import json
import logging
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime
from typing import Any, Callable

from include.bronze_query import count_previously_seen
from include.config import Settings, load_settings
from include.errors import IngestionError, PermanentIngestionError, PersistenceError, ResponseFormatError, ValidationError
from include.extractors import get_extractor_class
from include.extractors.base import JobSource
from include.extractors.http import HttpClient
from include.quality.jobs import ValidationReport, validate_archive, validate_records
from include.storage.bronze import (
    RunKey,
    build_rows,
    decode_responses,
    dumps_json,
    encode_responses,
    read_parquet_bytes,
    run_summary_key,
    sha256_hex,
    to_arrow_table,
    write_parquet_bytes,
)
from include.storage.object_storage import ObjectNotFoundError, ObjectStorage, storage_from_settings
from include.storage.state import SourceState, StateStore

log = logging.getLogger(__name__)

SUCCESS_STATUSES = frozenset({"succeeded", "not_modified"})


@dataclass(frozen=True)
class RunContext:
    source: str
    run_id: str
    ingestion_date: date
    try_number: int = 1
    full_refresh: bool = False

    @property
    def key(self) -> RunKey:
        return RunKey(source=self.source, run_id=self.run_id, ingestion_date=self.ingestion_date)


class IngestionPipeline:
    def __init__(
        self,
        settings: Settings | None = None,
        storage: ObjectStorage | None = None,
        *,
        http_factory: Callable[[], HttpClient] | None = None,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        self.settings = settings or load_settings()
        self.storage = storage or storage_from_settings(self.settings.storage)
        self.state_store = StateStore(self.storage)
        self._http_factory = http_factory or (lambda: HttpClient(self.settings.http))
        self._now = now or (lambda: datetime.now(UTC))

    # ================================================================ stages

    def extract(self, ctx: RunContext) -> dict[str, Any]:
        key = ctx.key
        started = self._now()
        self._record_attempt(ctx, "extract", "running", started_at=started)

        if self.storage.exists(key.manifest_key):
            log.info("%s: run %s already committed; extract is a no-op", ctx.source, ctx.run_id)
            self._record_attempt(ctx, "extract", "skipped_already_committed")
            return {"status": "already_committed", "bronze_uri": self.storage.uri(key.partition_prefix)}

        state = self.state_store.load(ctx.source)
        state.status = "running"
        state.last_attempted_run_id = ctx.run_id
        state.last_attempted_at = started.isoformat()
        self.state_store.save(state)

        removed = self.storage.delete_prefix(key.partition_prefix)
        if removed:
            log.warning("%s: removed %d uncommitted object(s) left by a previous attempt", ctx.source, removed)

        log.info(
            "%s: extracting (mode=%s, full_refresh=%s) from state %s",
            ctx.source, get_extractor_class(ctx.source).mode, ctx.full_refresh,
            json.dumps(state.incremental, default=str),
        )
        with self._http_factory() as http:
            extractor = self._extractor(ctx.source, http)
            result = extractor.extract(state.incremental, full_refresh=ctx.full_refresh)
            http_stats = asdict(http.stats)

        responses_bytes = 0
        if result.status == "fetched":
            archive = encode_responses(result.pages)
            self.storage.put_bytes(key.responses_key, archive, content_type="application/gzip")
            responses_bytes = len(archive)

        completed = self._now()
        requests = [
            {
                "seq": seq,
                "url": p.response.url,
                "status": p.response.status,
                "context": p.context,
                "fetched_at": p.response.fetched_at.isoformat(),
                "elapsed_ms": p.response.elapsed_ms,
                "attempts": p.response.attempts,
                "body_bytes": len(p.response.body),
                "body_sha256": sha256_hex(p.response.body),
                "record_count": p.record_count,
            }
            for seq, p in enumerate(result.pages, start=1)
        ]
        extraction = {
            "source": ctx.source,
            "pipeline_run_id": ctx.run_id,
            "ingestion_date": ctx.ingestion_date.isoformat(),
            "mode": extractor.mode,
            "status": result.status,
            "try_number": ctx.try_number,
            "full_refresh": ctx.full_refresh,
            "started_at": started.isoformat(),
            "completed_at": completed.isoformat(),
            "state_version_before": state.version,
            "state_before": state.incremental,
            "state_after": result.state_after,
            "records_received": result.records_received,
            "requests": requests,
            "http": http_stats,
            "notes": result.notes,
            "warnings": result.warnings,
            "responses_key": key.responses_key if result.status == "fetched" else None,
            "responses_bytes": responses_bytes,
        }
        self.storage.put_bytes(key.extraction_key, dumps_json(extraction), content_type="application/json")

        for warning in result.warnings:
            log.warning("%s: %s", ctx.source, warning)
        summary = {
            "status": result.status,
            "requests_made": http_stats["requests_made"],
            "http_retries": http_stats["retries"],
            "pages_fetched": len(result.pages),
            "bytes_received": http_stats["bytes_received"],
            "records_received": result.records_received,
            "notes": result.notes,
            "raw_archive_uri": self.storage.uri(key.responses_key) if result.status == "fetched" else None,
        }
        self._update_metrics(
            ctx,
            stage="extract",
            mode=extractor.mode,
            requests_made=summary["requests_made"],
            http_retries=summary["http_retries"],
            http_status_counts=http_stats["status_counts"],
            pages_fetched=summary["pages_fetched"],
            bytes_received=summary["bytes_received"],
            records_received=summary["records_received"],
            extraction_status=result.status,
            not_modified_reason=result.notes.get("reason") if result.status == "not_modified" else None,
            extract_notes=result.notes,
            warnings=result.warnings,
        )
        self._record_attempt(ctx, "extract", "success")
        log.info(
            "%s: extraction %s — %d request(s), %d page(s), %d record(s), %d bytes received; notes=%s",
            ctx.source, result.status, summary["requests_made"], summary["pages_fetched"],
            summary["records_received"], summary["bytes_received"], json.dumps(result.notes, default=str),
        )
        return summary

    def validate(self, ctx: RunContext) -> dict[str, Any]:
        key = ctx.key
        self._record_attempt(ctx, "validate", "running")
        if self.storage.exists(key.manifest_key):
            self._record_attempt(ctx, "validate", "skipped_already_committed")
            return {"status": "already_committed"}
        extraction = self._load_extraction(ctx)
        if extraction["status"] == "not_modified":
            log.info("%s: source not modified (%s); nothing to validate", ctx.source, extraction["notes"].get("reason"))
            self._record_attempt(ctx, "validate", "success")
            return {"status": "not_modified"}

        extractor = self._extractor(ctx.source, None)
        report = ValidationReport(source=ctx.source)
        pages = self._load_archive(key)
        validate_archive(report, pages, extraction)
        records: list[dict[str, Any]] = []
        if report.ok:
            try:
                _, records = build_rows(extractor, pages, key)
            except ResponseFormatError as exc:
                report.errors.append(f"Archived response no longer parses: {exc}")
        if report.ok:
            validate_records(report, extractor, records, expected_records=extraction["records_received"])

        self._update_metrics(ctx, stage="validate", validation=report.to_dict())
        for warning in report.warnings:
            log.warning("%s: %s", ctx.source, warning)
        if not report.ok:
            raise ValidationError(f"{ctx.source} failed validation: " + "; ".join(report.errors))
        self._record_attempt(ctx, "validate", "success")
        log.info("%s: validation passed for %d record(s) in %d response(s)", ctx.source, report.records, report.responses)
        return {"status": "valid", **report.to_dict()}

    def load_bronze(self, ctx: RunContext) -> dict[str, Any]:
        key = ctx.key
        self._record_attempt(ctx, "load_bronze", "running")
        if self.storage.exists(key.manifest_key):
            manifest = self._get_json(key.manifest_key)
            log.info("%s: run already committed at %s", ctx.source, self.storage.uri(key.partition_prefix))
            self._record_attempt(ctx, "load_bronze", "skipped_already_committed")
            return {"status": "already_committed", "records_written": manifest["records_written"],
                    "bronze_uri": self.storage.uri(key.partition_prefix)}
        extraction = self._load_extraction(ctx)
        if extraction["status"] == "not_modified":
            self._update_metrics(ctx, stage="load_bronze", records_written=0, bytes_written=0, bronze_uri=None)
            self._record_attempt(ctx, "load_bronze", "success")
            return {"status": "not_modified", "records_written": 0}

        extractor = self._extractor(ctx.source, None)
        pages = self._load_archive(key)
        meta_rows, records = build_rows(extractor, pages, key)
        table = to_arrow_table(meta_rows, records)
        parquet = write_parquet_bytes(
            table,
            metadata={"source": ctx.source, "pipeline_run_id": ctx.run_id, "ingestion_date": ctx.ingestion_date.isoformat()},
        )
        self.storage.put_bytes(key.records_key, parquet, content_type="application/vnd.apache.parquet")

        # Read back before committing: the manifest must only point at readable files.
        stored = self.storage.get_bytes(key.records_key)
        if sha256_hex(stored) != sha256_hex(parquet):
            raise PersistenceError(f"{key.records_key} read back with a different checksum")
        if read_parquet_bytes(stored).num_rows != table.num_rows:
            raise PersistenceError(f"{key.records_key} read back with a different row count")

        new_records = previously_seen = None
        try:
            new_records, previously_seen = count_previously_seen(self.settings, self.storage, ctx.source, key.records_key)
        except Exception as exc:  # a metric must never fail ingestion
            log.warning("%s: could not compute new/previously-seen counts: %s", ctx.source, exc)

        responses_bytes = int(extraction.get("responses_bytes") or 0)
        manifest = {
            "manifest_version": 1,
            "source": ctx.source,
            "pipeline_run_id": ctx.run_id,
            "ingestion_date": ctx.ingestion_date.isoformat(),
            "mode": extraction["mode"],
            "committed_at": self._now().isoformat(),
            "try_number": ctx.try_number,
            "records_written": table.num_rows,
            "requests": len(extraction["requests"]),
            "files": {
                "records": {"key": key.records_key, "bytes": len(parquet), "sha256": sha256_hex(parquet), "rows": table.num_rows},
                "responses": {"key": key.responses_key, "bytes": responses_bytes},
            },
            "columns": table.schema.names,
            "new_records": new_records,
            "previously_seen_records": previously_seen,
            "state_version_before": extraction["state_version_before"],
            "state_after": extraction["state_after"],
            "extract_notes": extraction["notes"],
        }
        manifest_bytes = dumps_json(manifest)
        self.storage.put_bytes(key.manifest_key, manifest_bytes, content_type="application/json")

        bytes_written = len(parquet) + responses_bytes + len(manifest_bytes)
        bronze_uri = self.storage.uri(key.partition_prefix)
        self._update_metrics(
            ctx,
            stage="load_bronze",
            records_written=table.num_rows,
            bytes_written=bytes_written,
            new_records=new_records,
            previously_seen_records=previously_seen,
            bronze_uri=bronze_uri,
        )
        self._record_attempt(ctx, "load_bronze", "success")
        log.info(
            "%s: committed %d record(s) (%d new, %s previously seen), %d bytes, to %s",
            ctx.source, table.num_rows, new_records or 0, previously_seen, bytes_written, bronze_uri,
        )
        return {"status": "committed", "records_written": table.num_rows, "bytes_written": bytes_written,
                "new_records": new_records, "previously_seen_records": previously_seen, "bronze_uri": bronze_uri}

    def commit_state(self, ctx: RunContext) -> dict[str, Any]:
        key = ctx.key
        self._record_attempt(ctx, "commit_state", "running")
        state = self.state_store.load(ctx.source)

        if self.storage.exists(key.manifest_key):
            manifest = self._get_json(key.manifest_key)
            outcome = self._apply_state(state, ctx, manifest["state_version_before"], manifest["state_after"], "succeeded", True)
            final_status = "succeeded"
        else:
            extraction = self._load_extraction(ctx)
            if extraction["status"] != "not_modified":
                raise PermanentIngestionError(
                    f"{ctx.source}: no Bronze manifest for run {ctx.run_id}; refusing to advance state"
                )
            outcome = self._apply_state(
                state, ctx, extraction["state_version_before"], extraction["state_after"], "not_modified", False
            )
            final_status = "not_modified"

        metrics = self._get_json(key.metrics_key) or {}
        completed = self._now()
        started = metrics.get("started_at")
        duration = (completed - datetime.fromisoformat(started)).total_seconds() if started else None
        self._update_metrics(
            ctx,
            stage="commit_state",
            status=final_status,
            completed_at=completed.isoformat(),
            duration_seconds=duration,
            state_commit=outcome,
            state_after=self.state_store.load(ctx.source).incremental,
            error=None,
        )
        self._record_attempt(ctx, "commit_state", "success")
        log.info("%s: state commit %s (run status %s)", ctx.source, outcome, final_status)
        return {"status": final_status, "state_commit": outcome}

    # ============================================================ failures

    def record_failure(self, ctx: RunContext, stage: str, exc: BaseException | None, *, will_retry: bool) -> None:
        """Called from Airflow failure/retry callbacks. Never advances incremental state."""
        error = {
            "stage": stage,
            "type": type(exc).__name__ if exc else "Unknown",
            "message": str(exc)[:2000] if exc else "task failed without an exception (killed or timed out)",
            "retryable": getattr(exc, "retryable", None),
            "try_number": ctx.try_number,
            "at": self._now().isoformat(),
        }
        status = "up_for_retry" if will_retry else "failed"
        self._update_metrics(ctx, stage=stage, status=status, error=error)
        self._record_attempt(ctx, stage, status, error=error["message"])
        if not will_retry:
            state = self.state_store.load(ctx.source)
            state.status = "failed"
            state.last_error = {**error, "run_id": ctx.run_id}
            self.state_store.save(state)

    # ======================================================== run summary

    def summarize_run(self, run_id: str, ingestion_date: date, sources: tuple[str, ...]) -> dict[str, Any]:
        per_source: dict[str, dict[str, Any]] = {}
        for source in sources:
            key = RunKey(source=source, run_id=run_id, ingestion_date=ingestion_date)
            metrics = self._get_json(key.metrics_key)
            per_source[source] = metrics or {"source": source, "status": "not_started"}

        statuses = {s: m.get("status", "unknown") for s, m in per_source.items()}
        ok = [s for s, st in statuses.items() if st in SUCCESS_STATUSES]
        overall = "succeeded" if len(ok) == len(sources) else ("failed" if not ok else "partially_failed")

        def total(field: str) -> int:
            return sum(int(m.get(field) or 0) for m in per_source.values())

        summary = {
            "pipeline_run_id": run_id,
            "ingestion_date": ingestion_date.isoformat(),
            "status": overall,
            "source_statuses": statuses,
            "failed_sources": sorted(s for s in sources if s not in ok),
            "requests_made": total("requests_made"),
            "records_received": total("records_received"),
            "records_written": total("records_written"),
            "bytes_written": total("bytes_written"),
            "sources": per_source,
            "summarized_at": self._now().isoformat(),
        }
        self.storage.put_bytes(run_summary_key(run_id, ingestion_date), dumps_json(summary), content_type="application/json")
        return summary

    # ============================================================= helpers

    def _extractor(self, source: str, http: HttpClient | None) -> JobSource:
        return get_extractor_class(source)(self.settings, http, now=self._now)  # type: ignore[arg-type]

    def _get_json(self, key: str) -> dict[str, Any] | None:
        try:
            return json.loads(self.storage.get_bytes(key))
        except ObjectNotFoundError:
            return None

    def _load_extraction(self, ctx: RunContext) -> dict[str, Any]:
        extraction = self._get_json(ctx.key.extraction_key)
        if extraction is None or extraction.get("pipeline_run_id") != ctx.run_id:
            raise PermanentIngestionError(
                f"{ctx.source}: no extraction record for run {ctx.run_id}; the extract task must succeed first"
            )
        return extraction

    def _load_archive(self, key: RunKey):
        try:
            return decode_responses(self.storage.get_bytes(key.responses_key))
        except ObjectNotFoundError as exc:
            raise PersistenceError(f"Raw archive {key.responses_key} is missing; re-run extract") from exc

    def _apply_state(
        self,
        state: SourceState,
        ctx: RunContext,
        version_before: int,
        state_after: dict[str, Any],
        status: str,
        wrote_bronze: bool,
    ) -> str:
        if state.last_successful_run_id == ctx.run_id and state.version == version_before + 1:
            return "already_applied"
        if state.version != version_before:
            log.warning(
                "%s: state moved from version %d to %d since run %s extracted; not applying its state "
                "(a newer run already covered this window)",
                ctx.source, version_before, state.version, ctx.run_id,
            )
            return "stale_skipped"
        state.incremental = state_after
        state.version += 1
        state.status = status
        state.last_successful_run_id = ctx.run_id
        state.last_successful_at = self._now().isoformat()
        if wrote_bronze:
            state.last_bronze_run_id = ctx.run_id
        state.last_error = None
        self.state_store.save(state)
        return "applied"

    def _update_metrics(self, ctx: RunContext, **fields: Any) -> None:
        key = ctx.key
        metrics = self._get_json(key.metrics_key) or {
            "source": ctx.source,
            "pipeline_run_id": ctx.run_id,
            "ingestion_date": ctx.ingestion_date.isoformat(),
            "attempts": [],
        }
        if "status" not in fields and metrics.get("status") in (None, "up_for_retry"):
            fields["status"] = "running"
        metrics.update(fields)
        metrics["try_number"] = ctx.try_number
        metrics["updated_at"] = self._now().isoformat()
        self.storage.put_bytes(key.metrics_key, dumps_json(metrics), content_type="application/json")

    def _record_attempt(
        self, ctx: RunContext, stage: str, status: str, *, started_at: datetime | None = None, error: str | None = None
    ) -> None:
        key = ctx.key
        metrics = self._get_json(key.metrics_key) or {
            "source": ctx.source,
            "pipeline_run_id": ctx.run_id,
            "ingestion_date": ctx.ingestion_date.isoformat(),
            "attempts": [],
        }
        entry = {"stage": stage, "try_number": ctx.try_number, "status": status, "at": self._now().isoformat()}
        if error:
            entry["error"] = error
        metrics.setdefault("attempts", []).append(entry)
        if started_at and not metrics.get("started_at"):
            metrics["started_at"] = started_at.isoformat()
        if status == "running" and metrics.get("status") not in SUCCESS_STATUSES:
            metrics["status"] = "running"
        metrics["updated_at"] = self._now().isoformat()
        self.storage.put_bytes(key.metrics_key, dumps_json(metrics), content_type="application/json")


def is_retryable(exc: BaseException) -> bool:
    return exc.retryable if isinstance(exc, IngestionError) else True
