"""Lightweight Bronze-stage validation.

These checks answer "did extraction work and is the payload what this
provider normally sends?". They do not enforce the future canonical Silver
schema: unknown or new provider fields are always allowed.

Checks:
- the archived responses match what the extract stage recorded
  (request count, byte-for-byte SHA-256, successful HTTP status)
- every archived response still parses (via the provider's own parser)
- a run that fetched data yielded at least one record
- the provider's identity field is present (tolerance: MISSING_RATIO_ERROR)
- the provider's own required fields are present (same tolerance)
- duplicate provider IDs within one run are reported, not rejected
"""

from __future__ import annotations

from collections import Counter
from dataclasses import asdict, dataclass, field
from typing import Any

from include.extractors.base import JobSource
from include.storage.bronze import ArchivedPage, sha256_hex

#: A field missing on more than this share of records fails the run: the
#: provider's schema has most likely changed. Anything above zero warns.
MISSING_RATIO_ERROR = 0.05


@dataclass
class ValidationReport:
    source: str
    records: int = 0
    responses: int = 0
    missing_source_job_id: int = 0
    missing_required: dict[str, int] = field(default_factory=dict)
    duplicate_source_job_ids: int = 0
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.errors

    def to_dict(self) -> dict[str, Any]:
        return {**asdict(self), "ok": self.ok}


def validate_archive(report: ValidationReport, pages: list[ArchivedPage], extraction: dict[str, Any]) -> None:
    """Check the persisted raw archive against the extraction record."""
    expected = extraction.get("requests", [])
    report.responses = len(pages)
    if len(pages) != len(expected):
        report.errors.append(f"Raw archive holds {len(pages)} responses, extraction recorded {len(expected)}")
        return
    for page, recorded in zip(pages, expected):
        if page.response.status != 200:
            report.errors.append(f"Request #{page.seq} archived with HTTP {page.response.status}")
        if sha256_hex(page.response.body) != recorded.get("body_sha256"):
            report.errors.append(f"Request #{page.seq} body differs from the extraction record")


def validate_records(
    report: ValidationReport,
    extractor: JobSource,
    records: list[dict[str, Any]],
    *,
    expected_records: int | None,
) -> None:
    report.records = len(records)
    if expected_records is not None and expected_records != len(records):
        report.errors.append(
            f"Parsed {len(records)} records from the archive but extraction counted {expected_records}"
        )
    if not records:
        report.errors.append("Extraction fetched data but produced zero records")
        return

    ids = [extractor.source_job_id(r) for r in records]
    report.missing_source_job_id = sum(1 for i in ids if i is None)
    _check_missing(report, f"identity field {extractor.id_field!r}", report.missing_source_job_id, len(records))

    for name in extractor.required_fields:
        missing = sum(1 for r in records if r.get(name) in (None, ""))
        if missing:
            report.missing_required[name] = missing
            _check_missing(report, f"required field {name!r}", missing, len(records))

    counts = Counter(i for i in ids if i is not None)
    report.duplicate_source_job_ids = sum(c - 1 for c in counts.values() if c > 1)
    if report.duplicate_source_job_ids:
        report.warnings.append(
            f"{report.duplicate_source_job_ids} duplicate {extractor.id_field!r} values within this run "
            "(kept as received; deduplication belongs to Silver)"
        )


def _check_missing(report: ValidationReport, label: str, missing: int, total: int) -> None:
    if not missing:
        return
    ratio = missing / total
    message = f"{label} missing on {missing}/{total} records ({ratio:.1%})"
    if ratio > MISSING_RATIO_ERROR:
        report.errors.append(message + " — provider schema may have changed")
    else:
        report.warnings.append(message)
