"""Bronze layer: object layout, raw-response archive and Parquet record files.

Layout (all keys relative to the lake bucket)::

    bronze/<source>/ingestion_date=YYYY-MM-DD/run_id=<run>/
        responses.jsonl.gz   exact HTTP responses (one JSON line per request)
        records.parquet      one row per provider record, queryable
        _manifest.json       commit marker, written last

    ingestion/state/<source>.json                                   incremental state
    ingestion/extractions/<source>/ingestion_date=/run_id=<run>.json  stage hand-off
    ingestion/metrics/<source>/ingestion_date=/run_id=<run>.json      per-source run metrics
    ingestion/runs/ingestion_date=/run_id=<run>.json                  DAG-run summary

One partition per (source, Airflow run): no per-page files, no tiny-file
explosion, and a deterministic key for idempotent retries. `ingestion_date`
is the UTC date of the DAG run's `run_after`, which is stable across retries.

Parquet rows keep provider fields verbatim under their original names.
Ingestion metadata columns all start with `_`.
"""

from __future__ import annotations

import base64
import gzip
import hashlib
import io
import json
import re
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Iterable

import pyarrow as pa
import pyarrow.parquet as pq

from include.errors import PersistenceError
from include.extractors.base import FetchedPage, JobSource
from include.extractors.http import HttpResponse

BRONZE_ROOT = "bronze"
INGESTION_ROOT = "ingestion"
RECORDS_FILE = "records.parquet"
RESPONSES_FILE = "responses.jsonl.gz"
MANIFEST_FILE = "_manifest.json"

METADATA_SCHEMA = pa.schema(
    [
        pa.field("_source", pa.string(), nullable=False),
        pa.field("_source_job_id", pa.string()),
        pa.field("_pipeline_run_id", pa.string(), nullable=False),
        pa.field("_ingestion_date", pa.date32(), nullable=False),
        pa.field("_extracted_at", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("_request_seq", pa.int32(), nullable=False),
        pa.field("_request_url", pa.string(), nullable=False),
        pa.field("_request_context", pa.string()),
        pa.field("_record_index", pa.int32(), nullable=False),
        pa.field("_record_sha256", pa.string(), nullable=False),
    ]
)
METADATA_COLUMNS = tuple(METADATA_SCHEMA.names)

_UNSAFE = re.compile(r"[^A-Za-z0-9._-]+")


def run_slug(run_id: str) -> str:
    """Make an Airflow run_id safe for object keys / hive partition values."""
    return _UNSAFE.sub("_", run_id).strip("_") or "run"


@dataclass(frozen=True)
class RunKey:
    source: str
    run_id: str
    ingestion_date: date

    @property
    def slug(self) -> str:
        return run_slug(self.run_id)

    @property
    def partition_prefix(self) -> str:
        return f"{BRONZE_ROOT}/{self.source}/ingestion_date={self.ingestion_date.isoformat()}/run_id={self.slug}/"

    @property
    def records_key(self) -> str:
        return self.partition_prefix + RECORDS_FILE

    @property
    def responses_key(self) -> str:
        return self.partition_prefix + RESPONSES_FILE

    @property
    def manifest_key(self) -> str:
        return self.partition_prefix + MANIFEST_FILE

    def _ingestion_key(self, kind: str) -> str:
        return (
            f"{INGESTION_ROOT}/{kind}/{self.source}/"
            f"ingestion_date={self.ingestion_date.isoformat()}/run_id={self.slug}.json"
        )

    @property
    def extraction_key(self) -> str:
        return self._ingestion_key("extractions")

    @property
    def metrics_key(self) -> str:
        return self._ingestion_key("metrics")


def state_key(source: str) -> str:
    return f"{INGESTION_ROOT}/state/{source}.json"


def run_summary_key(run_id: str, ingestion_date: date) -> str:
    return f"{INGESTION_ROOT}/runs/ingestion_date={ingestion_date.isoformat()}/run_id={run_slug(run_id)}.json"


def source_prefix(source: str) -> str:
    return f"{BRONZE_ROOT}/{source}/"


# --------------------------------------------------------------------- JSON


def dumps_json(obj: Any) -> bytes:
    return json.dumps(obj, ensure_ascii=False, indent=2, sort_keys=True, default=_json_default).encode("utf-8")


def _json_default(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    raise TypeError(f"Not JSON serialisable: {type(value).__name__}")


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


# -------------------------------------------------------- raw response archive


@dataclass(frozen=True)
class ArchivedPage:
    seq: int
    response: HttpResponse
    context: dict[str, Any]


def encode_responses(pages: Iterable[FetchedPage]) -> bytes:
    """Serialise raw responses as gzip'd JSON Lines, one line per HTTP request.

    Bodies are stored as UTF-8 text when decodable (base64 otherwise) together
    with the SHA-256 of the original bytes, so the exact payload is
    recoverable and verifiable. gzip mtime is pinned so identical input gives
    byte-identical output (useful for idempotency checks).
    """
    lines = []
    for seq, page in enumerate(pages, start=1):
        resp = page.response
        try:
            body, encoding = resp.body.decode("utf-8"), "utf-8"
        except UnicodeDecodeError:
            body, encoding = base64.b64encode(resp.body).decode("ascii"), "base64"
        line = {
            "seq": seq,
            "context": page.context,
            "request": {"method": resp.method, "url": resp.url, "headers": resp.request_headers},
            "response": {
                "status": resp.status,
                "headers": resp.headers,
                "fetched_at": resp.fetched_at.isoformat(),
                "elapsed_ms": resp.elapsed_ms,
                "attempts": resp.attempts,
                "body_bytes": len(resp.body),
                "body_sha256": sha256_hex(resp.body),
            },
            "body_encoding": encoding,
            "body": body,
        }
        lines.append(json.dumps(line, ensure_ascii=False, sort_keys=True))
    payload = ("\n".join(lines) + "\n").encode("utf-8") if lines else b""
    return gzip.compress(payload, compresslevel=6, mtime=0)


def decode_responses(data: bytes) -> list[ArchivedPage]:
    pages = []
    # Split on "\n" only: json.dumps escapes it inside strings, but it leaves
    # U+2028/U+2029/U+0085 raw, and str.splitlines() would break lines there.
    for raw in gzip.decompress(data).decode("utf-8").split("\n"):
        if not raw.strip():
            continue
        line = json.loads(raw)
        body = line["body"].encode("utf-8") if line["body_encoding"] == "utf-8" else base64.b64decode(line["body"])
        meta = line["response"]
        if sha256_hex(body) != meta["body_sha256"]:
            raise PersistenceError(f"Archived body for request #{line['seq']} failed its SHA-256 check")
        response = HttpResponse(
            method=line["request"]["method"],
            url=line["request"]["url"],
            status=meta["status"],
            headers=meta["headers"],
            body=body,
            fetched_at=datetime.fromisoformat(meta["fetched_at"]),
            elapsed_ms=meta["elapsed_ms"],
            attempts=meta["attempts"],
            request_headers=line["request"]["headers"],
        )
        pages.append(ArchivedPage(seq=line["seq"], response=response, context=line["context"]))
    return pages


# ------------------------------------------------------------ Parquet records


def record_sha256(record: dict[str, Any]) -> str:
    canonical = json.dumps(record, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return sha256_hex(canonical.encode("utf-8"))


def build_rows(
    extractor: JobSource, pages: list[ArchivedPage], run_key: RunKey
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Parse archived responses into (metadata rows, provider records), aligned by index."""
    meta_rows: list[dict[str, Any]] = []
    records: list[dict[str, Any]] = []
    for page in pages:
        parsed = extractor.parse_page(page.response, page.context)
        context_json = json.dumps(page.context, ensure_ascii=False, sort_keys=True)
        for index, record in enumerate(parsed.records):
            meta_rows.append(
                {
                    "_source": extractor.name,
                    "_source_job_id": extractor.source_job_id(record),
                    "_pipeline_run_id": run_key.run_id,
                    "_ingestion_date": run_key.ingestion_date,
                    "_extracted_at": page.response.fetched_at,
                    "_request_seq": page.seq,
                    "_request_url": page.response.url,
                    "_request_context": context_json,
                    "_record_index": index,
                    "_record_sha256": record_sha256(record),
                }
            )
            records.append(record)
    return meta_rows, records


def to_arrow_table(meta_rows: list[dict[str, Any]], records: list[dict[str, Any]]) -> pa.Table:
    """Metadata columns (fixed schema) followed by provider fields (inferred types).

    Provider fields keep their original names and, where Arrow can represent
    them, their original JSON types (numbers, strings, booleans, lists,
    objects). A column whose values cannot share one Arrow type (e.g. a field
    that is sometimes a number and sometimes a string) is stored as JSON text
    rather than being coerced or dropped.
    """
    columns: dict[str, pa.Array] = {
        name: pa.array([row[name] for row in meta_rows], type=METADATA_SCHEMA.field(name).type)
        for name in METADATA_COLUMNS
    }
    field_names: list[str] = []
    seen: set[str] = set()
    for record in records:
        for key in record:
            if key not in seen:
                seen.add(key)
                field_names.append(key)
    collisions = seen.intersection(METADATA_COLUMNS)
    if collisions:
        raise PersistenceError(f"Provider fields collide with metadata columns: {sorted(collisions)}")

    for name in field_names:
        columns[name] = _infer_column([record.get(name) for record in records])

    fields = [METADATA_SCHEMA.field(n) for n in METADATA_COLUMNS]
    fields += [pa.field(n, columns[n].type) for n in field_names]
    return pa.Table.from_arrays(list(columns.values()), schema=pa.schema(fields))


def _infer_column(values: list[Any]) -> pa.Array:
    try:
        array = pa.array(values)
    except (pa.ArrowInvalid, pa.ArrowTypeError, pa.ArrowNotImplementedError, OverflowError):
        return pa.array([_as_json_text(v) for v in values], type=pa.string())
    target = _without_null_types(array.type)
    return array if target == array.type else array.cast(target)


def _as_json_text(value: Any) -> str | None:
    if value is None or isinstance(value, str):
        return value
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def _without_null_types(dtype: pa.DataType) -> pa.DataType:
    """Replace Arrow `null` types (all-null columns, empty lists) with string.

    Keeps schemas stable across files so DuckDB can union them by name.
    """
    if pa.types.is_null(dtype):
        return pa.string()
    if pa.types.is_list(dtype) or pa.types.is_large_list(dtype):
        return pa.list_(_without_null_types(dtype.value_type))
    if pa.types.is_struct(dtype):
        return pa.struct([pa.field(f.name, _without_null_types(f.type)) for f in dtype])
    return dtype


def write_parquet_bytes(table: pa.Table, *, metadata: dict[str, str]) -> bytes:
    schema_meta = dict(table.schema.metadata or {})
    schema_meta.update({f"bronze.{k}".encode(): v.encode() for k, v in metadata.items()})
    table = table.replace_schema_metadata(schema_meta)
    sink = io.BytesIO()
    pq.write_table(table, sink, compression="zstd", row_group_size=50_000)
    return sink.getvalue()


def read_parquet_bytes(data: bytes) -> pa.Table:
    return pq.read_table(io.BytesIO(data))
