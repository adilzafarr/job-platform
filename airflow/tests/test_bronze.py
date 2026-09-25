from __future__ import annotations

from datetime import UTC, date, datetime

import pyarrow as pa
import pytest

from include.errors import PersistenceError
from include.extractors.base import FetchedPage
from include.extractors.http import HttpResponse
from include.storage.bronze import (
    METADATA_COLUMNS,
    RunKey,
    decode_responses,
    encode_responses,
    read_parquet_bytes,
    run_slug,
    to_arrow_table,
    write_parquet_bytes,
)


def _page(body: bytes, **context) -> FetchedPage:
    response = HttpResponse(
        method="GET",
        url="https://api.example.test/jobs?limit=20",
        status=200,
        headers={"content-type": "application/json"},
        body=body,
        fetched_at=datetime(2026, 9, 25, 6, 0, tzinfo=UTC),
        elapsed_ms=120,
        attempts=1,
        request_headers={"If-Modified-Since": "x"},
    )
    return FetchedPage(response=response, context=context, record_count=0)


def test_run_slug_and_layout():
    key = RunKey("remoteok", "manual__2026-09-25T06:12:33.123456+00:00", date(2026, 9, 25))
    assert key.slug == "manual__2026-09-25T06_12_33.123456_00_00"
    assert key.partition_prefix == f"bronze/remoteok/ingestion_date=2026-09-25/run_id={key.slug}/"
    assert key.records_key.endswith("/records.parquet")
    assert key.manifest_key.endswith("/_manifest.json")
    assert key.metrics_key == f"ingestion/metrics/remoteok/ingestion_date=2026-09-25/run_id={key.slug}.json"
    assert run_slug("scheduled__2026-09-25T06:00:00+00:00") == "scheduled__2026-09-25T06_00_00_00_00"


def test_raw_archive_round_trip_is_exact_and_deterministic():
    pages = [_page('{"jobs": ["é"]}'.encode(), page=1, cursor=None), _page(b"\xff\xfe binary", page=2, cursor="c1")]

    archive = encode_responses(pages)
    decoded = decode_responses(archive)

    assert encode_responses(pages) == archive  # deterministic encoding (gzip mtime pinned)
    assert [p.response.body for p in decoded] == [p.response.body for p in pages]
    assert decoded[1].context == {"page": 2, "cursor": "c1"}
    assert decoded[0].response.request_headers == {"If-Modified-Since": "x"}
    assert decoded[0].seq == 1 and decoded[1].seq == 2


def test_raw_archive_survives_unicode_line_separators():
    # Regression: real Himalayas descriptions contain U+2028; splitlines() split the JSON line there.
    body = '{"description": "a b c\x85d\x0be\x1cf"}'.encode()
    assert decode_responses(encode_responses([_page(body)]))[0].response.body == body


def test_raw_archive_detects_corruption():
    import gzip

    archive = gzip.decompress(encode_responses([_page(b'{"a":1}')])).replace(b'{\\"a\\":1}', b'{\\"a\\":2}')
    with pytest.raises(PersistenceError, match="SHA-256"):
        decode_responses(gzip.compress(archive))


def _meta(n: int) -> list[dict]:
    return [
        {
            "_source": "remoteok",
            "_source_job_id": str(i),
            "_pipeline_run_id": "run",
            "_ingestion_date": date(2026, 9, 25),
            "_extracted_at": datetime(2026, 9, 25, tzinfo=UTC),
            "_request_seq": 1,
            "_request_url": "https://x",
            "_request_context": "{}",
            "_record_index": i,
            "_record_sha256": "0" * 64,
        }
        for i in range(n)
    ]


def test_arrow_table_keeps_provider_fields_and_types():
    records = [
        {"id": "1", "position": "Dev", "tags": ["python"], "salary_min": 0, "extra": None},
        {"id": "2", "position": "Ops", "tags": [], "salary_min": 50000, "new_field": {"a": 1}},
    ]
    table = to_arrow_table(_meta(2), records)

    assert table.schema.names[: len(METADATA_COLUMNS)] == list(METADATA_COLUMNS)
    assert table.schema.names[len(METADATA_COLUMNS):] == ["id", "position", "tags", "salary_min", "extra", "new_field"]
    assert table.schema.field("tags").type == pa.list_(pa.string())
    assert table.schema.field("salary_min").type == pa.int64()
    assert table.schema.field("extra").type == pa.string()  # all-null column gets a stable type
    assert table.column("new_field").to_pylist() == [None, {"a": 1}]


def test_arrow_table_mixed_types_fall_back_to_json_text():
    table = to_arrow_table(_meta(3), [{"salary": 100}, {"salary": "$100k"}, {"salary": [1, 2]}])
    assert table.schema.field("salary").type == pa.string()
    assert table.column("salary").to_pylist() == ["100", "$100k", "[1, 2]"]


def test_arrow_table_rejects_metadata_collisions():
    with pytest.raises(PersistenceError, match="collide"):
        to_arrow_table(_meta(1), [{"_source": "evil"}])


def test_parquet_round_trip_with_metadata():
    table = to_arrow_table(_meta(2), [{"id": "1"}, {"id": "2"}])
    data = write_parquet_bytes(table, metadata={"source": "remoteok"})
    back = read_parquet_bytes(data)
    assert back.num_rows == 2
    assert back.schema.metadata[b"bronze.source"] == b"remoteok"
