"""Query the Bronze layer with DuckDB — no warehouse, just Parquet + JSON in the lake.

Views created on connect:

    bronze_himalayas, bronze_remoteok, bronze_remotive, bronze_weworkremotely
        provider records with their original fields + `_` metadata columns
    bronze_all            metadata columns of all four sources (UNION ALL BY NAME)
    bronze_manifests      one row per committed Bronze partition
    ingestion_metrics     one row per (source, pipeline run)
    ingestion_runs        one row per DAG run (summary written by record_run_metrics)
    ingestion_state       current incremental state per source

Only *committed* partitions (those with a `_manifest.json`) are exposed, so a
partially written, never-committed run can never leak into queries. The file
list is resolved when the connection is created; reconnect to see new runs.

CLI (inside the Airflow containers; see Makefile / docs):

    python -m include.bronze_query "SELECT _source, count(*) FROM bronze_all GROUP BY 1"
    python -m include.bronze_query                      # interactive prompt
    python -m include.bronze_query --init-sql --s3-endpoint localhost:9000 > bronze.sql
"""

from __future__ import annotations

import argparse
import sys
from urllib.parse import urlparse

from include.config import SOURCES, Settings, StorageSettings, load_settings
from include.storage.bronze import (
    BRONZE_ROOT,
    INGESTION_ROOT,
    MANIFEST_FILE,
    METADATA_SCHEMA,
    RECORDS_FILE,
    source_prefix,
)
from include.storage.object_storage import ObjectStorage, storage_from_settings

_DUCKDB_TYPES = {
    "string": "VARCHAR",
    "date32[day]": "DATE",
    "timestamp[us, tz=UTC]": "TIMESTAMPTZ",
    "int32": "INTEGER",
}


def _quote(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _sql_list(values: list[str]) -> str:
    return "[" + ", ".join(_quote(v) for v in values) + "]"


def committed_record_keys(storage: ObjectStorage, source: str) -> list[str]:
    """records.parquet keys whose partition has a commit manifest."""
    keys = set(storage.list_keys(source_prefix(source)))
    return sorted(
        k for k in keys if k.endswith("/" + RECORDS_FILE) and k[: -len(RECORDS_FILE)] + MANIFEST_FILE in keys
    )


def _secret_sql(storage: StorageSettings, endpoint_override: str | None) -> list[str]:
    stmts = ["INSTALL httpfs", "LOAD httpfs"]
    endpoint = endpoint_override or storage.endpoint_url
    parts = [f"TYPE s3", f"REGION {_quote(storage.region)}"]
    if storage.access_key_id and storage.secret_access_key:
        parts += [f"KEY_ID {_quote(storage.access_key_id)}", f"SECRET {_quote(storage.secret_access_key)}"]
    else:
        parts.append("PROVIDER credential_chain")
    if endpoint:
        parsed = urlparse(endpoint if "://" in endpoint else f"http://{endpoint}")
        parts += [
            f"ENDPOINT {_quote(parsed.netloc)}",
            "URL_STYLE 'path'",
            f"USE_SSL {'true' if parsed.scheme == 'https' else 'false'}",
        ]
    stmts.append(f"CREATE OR REPLACE SECRET lake_s3 ({', '.join(parts)})")
    return stmts


def _empty_bronze_view(name: str) -> str:
    cols = ", ".join(
        f"NULL::{_DUCKDB_TYPES[str(f.type)]} AS {f.name}" for f in METADATA_SCHEMA
    )
    return f"CREATE OR REPLACE VIEW {name} AS SELECT {cols} WHERE false"


def _json_view(name: str, uris: list[str]) -> str:
    if not uris:
        return f"CREATE OR REPLACE VIEW {name} AS SELECT NULL::VARCHAR AS source WHERE false"
    return (
        f"CREATE OR REPLACE VIEW {name} AS SELECT * FROM "
        f"read_json_auto({_sql_list(uris)}, union_by_name=true, format='auto')"
    )


def build_init_sql(
    settings: Settings,
    storage: ObjectStorage,
    *,
    endpoint_override: str | None = None,
) -> list[str]:
    stmts: list[str] = []
    if settings.storage.backend == "s3":
        stmts += _secret_sql(settings.storage, endpoint_override)

    for source in SOURCES:
        files = [storage.uri(k) for k in committed_record_keys(storage, source)]
        view = f"bronze_{source}"
        if files:
            stmts.append(
                f"CREATE OR REPLACE VIEW {view} AS SELECT * FROM "
                f"read_parquet({_sql_list(files)}, union_by_name=true, hive_partitioning=false)"
            )
        else:
            stmts.append(_empty_bronze_view(view))

    meta_cols = ", ".join(METADATA_SCHEMA.names)
    union = " UNION ALL BY NAME ".join(f"SELECT {meta_cols} FROM bronze_{s}" for s in SOURCES)
    stmts.append(f"CREATE OR REPLACE VIEW bronze_all AS {union}")

    def uris(prefix: str, suffix: str) -> list[str]:
        return [storage.uri(k) for k in storage.list_keys(prefix) if k.endswith(suffix)]

    stmts.append(_json_view("bronze_manifests", uris(f"{BRONZE_ROOT}/", "/" + MANIFEST_FILE)))
    stmts.append(_json_view("ingestion_metrics", uris(f"{INGESTION_ROOT}/metrics/", ".json")))
    stmts.append(_json_view("ingestion_runs", uris(f"{INGESTION_ROOT}/runs/", ".json")))
    stmts.append(_json_view("ingestion_state", uris(f"{INGESTION_ROOT}/state/", ".json")))
    return stmts


def connect(
    settings: Settings | None = None,
    *,
    storage: ObjectStorage | None = None,
    endpoint_override: str | None = None,
    database: str = ":memory:",
):
    import duckdb

    settings = settings or load_settings()
    storage = storage or storage_from_settings(settings.storage)
    con = duckdb.connect(database)
    for stmt in build_init_sql(settings, storage, endpoint_override=endpoint_override):
        con.execute(stmt)
    return con


def count_previously_seen(
    settings: Settings,
    storage: ObjectStorage,
    source: str,
    current_records_key: str,
) -> tuple[int, int]:
    """(new, previously_seen) distinct provider IDs of one run vs. all earlier committed runs."""
    import duckdb

    con = duckdb.connect()
    try:
        if settings.storage.backend == "s3":
            for stmt in _secret_sql(settings.storage, None):
                con.execute(stmt)
        current = storage.uri(current_records_key)
        earlier = [storage.uri(k) for k in committed_record_keys(storage, source) if k != current_records_key]
        current_ids = (
            f"SELECT DISTINCT _source_job_id AS id FROM read_parquet({_quote(current)}) "
            "WHERE _source_job_id IS NOT NULL"
        )
        if not earlier:
            (total,) = con.execute(f"SELECT count(*) FROM ({current_ids})").fetchone()
            return int(total), 0
        earlier_ids = (
            f"SELECT DISTINCT _source_job_id AS id FROM read_parquet({_sql_list(earlier)}, union_by_name=true) "
            "WHERE _source_job_id IS NOT NULL"
        )
        total, seen = con.execute(
            f"SELECT count(*), count(*) FILTER (WHERE id IN ({earlier_ids})) FROM ({current_ids})"
        ).fetchone()
        return int(total) - int(seen), int(seen)
    finally:
        con.close()


# ----------------------------------------------------------------------- CLI


def _run(con, sql: str, max_rows: int) -> None:
    relation = con.sql(sql)
    if relation is not None:
        relation.show(max_rows=max_rows, max_width=220)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Query the Bronze layer with DuckDB")
    parser.add_argument("sql", nargs="?", help="SQL to run; omit for an interactive prompt")
    parser.add_argument("--init-sql", action="store_true", help="print the view definitions and exit")
    parser.add_argument("--s3-endpoint", help="override the S3 endpoint (e.g. localhost:9000 from the host)")
    parser.add_argument("--max-rows", type=int, default=40)
    args = parser.parse_args(argv)

    settings = load_settings()
    storage = storage_from_settings(settings.storage)

    if args.init_sql:
        print(";\n".join(build_init_sql(settings, storage, endpoint_override=args.s3_endpoint)) + ";")
        return 0

    con = connect(settings, storage=storage, endpoint_override=args.s3_endpoint)
    if args.sql:
        _run(con, args.sql, args.max_rows)
        return 0

    print("Bronze DuckDB shell. Views: " + ", ".join(
        [f"bronze_{s}" for s in SOURCES]
        + ["bronze_all", "bronze_manifests", "ingestion_metrics", "ingestion_runs", "ingestion_state"]
    ))
    print("End statements with ';'. Ctrl-D to exit.")
    buffer: list[str] = []
    while True:
        try:
            line = input("bronze> " if not buffer else "   ...> ")
        except EOFError:
            print()
            return 0
        buffer.append(line)
        if line.rstrip().endswith(";"):
            sql = "\n".join(buffer)
            buffer.clear()
            try:
                _run(con, sql, args.max_rows)
            except Exception as exc:  # interactive shell: report and continue
                print(f"Error: {exc}", file=sys.stderr)


if __name__ == "__main__":
    raise SystemExit(main())
