# Job Ingestion: Airflow → Bronze

This phase fetches raw job postings from **Himalayas, Remote OK, Remotive and
We Work Remotely**, stores them unmodified in a Bronze layer on MinIO/S3, and
makes them queryable with DuckDB. Silver/Gold, dbt, FastAPI and the frontend
are out of scope.

- API research and the per-provider strategy: [api-research.md](api-research.md)
- Code: `airflow/dags/job_ingestion.py` (orchestration only) and `airflow/include/` (logic)

```
Public job APIs ─► Airflow (job_ingestion) ─► source extractors ─► MinIO bucket
                                                                  ├─ bronze/…    raw + Parquet
                                                                  └─ ingestion/… state, metrics
                                                         DuckDB ◄─┘ (views over committed Parquet)
```

---

## 1. Running it locally

Requires Docker with at least 4 GB of memory. Every value has a local
default, so `.env` is optional: `cp .env.example .env` to override one (for
example `JOB_INGESTION_USER_AGENT` with your contact address).

| Task | Command (`make` target in brackets) |
|---|---|
| Build and start | `docker compose up -d --build` (`make up`) |
| Status | `docker compose ps` |
| Airflow UI | <http://localhost:8080>, login `airflow` / `airflow` |
| MinIO console | <http://localhost:9001>, login `minioadmin` / `minioadmin-local` |
| Trigger manually | UI ▶ *Trigger*, or `docker compose exec airflow-scheduler airflow dags trigger job_ingestion` (`make trigger`) |
| Full-refresh run | `docker compose exec airflow-scheduler airflow dags trigger job_ingestion -c '{"full_refresh": true}'` |
| List runs | `docker compose exec airflow-scheduler airflow dags list-runs job_ingestion` |
| Task states for a run | `docker compose exec airflow-scheduler airflow tasks states-for-dag-run job_ingestion <run_id>` |
| Scheduler logs | `docker compose logs -f airflow-scheduler` |
| Stop (data kept) | `docker compose down`; add `-v` to delete all data |
| Unit tests | `docker compose --profile test run --rm --build airflow-tests` (`make test`) |
| Live smoke test | `RUN_LIVE_TESTS=1 docker compose --profile test run --rm airflow-tests -m live` |

**Retrying one provider:** in the UI, open the failed run, select the failed
task(s) of that source group, and use *Clear* with *Downstream* checked,
together with `record_run_metrics` and `end`. The other sources' committed
data is untouched. Clearing `validate` alone re-checks the archived responses
**without calling the API** again. That is useful after fixing a parser. Over
REST, `POST /api/v2/dags/job_ingestion/clearTaskInstances` with
`"dag_run_id"`, `"task_ids"` and **`"only_failed": false`**. That flag
defaults to true, so successful tasks would otherwise be skipped silently.

**Task logs** are in the UI: *DAG → Grid → select a task instance → Logs*.
The files are in the `airflow-logs` volume under
`/opt/airflow/logs/dag_id=job_ingestion/run_id=…/task_id=<source>.<stage>/attempt=N.log`:

```bash
docker compose exec airflow-scheduler bash -c 'ls -R /opt/airflow/logs/dag_id=job_ingestion | head -50'
```

**Scheduling:** `JOB_INGESTION_SCHEDULE` is a cron expression in UTC. The
default is `0 6 * * *` (daily). Set it to `none` for manual-only runs.
The DAG is created **unpaused** (`JOB_INGESTION_PAUSED_ON_CREATION=false`), so
the first scheduled interval runs right after startup.

**Catchup is off.** An API extractor always fetches "now"; replaying past
intervals would only resend the same requests many times. With
`catchup=False`, Airflow creates at most one run (the latest interval) when
the environment starts, even after long downtime. Missed time is covered
anyway: Himalayas' watermark and the snapshot feeds' "latest state" semantics
both catch up in the next run. `max_active_runs=1` keeps runs from
overlapping, which also makes the per-source state single-writer.

---

## 2. DAG design

```
start ─┬─ himalayas      : extract → validate → load_bronze → commit_state ─┐
       ├─ remoteok       : extract → validate → load_bronze → commit_state ─┤
       ├─ remotive       : extract → validate → load_bronze → commit_state ─┼─ record_run_metrics ─ end
       └─ weworkremotely : extract → validate → load_bronze → commit_state ─┘
```

| Task | Does | Retries |
|---|---|---|
| `extract` | Loads state, calls the API, archives raw responses, writes the hand-off record | 2, from 5 min, exponential |
| `validate` | Re-parses the **archived** responses and runs quality checks | 1 (data doesn't change) |
| `load_bronze` | Writes `records.parquet`, reads it back, writes `_manifest.json` (the commit) | 2, from 1 min |
| `commit_state` | Advances incremental state from the manifest | 2, from 1 min |
| `record_run_metrics` | `all_done`: writes the run summary and logs a per-source table | 1 |
| `end` | `all_done`: **fails the DAG run** if any source failed, naming the sources | 0 |

- Each source is its own TaskGroup, so failures are visible and retryable per
  provider. A failed Remotive never blocks or rolls back Himalayas.
- The DAG file only wires tasks together. Each task calls a method of
  `include.pipeline.IngestionPipeline`, which has no Airflow imports and is
  unit-tested directly.
- **Retry layering:** the HTTP client makes up to 3 quick attempts (2 s, 4 s
  backoff with jitter, honouring `Retry-After` up to 120 s). Airflow retries
  on top of that are few and minutes apart. Permanent errors (400/401/403/404,
  validation failures) raise `AirflowFailException`, which skips Airflow
  retries.
- **Every task returns a small JSON value (XCom)**: request count, records,
  bytes, `bronze_uri`, `stop_reason`, and so on. Open a task → *XCom* in the
  UI to see where its Bronze output went. The UI also shows try numbers,
  durations and logs natively.

---

## 3. Bronze storage

### Format decision

| Option | Verdict |
|---|---|
| Parquet only | Queryable, but loses the exact provider bytes (XML, key order, envelopes such as Remote OK's legal notice) |
| Raw JSON/XML only | Exact, but every question needs custom parsing |
| **Both, per run: `responses.jsonl.gz` + `records.parquet`** | **Chosen.** Exact archive for replay and debugging, plus a columnar file for queries |

No warehouse or query server is involved. DuckDB reads Parquet straight from
MinIO. Moving to AWS S3 is a configuration change: unset
`LAKE_S3_ENDPOINT_URL`, provide IAM credentials, and change the bucket.

### Layout

```
s3://job-platform-lake/
  bronze/<source>/ingestion_date=YYYY-MM-DD/run_id=<airflow run id, sanitised>/
      responses.jsonl.gz   one JSON line per HTTP request: url, headers, status, fetched_at,
                           body_sha256 and the exact body (UTF-8 text, or base64)
      records.parquet      one row per provider record
      _manifest.json       commit marker: files, row count, checksums, state to commit
  ingestion/state/<source>.json                                      incremental state
  ingestion/extractions/<source>/ingestion_date=…/run_id=….json      extract → validate/load hand-off
  ingestion/metrics/<source>/ingestion_date=…/run_id=….json          per-source run metrics
  ingestion/runs/ingestion_date=…/run_id=….json                      DAG-run summary
```

- **Partitioning:** by source, then UTC ingestion date (the date of the DAG
  run's `run_after`, stable across retries), then run. That supports
  one-provider queries, date-range pruning, identifying a run, and later
  incremental Silver loads.
- **File count:** one Parquet file and one archive per source per run, not
  per page. Himalayas' ~200 pages still become 2 files, and daily runs produce
  about 8 data files a day. Runs where the source is unchanged
  (`not_modified`) write **no** Bronze files.

### Records and metadata

Provider fields keep their **original names and JSON types**: Remote OK's
`position`/`epoch`/`tags`, Remotive's `title`/`company_name`/`publication_date`,
Himalayas' `companyName`/`pubDate`, WWR's `pubDate`/`expires_at`/`media:content`.
Nothing is renamed or normalised. New provider fields appear as new columns.
If a field's values can't share one type, that column is stored as JSON text
rather than coerced.

Ingestion metadata columns all start with `_`:

| Column | Meaning |
|---|---|
| `_source` | `himalayas` / `remoteok` / `remotive` / `weworkremotely` |
| `_source_job_id` | Provider's own ID as text: Himalayas `guid`, Remote OK `id`, Remotive `id`, WWR `guid` (falls back to `link`) |
| `_pipeline_run_id` | Airflow `run_id`, unmodified |
| `_ingestion_date` | Partition date (DATE) |
| `_extracted_at` | When the response carrying this record was received (TIMESTAMPTZ) |
| `_request_seq` / `_request_url` | Which request in the run produced the record |
| `_request_context` | JSON: page number, Himalayas cursor, WWR feed name, conditional headers |
| `_record_index` | Position within that response |
| `_record_sha256` | Hash of the record's canonical JSON, for change detection in Silver |

No IDs are generated. All four providers expose a stable ID. If one were ever
missing, `_source_job_id` would be NULL and validation would fail the run
once more than 5% of records lacked it.

### Idempotency

1. **Deterministic keys.** All objects for (source, run) live at fixed
   paths, so a retry overwrites instead of adding files.
2. **Clean start.** `extract` deletes any *uncommitted* files under its
   partition before fetching, which covers torn writes from a crashed attempt.
3. **Commit marker.** `load_bronze` writes Parquet, reads it back (checksum
   and row count), and only then writes `_manifest.json`. S3 PUTs are atomic,
   so a file is either complete or absent.
4. **Committed means final.** Once a manifest exists, every stage for that
   (source, run) is a no-op. Clearing a successful task never re-calls the API
   or replaces data with a smaller incremental fetch.
5. **State after data.** Incremental state changes only in `commit_state`,
   only from a manifest (or a `not_modified` extraction), and only if the state
   `version` still equals the version the extraction started from:
   - A crash between the manifest and the state write is **rolled forward**
     on retry.
   - A late retry of an old run is `stale_skipped`; it can't regress state
     that a newer run advanced.
6. **Queries see committed data only.** The DuckDB views list only Parquet
   files whose partition has a manifest.

Bronze keeps history. The same job fetched on two days appears twice, which is
intended. Only accidental duplication (retries, reruns, rewritten pages) is
prevented. Cross-provider deduplication is a Silver concern.

### Validation, empty responses and failures

| Situation | Handling |
|---|---|
| HTTP 429 | Backoff honouring `Retry-After`. If it exceeds 120 s, defer to an Airflow retry |
| HTTP 5xx, timeout, connection error | 3 in-task attempts, then Airflow retries |
| HTTP 400/401/403/404 | Fail immediately, no retries (configuration or request problem) |
| HTML page served with 200 (block or challenge) | `ResponseFormatError` (content type checked before parsing) |
| Invalid JSON/XML | `ResponseFormatError`. The raw body stays in the uncommitted archive for debugging |
| Parsed but wrong shape (no `jobs` list, not an array, not RSS) | `SchemaError` |
| Structurally valid but zero records | Himalayas first page, Remote OK, Remotive, or **all** WWR feeds: `EmptyResponseError` (fails). A single empty WWR feed only warns |
| Nothing new | `not_modified` (304 or unchanged hash): success, no Bronze write |
| Identity or required fields missing on more than 5% of records | `ValidationError`: no retry, state untouched |
| Duplicate IDs within one run | Warning and metric. Kept as received |

Validation deliberately doesn't check canonical fields (salary, location,
and so on). Unknown provider fields are always accepted.

### Metrics

`ingestion/metrics/<source>/…/run_id=….json` is updated by every stage and by
the failure and retry callbacks. It holds: `status`,
`started_at`/`completed_at`/`duration_seconds`, `requests_made`,
`http_retries`, `http_status_counts`, `pages_fetched`, `bytes_received`,
`records_received`, `records_written`, `bytes_written`, `new_records` and
`previously_seen_records` (distinct IDs never or already seen in earlier
committed runs of the source), `not_modified_reason`, `warnings`, the
`validation` report, `error {stage, type, message, retryable, try_number}`,
and `attempts[]` (every stage attempt, which shows retries).
`record_run_metrics` rolls these up into `ingestion/runs/…`.

---

## 4. Querying Bronze

DuckDB reads the lake directly. The helper module builds these views:

| View | Contents |
|---|---|
| `bronze_himalayas`, `bronze_remoteok`, `bronze_remotive`, `bronze_weworkremotely` | Provider records (original fields) plus `_` metadata |
| `bronze_all` | Metadata columns of all four sources |
| `bronze_manifests` | One row per committed partition |
| `ingestion_metrics` | One row per source per run |
| `ingestion_runs` | One row per DAG run |
| `ingestion_state` | Current state per source |

**One-off query** (runs inside the scheduler container, which already has
credentials):

```bash
docker compose exec airflow-scheduler python -m include.bronze_query "SELECT _source, count(*) FROM bronze_all GROUP BY 1"
# make bronze-sql Q="SELECT count(*) FROM bronze_remoteok"
```

**Interactive prompt** (end statements with `;`):

```bash
docker compose exec -it airflow-scheduler python -m include.bronze_query
```

**Web UI (DuckDB UI).** This is a browser SQL notebook with autocomplete, a
schema browser and result grids. It runs locally, needs no extra containers,
and reads Parquet straight from MinIO on `localhost:9000`. Install the CLI
once with `winget install DuckDB.cli`, then:

```powershell
./scripts/bronze-ui.ps1          # opens http://localhost:4213 with all views loaded
```

(`make bronze-ui` does the same from a bash shell.) Keep the terminal open
while you use the UI, and type `.exit` there to stop it. The script
regenerates `.duckdb/bronze_init.sql` on every launch, so each launch
includes the newest committed runs. That file contains the MinIO credentials
and is git-ignored. The UI's page code is loaded from DuckDB's website; your
queries and data stay on your machine. Notebooks are saved in `~/.duckdb`.

**Other clients.** `./scripts/bronze-ui.ps1 -NoLaunch` only writes
`.duckdb/bronze_init.sql`:
- DuckDB CLI: `duckdb -init .duckdb/bronze_init.sql`
- DBeaver: create a DuckDB connection to an in-memory database and run that
  file once.

The views are resolved when you connect, so reconnect to pick up newer runs.
Raw S3 globs also work if you create the secret yourself:
`SELECT * FROM read_parquet('s3://job-platform-lake/bronze/remoteok/*/*/records.parquet')`.
Those globs also include uncommitted files, though.

### Example queries

```sql
-- Records per source
SELECT _source, count(*) AS records, count(DISTINCT _source_job_id) AS distinct_jobs
FROM bronze_all GROUP BY 1 ORDER BY 1;

-- How many records came from Remote OK?
SELECT count(*) FROM bronze_remoteok;

-- Records by source and ingestion date
SELECT _source, _ingestion_date, count(*) FROM bronze_all GROUP BY ALL ORDER BY 2 DESC, 1;

-- What did yesterday's ingestion retrieve?
SELECT _source, count(*) FROM bronze_all
WHERE _ingestion_date = current_date - 1 GROUP BY 1;

-- Raw Remotive jobs, with provider field names
SELECT id, title, company_name, category, publication_date, url FROM bronze_remotive LIMIT 20;

-- Remote OK / Himalayas / WWR in their own shapes
SELECT id, position, company, epoch, tags FROM bronze_remoteok LIMIT 20;
SELECT guid, title, companyName, to_timestamp(pubDate) AS published FROM bronze_himalayas LIMIT 20;
SELECT guid, title, region, category, pubDate, _request_context->>'feed' AS feed FROM bronze_weworkremotely LIMIT 20;

-- Which source_job_ids were retrieved in a particular run?
SELECT _source, _source_job_id FROM bronze_all
WHERE _pipeline_run_id = 'manual__2026-09-25T06:30:00+00:00' ORDER BY 1, 2;

-- Run history per source (metrics)
SELECT source, pipeline_run_id, status, requests_made, records_received, records_written,
       new_records, previously_seen_records, duration_seconds, not_modified_reason
FROM ingestion_metrics ORDER BY updated_at DESC;

-- Current incremental state
SELECT source, status, version, last_successful_run_id, incremental FROM ingestion_state;

-- Where each committed partition lives
SELECT source, ingestion_date, pipeline_run_id, records_written, files.records.key FROM bronze_manifests;
```

---

## 5. Configuration reference

All settings are environment variables. See `.env.example` and
`airflow/include/config.py`. The only secrets are the MinIO/S3 credentials
(`MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD`, mapped to
`LAKE_S3_ACCESS_KEY_ID` / `LAKE_S3_SECRET_ACCESS_KEY`). None of the four
providers needs an API key. `.env` is git-ignored.

Local MinIO uses the `chainguard/minio` image, because `minio/minio` is no
longer published. Set `MINIO_IMAGE` to use another S3-compatible image. For
local development it runs as root so the fresh named volume is writable.

## 6. Known limitations

- **Himalayas:** there is no `updatedAt`, so edits to already-ingested jobs
  are not re-fetched. Jobs inserted with a `pubDate` older than the one-hour
  overlap are missed. The first run looks back 48 hours, not across the
  ~98k-job history.
- **Remote OK** exposes only its ~100 newest jobs, and **Remotive** only a
  small, 24-hour-delayed public subset. That is a provider limit, not a
  pipeline one.
- **WWR** RSS has no pagination. The feeds show what WWR chooses to list
  (about 260 jobs across categories when checked).
- **Remote OK** sends some titles double-encoded: the raw body contains
  `MecÃ¡nico`, which decodes to "MecÃ¡nico". Bronze preserves this
  as received. Repairing it is a Silver concern.
- `ingestion_date` is the UTC date of the run's `run_after`. For a scheduled
  run that is the end of its data interval (for example, the 2026-09-25 02:12
  startup run belonged to the interval ending 2026-09-24 06:00). For a manual
  run it is the trigger time.
- Himalayas' one-hour overlap means every incremental run re-reads about one
  page of already-seen jobs (`previously_seen_records` in the metrics).
- Views list files at connect time, so reconnect to see new runs.
- A worker killed hard during the *final* retry of `load_bronze` can leave an
  uncommitted `records.parquet`. It is invisible to the views and is removed
  the next time that task group is cleared.
