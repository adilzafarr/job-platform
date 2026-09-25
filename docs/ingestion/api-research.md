# Job Source API Research

Research date: **2026-09-25**. Every claim below is either quoted from the
provider's own documentation or was **observed directly** with `curl` against
the live endpoint on that date (marked *observed*). Nothing here is inferred
from third-party wrappers.

| | Himalayas | Remote OK | Remotive | We Work Remotely |
|---|---|---|---|---|
| Read endpoint | `GET https://himalayas.app/jobs/api` | `GET https://remoteok.com/api` | `GET https://remotive.com/api/remote-jobs` | RSS: `GET https://weworkremotely.com/categories/<slug>.rss` |
| Format | JSON object | JSON array | JSON object | RSS 2.0 XML |
| Auth | None | None | None | None for RSS (the JSON API is a token-gated *posting* API) |
| Pagination | Cursor (`cursor` ← `nextCursor`), max `limit=20` | None (single document) | None (`limit` only truncates) | None (one document per feed) |
| Stable job ID | `guid` (job URL) | `id` (numeric string) | `id` (integer) | `guid` (job permalink) |
| Published timestamp | `pubDate` (unix seconds) | `epoch` (unix s) + `date` (ISO 8601) | `publication_date` (ISO 8601, no TZ) | `pubDate` (RFC 822) |
| Updated timestamp | None | None | None | None |
| Expiry | `expiryDate` (unix s) | None | None | `expires_at` (RFC 822) |
| Conditional requests | **Not honoured** (observed) | **No validators** (observed) | **`If-Modified-Since` → 304** (observed) | **ETag unusable** (observed) |
| Date filtering | No | No | No | No |
| Documented rate limit | "we ratelimit" → HTTP 429; "polling more than once per day provides no benefit" | Not documented | ">2x per minute will be blocked"; "max. 4 times a day" | Not documented for RSS |
| Chosen strategy | Resumable newest-first cursor sweep down to a watermark | Snapshot + content-hash change detection | Conditional GET + content hash | Per-feed snapshot + content-hash change detection |

---

## Himalayas

**Docs:** <https://himalayas.app/api>

- **Endpoints:** `GET /jobs/api` (browse, used here) and `GET /jobs/api/search`
  (free-text/filter search, page-numbered). No authentication and no API key.
- **Pagination:** cursor based. Quoting the response's own `comments` field
  (21/08/2026): *"Cursor pagination is now available and is the preferred way
  to page through the feed. Pass the nextCursor value from each response back
  as ?cursor=. It is faster than offset and will never return the same job
  twice. The offset parameter is deprecated."*
  - `limit` is capped at **20**. *Observed:* `limit=50` still returns 20.
  - Paging ends when a response has no `nextCursor`.
  - *Observed:* the cursor is base64 of `<pubDate ISO, µs precision>|<internal id>`,
    for example `2026-09-24T20:32:10.710499Z|2064823`. We treat it as opaque.
  - *Observed:* an invalid cursor returns `400 {"ok":false,"errors":"Invalid cursor."}`.
- **Response:** `{comments, updatedAt, offset, limit, totalCount, nextCursor, jobs[]}`.
  *Observed* job keys: `title, excerpt, companyName, companySlug, companyLogo,
  employmentType, minSalary, maxSalary, salaryPeriod, seniority, currency,
  locationRestrictions, timezoneRestrictions, categories, parentCategories,
  description, pubDate, expiryDate, applicationLink, guid`.
- **Ordering:** *observed* strictly newest-first by `pubDate` (unix seconds).
  Bulk imports produce **many jobs with the same second**. One observed run
  had 100 jobs inside a 12-second window.
- **Volume:** *observed* `totalCount` = **98,768**. With `expiryDate − pubDate`
  = 60 days, that is about 1,650 jobs a day, or about 80 pages a day. A full
  crawl would be about 5,000 requests, which is not acceptable per run.
- **Stable ID:** `guid`, documented as *"Unique identifier for the job"*
  (its value is the job's Himalayas URL).
- **Timestamps:** `pubDate` ("when the job was published") and `expiryDate`
  ("when the job will expire"). There is **no updated-at**.
- **Deletions/expiry:** `expiryDate` is known up front. Early removals cannot
  be detected by an incremental sweep.
- **Conditional requests:** the response carries `Last-Modified` and
  `Cache-Control: s-maxage=7200` from its CDN, but *observed*
  `If-Modified-Since` with the exact `Last-Modified` value still returns
  **200**. There is no `ETag`.
- **Rate limits:** *"Due to server capacity constraints, we ratelimit the
  number of requests"*, with 429 responses. No numeric limit is published.
  *"polling more than once per day provides no benefit."*
- **Terms:** attribution required (link back and name Himalayas as the
  source). Do not resubmit jobs to third-party boards (Jooble, Neuvoo, Google
  Jobs, LinkedIn Jobs).

**Chosen strategy: resumable, newest-first cursor sweep down to a watermark.**

1. State keeps `last_seen_published_at`, the high-water mark of the last
   *completed* sweep. On the first run it is initialised to
   `now − HIMALAYAS_INITIAL_LOOKBACK_HOURS` (default 48 h), so the pipeline
   never crawls the 98k-job history.
2. Each run pages from the newest job downward. It stops on the first page
   that contains a job with `pubDate < watermark − overlap`, or when there is
   no `nextCursor`. The default overlap of one hour guards against late or
   backdated inserts. Comparisons are strict because many jobs share a second.
3. If the run reaches `HIMALAYAS_MAX_PAGES` (default 200, which is 4,000 jobs)
   before the watermark, the next cursor and the sweep's newest `pubDate` are
   saved as `last_cursor` and `sweep_high_watermark`. The **next run resumes
   from that cursor** instead of starting over. Only when the sweep reaches
   the old watermark does `last_seen_published_at` advance, so a backlog
   never leaves a gap.
4. If the saved cursor is rejected (`400 Invalid cursor`), the sweep restarts
   from the top with the old watermark. This is safe but uses more requests.
5. Requests are paced 1 s apart.

Limitation: there is no `updatedAt`, so **edits to already-ingested jobs are
not re-fetched**. Neither are jobs inserted with a `pubDate` older than the
overlap window.

---

## Remote OK

**Docs:** the endpoint itself is the documentation. Its first array element
is a terms notice. <https://remoteok.com/api>

- **Endpoint:** `GET https://remoteok.com/api`. No authentication. There are
  no documented query parameters and no pagination.
- **Response (*observed*):** a JSON **array**. Element 0 is
  `{"last_updated": <unix s>, "legal": "API Terms of Service: …"}`. The rest
  are jobs, **99 of them**, spanning roughly 55 days
  (2026-07-31 → 2026-09-24): the newest jobs only.
- **Job keys (*observed*):** `slug, id, epoch, date, company, company_logo,
  position, tags, description, location, apply_url, salary_min, salary_max,
  logo, url`, and sometimes `original` and `verified`.
- **Stable ID:** `id`, a numeric string such as `"1137429"`.
- **Timestamps:** `epoch` (unix seconds) and `date` (ISO 8601 with offset).
  There is no updated or expiry timestamp.
- **Deletions:** a job disappearing from the feed does not mean it was
  deleted. It may just have been pushed out of the latest-100 window.
- **Conditional requests:** *observed* **neither `ETag` nor `Last-Modified`**.
- **Content stability:** *observed* two fetches minutes apart returned
  **byte-identical bodies**, so a SHA-256 of the body is a reliable change
  detector.
- **Rate limits:** none published. We send a descriptive `User-Agent`
  (*observed:* not strictly required today).
- **Terms (quoted):** *"Please link back (with follow, and without nofollow!)
  to the URL on Remote OK and mention Remote OK as a source … If you do not
  we'll have to suspend API access. Please don't use the Remote OK logo
  without written permission."*

**Chosen strategy: one request per run, with content-hash change detection.**
The request cannot be avoided because the API offers no validators. If the
body's SHA-256 equals the last committed hash, the run is recorded as
`not_modified` and nothing is written to Bronze. Otherwise the full snapshot
is written. At about 2 new jobs a day, a daily poll cannot lose jobs off the
end of the 99-job window.

---

## Remotive

**Docs:** <https://github.com/remotive-com/remote-jobs-api> (official repo).
`https://remotive.com/api-documentation` returns 403 to automated fetchers.

- **Endpoint:** `GET https://remotive.com/api/remote-jobs`. Optional `category`,
  `company_name`, `search`, `limit`. No authentication.
- **Pagination:** none. `limit` only truncates. Without it, the endpoint
  returns everything it exposes.
- **Response (*observed*):** `{"00-warning", "0-legal-notice", "job-count",
  "total-job-count", "jobs": [...]}`. Only **19 jobs** were exposed.
- **Job keys (*observed*):** `id, url, title, company_name, company_logo,
  company_logo_url, category, tags, job_type, publication_date,
  candidate_required_location, salary, description`.
- **Stable ID:** `id` (integer).
- **Timestamps:** `publication_date` is ISO 8601 **without a time zone**, for
  example `2026-09-21T12:55:11`. There is no updated or expiry field.
- **Deletions:** the response is the full public list, so disappearance
  between snapshots means the job was removed from the public feed.
- **Conditional requests:** the response has `Last-Modified`. *Observed:*
  sending it back as `If-Modified-Since` returns **`304 Not Modified`** with
  an empty body. No `ETag`.
- **Rate limits (quoted):** *"we advise max. 4 times a day"*; *"Excessive
  requests (more than 2x per minute) will be blocked."*
- **Terms (quoted):** *"Jobs displayed are delayed by 24 hours"*; *"Please
  link back to the URL found on Remotive AND mention Remotive as a source"*;
  do not submit jobs to Jooble, Neuvoo, Google Jobs or LinkedIn Jobs.

**Chosen strategy: conditional GET, with a content hash as backup.** The
stored `Last-Modified` is sent as `If-Modified-Since`. A `304` costs no body
and is recorded as `not_modified`. A `200` whose body hash matches the last
commit is also treated as `not_modified`. Otherwise the full snapshot is
written.

---

## We Work Remotely

**Docs:** <https://weworkremotely.com/api> and <https://weworkremotely.com/remote-job-rss-feed>

- **The JSON API is not a read API.** `https://weworkremotely.com/api/v1/remote-jobs`
  is a *partner posting* API. Quoting: *"Before using this API endpoint, you
  need a special token. Please reach out to … if you would like to partner
  with WWR and post jobs via our API."* Its `GET /remote-jobs/:id` returns a
  partner's *own* listing. It cannot list the board. (It documents 1,000
  requests a day per token and ETag/Last-Modified support, but none of that
  applies to us.)
- **Public read mechanism: RSS feeds.** No authentication. There is one main
  feed and 11 category feeds, listed on the RSS page:
  `all-other-remote-jobs, remote-back-end-programming-jobs,
  remote-customer-support-jobs, remote-design-jobs, remote-devops-sysadmin-jobs,
  remote-front-end-programming-jobs, remote-full-stack-programming-jobs,
  remote-management-and-finance-jobs, remote-product-jobs,
  remote-programming-jobs, remote-sales-and-marketing-jobs`.
- **Coverage (*observed*):**
  - The **main feed returns only ~10 items per category** (84 total), so it
    is a sample, not the board.
  - The 11 category feeds held **260 unique jobs**.
  - Every job in the main feed and in `remote-programming-jobs` also appears
    in a leaf category feed. `remote-programming-jobs` is exactly the union
    of full-stack, back-end and front-end.
  - `remote-front-end-programming-jobs` was a **valid, empty feed** (0 items).
- **Item elements (*observed*):** `title` (formatted `"<Company>: <Title>"`),
  `region, country, state, skills, category, type, description, pubDate,
  expires_at, guid, link`, and sometimes `media:content` with attributes.
  The channel has `<ttl>60</ttl>`.
- **Stable ID:** `guid`, which equals the job permalink
  (`https://weworkremotely.com/remote-jobs/<slug>`). It is the only identity
  WWR publishes. Fallback if it were ever absent: `link`. A slug may change if
  a posting is re-titled. That is a known limitation.
- **Timestamps:** `pubDate` and `expires_at` (RFC 822). No updated timestamp.
  Feeds include some very old pinned items (2023).
- **Conditional requests:** the responses carry an `ETag`, but *observed*
  **the ETag changes on every request even when the body is byte-identical**,
  and `If-None-Match` returns **200**. It cannot be used. There is no
  `Last-Modified`.
- **Latency:** *observed* some feeds take 20–40 s to respond, so the read
  timeout is set to 90 s.
- **Rate limits:** none published for RSS. We fetch 10 feeds, 2 s apart.

**Chosen strategy: per-feed snapshot with content-hash change detection.**
The 10 leaf category feeds are fetched. The main feed and the programming
aggregate are skipped because they are redundant; the list is configurable
through `WWR_FEEDS`. The SHA-256 of each feed body is stored in state:
- If **no** feed changed, the run is `not_modified` and nothing is written.
- If **any** feed changed, all feeds from that run are written, so each WWR
  Bronze partition is a **complete snapshot** that Silver can diff to detect
  removals.

An empty individual feed is valid. All feeds empty at once is treated as a
failure.

---

## Cross-cutting decisions

- **User-Agent:** `JOB_INGESTION_USER_AGENT` (configurable). It identifies the
  project and a contact address.
- **Schedule:** daily at 06:00 UTC by default (`JOB_INGESTION_SCHEDULE`). This
  is inside every provider's guidance: Himalayas says more than daily has no
  benefit; Remotive allows at most 4 a day and delays jobs 24 h.
- **No provider supports server-side date filtering or an updated-since
  query.** No incremental capability beyond those listed above was assumed.
