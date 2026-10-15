# DECISIONS.md — Open Library Pipeline

> Draft. Edit anything marked **TODO** or that doesn't match what you actually build.
> Rewrite the "why" in your own words; you'll be asked to defend it.

## The question

By the end of this, I'll be able to answer:

1. **Which subjects have added the most works per decade of first publication, from the 1900s through the 2020s, and which are growing fastest?**
2. **Among the most prominent works in each subject, do works with more editions earn higher ratings and more want-to-read interest, and how does that differ by subject?**

…about **books in the Open Library catalog**.

**What each question runs on, and its limits**

- **Question 1** uses true catalog counts (`numFound` per subject and decade), not the sampled pages, so it isn't affected by relevance ranking. Works first published before 1900 are excluded by design. The 2020s are a partial decade (through 2026), so I compare it on a per-year basis or flag it as incomplete rather than treating it as a full decade.
- **Question 2** uses a relevance-ranked sample (up to ~1,500 works per subject), so it describes *prominent* works, not the whole catalog. Ratings are only compared above a minimum-ratings threshold, since averages from a handful of ratings are unreliable.

---

## Architecture at a glance

```
Open Library      RustFS (S3)         DuckDB                dbt                 Streamlit
search.json  ──►  raw JSON pages ──►  bronze (as-is)  ──►   silver ──► gold ──► 2+ views
(per subject)     date-keyed          warehouse.duckdb      + tests             read-only serving copy
                  └────────────── Prefect: scheduled · retried · logged ──────────────┘
```

---

## 1. Source — Open Library Search API

- **What:** `https://openlibrary.org/search.json`, one query per subject (`q=subject:<name>`), paginated with `limit=100` and a `page` loop that stops on an empty page.
- **Sample size:** ~15–20 subjects, capped at ~1,000–2,000 works each (for question 2). **TODO:** record final subject list and row count.
- **Second landing, for question 1:** one small request per subject and decade (`first_publish_year:[1990 TO 1999]`, `limit=1`) landed under `openlibrary_counts/`. Only `numFound` is used, which reflects the whole catalog instead of the ranked sample. **TODO:** record the result of the one-subject range-filter check.
- **Terms / etiquette:** The API is intended for low-volume use, not bulk access. Requests send a `User-Agent` with a contact email, which earns a higher rate limit than anonymous requests. **TODO:** confirm current limits and terms at https://openlibrary.org/developers/api and link them here.
- **Update frequency:** Community-edited catalog; changes slowly. **TODO:** note observed day-to-day change after a few runs.
- **Why this source:** Real pagination and rate-limit handling, nested array fields that need real silver work, and a year column plus subject for trend and group-by analysis.

## 2. Raw landing — RustFS (S3-compatible)

- **Layout:** `raw/openlibrary/subject=<name>/<YYYY-MM-DD>/page-<NNN>.json`, stored byte-for-byte as returned.
- **Why:** Date-keyed objects mean a scheduled run never overwrites a previous landing, and a retry rewrites the same key, so the step is safe to run twice. Bronze and everything downstream can be rebuilt from raw without hitting the API again.
- **Tradeoff:** One more service to run (Docker). A plain durable folder would be simpler but wouldn't practice the S3 interface. **TODO:** decide if you're keeping RustFS.

## 3. Analytical store — DuckDB

- **Why:** Analytical, columnar scans on a dataset that fits on one machine, with zero servers to manage. It can read JSON directly into bronze.
- **Tradeoff:** Single writer. Streamlit holding the file open would block `dbt build`, so the flow ends with a `publish` task that copies gold into a separate read-only serving file that Streamlit reads.
- **Alternative considered:** Postgres, which handles concurrent clients and removes the publish step, but adds a service and credentials to manage. **TODO:** one sentence on why you ruled it out.

## 4. Transform + orchestration — dbt + Prefect

**Layers**

| Layer | Models (draft) | Notes |
| --- | --- | --- |
| Bronze | `openlibrary_works`, `openlibrary_decade_counts` | Raw JSON loaded as-is, plus lineage columns (`_source_file`, `_queried_subject`, `_run_date`, `_loaded_at`) |
| Silver | `silver_works`, `silver_work_subjects`, `silver_work_authors` | Flatten arrays into bridge tables, normalize subject labels, cast types, handle missing ratings |
| Gold | `gold_works_by_subject_decade` (from decade counts, question 1), `gold_edition_popularity_by_subject` (from the works sample, question 2) | One table per question |

**Tests (need at least 4; draft has 6)**
- `unique` + `not_null` on `silver_works.work_key`
- `not_null` on `silver_work_subjects.subject`
- `relationships` from `silver_work_subjects.work_key` to `silver_works.work_key`
- `accepted_range` on `first_publish_year` (**TODO:** pick bounds after profiling)
- `accepted_range` on `ratings_average` (0–5)

**Flow:** `extract_and_land` (retries=3) → `load_bronze` → `dbt_build` → `publish`, run on a daily cron via `.serve()`.

- **Idempotency:** date-keyed S3 objects and `CREATE OR REPLACE` mean a retried or re-run step doesn't duplicate data.
- **Logging:** `get_run_logger()` records each landed key and row counts.
- **Deliberate failure test:** **TODO:** decide how you'll trigger one (for example a bad User-Agent or an unreachable host) so the run history shows a failed run.
- **Why:** Both are the course's reference tools, so I can spend time on the data instead of the tooling.

## 5. Serving — Streamlit

- **View 1:** Works per decade by subject (line or stacked bar), with a subject multiselect. Answers question 1.
- **View 2:** Edition count vs. ratings / want-to-read by subject (scatter or bar), with a minimum-ratings slider. Answers question 2.
- Both read from the **read-only serving copy**, never the working warehouse.
- **Why:** Code-first, easy to make interactive, and a simple read-only DuckDB connection.

---

## What I'd flag as risks

- Several fields (ratings, want-to-read counts) may be sparse. **TODO:** report null rates from a first sample.
- Subject labels are inconsistent, so question 1 depends on how well silver normalizes them.
- Daily runs may land nearly identical data. Plan: keep dated snapshots; stretch goal is tracking `want_to_read_count` over time.