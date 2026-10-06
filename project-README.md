# Final Project — One Pipeline, Yours

Six weeks you've been handed the data, the bucket, the models, and the flow.
This time you pick all of it. Same parts, your subject: **go get data, land it
raw, warehouse it, model it in layers, orchestrate it, and put it in front of a
human.** Then stand up and tell us what it says.

```
 ┌──────────────┐    ┌──────────────┐    ┌────────────────┐    ┌──────────────┐
 │ YOUR SOURCE  │    │ RAW LANDING  │    │ ANALYTICAL     │    │ SERVING      │
 │ files, API,  │───►│ original,    │───►│ bronze → silver│───►│ dashboard,   │
 │ database, ...│    │ replayable   │    │ → gold + tests │    │ app, report  │
 └──────────────┘    └──────────────┘    └────────────────┘    └──────────────┘
        │                   │                    │                    │
        └───────────────────┴────────────────────┴────────────────────┘
                            orchestrated · scheduled · retried
                            logged · observable
```

Nothing here is new. Every box is a capability you already built. The work is
**wiring your own version end to end** and **defending the choices**.

### The architecture is a suggestion, not a constraint

RustFS, DuckDB/Postgres, dbt, Prefect, and Metabase/Streamlit form a known-good
reference architecture because you have already used them. You may keep that
stack, swap out one component, or design a different architecture. Your choices
are valid when the finished pipeline still meets the requirements below and you
can explain the tradeoffs in `DECISIONS.md`.

The boxes matter more than the product names: preserve the raw source, create
trustworthy analytical layers, automate and observe the run, test the data, and
put useful results in front of a human.

---

## The five design decisions

Write these down. They are the first slide of your deck and the first section of
your `DECISIONS.md`. The options are examples, not an exhaustive list.

| # | Choice | Options | Notes |
| --- | --- | --- | --- |
| 1 | **Source** | Files, an HTTP API, a database, or another documented source | Choose data that can answer worthwhile questions. |
| 2 | **Raw landing** | RustFS/S3, durable filesystem, or another replayable store | Preserve the source as it arrived. |
| 3 | **Analytical store** | Postgres, DuckDB, or an equivalent | Pick one and be able to say why. |
| 4 | **Transform + orchestration** | dbt + Prefect, or equivalent tools | Produce tested bronze → silver → gold outcomes in an observable run. |
| 5 | **Serving** | Metabase, Streamlit, or an equivalent | At least **2 views/questions** that answer a real question. |

**The question comes first.** Before you write a line of code, finish this
sentence: *"By the end of this, I'll be able to answer ____ and ____ about
____."* If your data can't answer two interesting questions, pick different data
before building the rest of the pipeline.

---

## Requirements checklist

- [ ] A source you chose, documented (URL, license/terms, how often it updates)
- [ ] Raw data landed unmodified in durable storage — the source as it arrived,
      retained so the pipeline can replay it
- [ ] Raw data loaded **as-is** into an analytical store (bronze)
- [ ] **Bronze → silver → gold** transformations with visible dependencies and lineage
- [ ] At least **4 automated data-quality checks** that pass (for example:
      `not_null`, `unique`, accepted values, and relationships)
- [ ] An orchestrated workflow that runs the whole pipeline: extract → land →
      load → transform/test → publish
- [ ] At least one retry on a failure-prone operation and structured run logging
- [ ] A scheduled deployment with successful runs visible in its run history or UI
- [ ] **2+ views** in a dashboard, application, report, or equivalent, built on gold outputs
- [ ] `README.md` — how to run your project from zero
- [ ] `DECISIONS.md` — the five choices, defended
- [ ] **Slide deck** (below)

---

## Section refreshers

Each of these is a capability you already built. The examples use the course's
reference stack; adapt or replace them if your architecture provides the same
outcome. Skim the refresher, reuse your own old repo, and use the docs links
when you're stuck.

### 1 — Get the data (Week 1 / Week 3)

**If you chose CSVs:** download them by hand once, into `data/source/`. Don't
clean them, don't open them in Excel and re-save. Messy is the point — that's
what silver is for.

**If you chose an API:** the three realities from Week 3 still apply.

- **Auth** — token in a `.env`, read via `os.getenv`, never committed.
- **Pagination** — loop until the API says there's no next page. Don't hardcode
  a page count.
- **Rate limits** — a `429` is not a crash, it's a *wait*. Back off and retry.

```python
import httpx
from tenacity import retry, stop_after_attempt, wait_exponential

@retry(stop=stop_after_attempt(5), wait=wait_exponential(min=1, max=30))
def fetch_page(url: str, params: dict) -> dict:
    resp = httpx.get(url, params=params, timeout=30)
    resp.raise_for_status()          # 429/5xx -> raises -> tenacity retries
    return resp.json()
```

**Where to find data**

| Source | Link |
| --- | --- |
| data.gov | https://data.gov |
| Nashville Open Data | https://data.nashville.gov |
| Kaggle Datasets | https://www.kaggle.com/datasets |
| NYC Open Data | https://opendata.cityofnewyork.us |
| Public APIs list | https://github.com/public-apis/public-apis |
| USGS Earthquakes (API) | https://earthquake.usgs.gov/fdsnws/event/1/ |
| NWS Weather (API, no key) | https://www.weather.gov/documentation/services-web-api |
| Open-Meteo (API, no key) | https://open-meteo.com/en/docs |
| FBI Crime Data (API) | https://cde.ucr.cjis.gov/LATEST/webapp/#/pages/docApi |
| Spotify / GitHub / OpenLibrary | https://developer.spotify.com/documentation/web-api · https://docs.github.com/rest · https://openlibrary.org/developers/api |

**Docs:** [httpx](https://www.python-httpx.org/) ·
[tenacity](https://tenacity.readthedocs.io/) ·
[python-dotenv](https://pypi.org/project/python-dotenv/) ·
[requests](https://requests.readthedocs.io/) (if you prefer it)

> **Sanity check:** aim for something with real *shape* — at least a few
> thousand rows, a date column, and a category or ID you can group by. A 40-row
> CSV won't produce an interesting gold table.

### 2 — Land it raw (reference option: RustFS)

The **land-raw principle**: whatever arrives, preserve it before you transform
it. If a transform is wrong later, you can replay from raw instead of re-hitting
the source. RustFS is the reference implementation from the course; another
durable, replayable raw store is equally valid.

```yaml
# docker-compose.yml
services:
  rustfs:
    image: rustfs/rustfs:1.0.0-beta.8
    container_name: final_rustfs
    ports:
      - "9000:9000"   # S3 API      -> boto3 / DuckDB point here
      - "9001:9001"   # web console -> browse your buckets
    environment:
      RUSTFS_ACCESS_KEY: rustfsadmin
      RUSTFS_SECRET_KEY: rustfsadmin
      RUSTFS_CONSOLE_ENABLE: "true"
    volumes:
      - rustfs_final_data:/data
    command: ["/data"]
    restart: unless-stopped

volumes:
  rustfs_final_data:
```

```python
import boto3

s3 = boto3.client(
    "s3",
    endpoint_url="http://localhost:9000",
    aws_access_key_id="rustfsadmin",
    aws_secret_access_key="rustfsadmin",
    region_name="us-east-1",
)
s3.create_bucket(Bucket="raw")
s3.put_object(Bucket="raw", Key="earthquakes/2026-07-22.json", Body=raw_bytes)
```

**Key your objects by date** (`source/YYYY-MM-DD.json`) so a scheduled run
doesn't stomp yesterday's landing. That's what makes a re-run *safe*.

**Docs:** [RustFS](https://docs.rustfs.com/) ·
[boto3 S3 client](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html) ·
[DuckDB httpfs / S3](https://duckdb.org/docs/stable/core_extensions/httpfs/s3api)

### 3 — Pick an analytical store (reference options: Postgres or DuckDB)

| | **DuckDB** | **Postgres** |
| --- | --- | --- |
| Setup | A file. Zero servers. | Docker container + credentials. |
| Writers | **One at a time** — the file locks. | Many concurrent clients. |
| Best at | Analytics on one machine, columnar scans, Parquet | Concurrency, serving apps, being a real server |
| Gotcha | Metabase/Streamlit holding the file blocks `dbt build` | You have to actually manage a service |

**If you pick DuckDB**, you inherit Week 6's single-writer problem: publish a
**separate serving copy** of your gold tables (`data/serving/warehouse.duckdb`)
and point the serving tool at *that*, read-only. The transformation writer and
the serving reader must not hold the same DuckDB file open.

**If you pick Postgres**, that whole problem disappears — dbt writes while
Metabase reads. That's a legitimate reason to choose it, and a great
`DECISIONS.md` paragraph.

```yaml
# docker-compose.yml — the Postgres option
services:
  postgres:
    image: postgres:17
    container_name: final_warehouse
    ports:
      - "5432:5432"
    environment:
      POSTGRES_DB: warehouse
      POSTGRES_USER: de
      POSTGRES_PASSWORD: de
    volumes:
      - pg_final_data:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U de -d warehouse"]
      interval: 5s
      retries: 10

volumes:
  pg_final_data:
```

**Docs:** [DuckDB](https://duckdb.org/docs/stable/) ·
[DuckDB Python API](https://duckdb.org/docs/stable/clients/python/overview) ·
[Postgres 17](https://www.postgresql.org/docs/17/index.html) ·
[psycopg](https://www.psycopg.org/psycopg3/docs/)

### 4 — Medallion: bronze → silver → gold (Week 3 code-along)

Three layers, one job each. Don't skip a layer because "my data is already
clean" — it isn't, and the layers are what make the lineage readable.

| Layer | Contains | Rule |
| --- | --- | --- |
| **Bronze** | The raw landing, loaded as-is | **No cleaning.** Text columns are fine. One table per source file. |
| **Silver** | Typed, deduped, filtered, renamed | One row per real-world thing. Money is a number. Dates are dates. |
| **Gold** | Business-ready aggregates | Answers your question directly. This is what the dashboard reads. |

The Week 2/3 messy-data toolkit, still the same:

```sql
-- dedupe: keep the newest record per id
with ranked as (
    select *,
           row_number() over (partition by order_id order by updated_at desc) as rn
    from {{ ref('bronze_orders') }}
)
select * from ranked where rn = 1
```

Money as text → `replace(replace(price, '$', ''), ',', '')::decimal(10,2)`.
Multiple date formats → `try_strptime` (DuckDB) / `to_date` with a coalesce
chain (Postgres). NULL join keys → **decide** what happens to them and say so in
a comment.

### 5 — Transform and test (reference option: dbt)

Your transformation system needs a dependency graph or equally clear lineage,
automated data-quality checks, and documentation. dbt is the reference option,
and its layout mirrors the medallion:

```
models/
├── sources.yml            # declare the bronze tables dbt did NOT create
├── bronze/
│   └── schema.yml
├── silver/
│   ├── silver_orders.sql          -- {{ source('raw', 'orders') }}
│   └── schema.yml                 -- tests live here
└── gold/
    ├── gold_daily_revenue.sql     -- {{ ref('silver_orders') }}
    └── schema.yml
```

```yaml
# models/silver/schema.yml
version: 2
models:
  - name: silver_orders
    description: One row per order, deduped and typed.
    columns:
      - name: order_id
        tests: [unique, not_null]
      - name: status
        tests:
          - accepted_values:
              values: ['shipped', 'pending', 'cancelled']
```

In the reference stack, `dbt build` means run **and** test in dependency order,
and the Prefect flow calls that command. Materialize silver as `view` and gold
as `table` unless you have a reason not to.

```yaml
# profiles.yml — DuckDB
final:
  target: dev
  outputs:
    dev: { type: duckdb, path: data/warehouse.duckdb, threads: 1 }

# profiles.yml — Postgres
final:
  target: dev
  outputs:
    dev:
      type: postgres
      host: localhost
      port: 5432
      user: de
      password: de
      dbname: warehouse
      schema: public
      threads: 4
```

**Docs:** [dbt docs home](https://docs.getdbt.com/) ·
[Build your first models](https://docs.getdbt.com/guides/manual-install) ·
[Tests](https://docs.getdbt.com/docs/build/data-tests) ·
[Sources](https://docs.getdbt.com/docs/build/sources) ·
[dbt-duckdb](https://github.com/duckdb/dbt-duckdb) ·
[dbt-postgres](https://docs.getdbt.com/docs/core/connect-data-platform/postgres-setup)

### 6 — Orchestrate it (reference option: Prefect)

Your pipeline becomes one observable workflow with run history, retries,
schedules, and logs. Prefect is the reference option from the course:

```python
from prefect import flow, task, get_run_logger

@task(retries=3, retry_delay_seconds=5)     # the flaky edge: the network
def extract_and_land() -> str:
    logger = get_run_logger()
    key = land_to_s3(fetch_source())
    logger.info("landed raw at s3://raw/%s", key)
    return key

@task
def load_bronze(key: str) -> None: ...

@task
def dbt_build() -> None: ...                 # subprocess: `dbt build`

@task
def publish() -> None: ...                   # gold -> serving copy (DuckDB only)

@flow(log_prints=True)
def run_pipeline():
    key = extract_and_land()
    load_bronze(key)
    dbt_build()
    publish()

if __name__ == "__main__":
    run_pipeline.serve(name="final-project", cron="0 6 * * *")
```

```bash
uv run prefect server start                                    # terminal 1, UI :4200
uv run prefect config set PREFECT_API_URL=http://127.0.0.1:4200/api   # once
uv run python -m pipeline.flow                                 # terminal 2
```

**Retries mean steps run twice.** Walk each task and ask *"what happens if this
runs again?"* — `CREATE OR REPLACE` and date-keyed S3 objects are doing the
quiet work of making that safe. Anything that *appends* is a bug waiting for a
retry.

**Docs:** [Prefect docs](https://docs.prefect.io/v3/get-started) ·
[Tasks & retries](https://docs.prefect.io/v3/concepts/tasks) ·
[Deployments & schedules](https://docs.prefect.io/v3/how-to-guides/deployments/create-schedules) ·
[Logging](https://docs.prefect.io/v3/concepts/logging) ·
[cron syntax helper](https://crontab.guru/)

### 7 — Serve it (reference options: Metabase or Streamlit)

Two views minimum, built on **gold**, answering the questions from slide one.
Not toy numbers — your real columns. Metabase and Streamlit are the reference
options, but another human-facing delivery method is valid.

**Metabase** — clicks, dashboards, the thing analysts actually use.

```yaml
services:
  metabase:
    build: ./metabase          # Metabase + DuckDB driver (Week 5's Dockerfile)
    ports: ["3000:3000"]
    environment:
      MB_DB_FILE: /metabase-data/metabase.db
    volumes:
      - ./data/serving:/serving:ro       # DuckDB: read-only serving copy
      - ./data/metabase:/metabase-data
```

DuckDB in Metabase: database file `/serving/warehouse.duckdb`, **Read-only ON**
(required — the mount is `:ro`). Postgres in Metabase: host `host.docker.internal`,
port 5432, your db/user/password.

**Streamlit** — code-first, a real app, easy to make interactive.

```python
import duckdb, streamlit as st

@st.cache_data
def load():
    with duckdb.connect("data/serving/warehouse.duckdb", read_only=True) as con:
        return con.sql("select * from main.gold_daily_revenue").df()

st.title("Earthquakes by day")
df = load()
floor = st.slider("Minimum magnitude", 0.0, 8.0, 4.0)
st.bar_chart(df[df.max_magnitude >= floor].set_index("day")["quake_count"])
```

```bash
uv run streamlit run app.py        # http://localhost:8501
```

**Docs:** [Metabase](https://www.metabase.com/docs/latest/) ·
[Metabase questions](https://www.metabase.com/docs/latest/questions/query-builder/editor) ·
[Streamlit](https://docs.streamlit.io/) ·
[Streamlit charts](https://docs.streamlit.io/develop/api-reference/charts) ·
[Plotly](https://plotly.com/python/) (if you want nicer charts)

---

## The presentation

A slide deck — Google Slides, Keynote, PowerPoint, or equivalent. You are
presenting **the data**, not a code tour.

| Slide | Content |
| --- | --- |
| 1 | **The data** — what it is, where it came from, why you picked it |
| 2 | **The questions** — the two-plus things you set out to answer |
| 3 | **The pipeline** — one diagram, your five choices marked on it |
| 4 | **The answers** — 📸 **screenshot of your dashboard, app, report, or other serving output** |
| 5 | **It runs itself** — 📸 **screenshot of your orchestrator showing successful runs** |
| 6 | **What broke / what I'd do next** — the honest slide, and the best one |

**Two screenshots are required and graded:**

1. **Your data visualization** — the actual dashboard, app, report, or equivalent,
   readable, with real numbers from your gold outputs.
2. **Your orchestration evidence** — a run-history or deployment view showing
   **successful runs** (plural, including scheduled ones) with timestamps.

Both screenshots must show current, reproducible results. "The pipeline worked
before" is not evidence.

If scope becomes too large, **cut scope, not pipeline capabilities.** Use fewer
columns, one source, or a narrower question while preserving raw retention,
trusted transformations, tests, orchestration, and a human-facing result. A
small complete pipeline beats an ambitious broken one.

---

## How you'll know you're done

Reset your analytical store, stop every local service, and run your own README
from the top. If a stranger can go from `git clone` to the final serving output
using only what you wrote down, you're done. If you had to remember one
undocumented step, you're not.

Then check your orchestrator's evidence: successful scheduled runs, at least one
retry in the logs, and a failed run from a deliberate failure test. That's a
pipeline.

---

## Troubleshooting

These entries cover the course's reference stack. If you substitute a component,
document the equivalent failure modes and recovery steps in your own README.

- **`database is locked` / `Could not set lock` (DuckDB)** — two writers. Metabase,
  Streamlit, a stray REPL, or a second flow run has the file open. One writer at
  a time; that's what the serving copy is for.
- **Metabase shows stale numbers** — *Sync database schema now* in the database
  settings, or re-run the question. It cached the previous serving copy.
- **Prefect UI shows no runs** — your flow talked to a throwaway local API. Run
  `prefect config set PREFECT_API_URL=http://127.0.0.1:4200/api` and keep the
  server terminal alive.
- **`.serve()` seems to hang** — it isn't. It's serving, waiting for the cron.
  Ctrl+C stops it (and pauses the schedule).
- **S3 `NoSuchBucket`** — create it once (`s3.create_bucket`) or make the load
  step idempotent about it.
- **API returns 403/401** — token in `.env`, `load_dotenv()` called, and the
  header spelled exactly how the docs spell it.
- **Port already in use** — an old week's RustFS (9000/9001), Metabase (3000),
  Prefect (4200), or Postgres (5432) is still up. `docker compose down` in that
  folder.
- **dbt can't find your source** — `sources.yml` names the *schema and table as
  they exist in the warehouse*, not what you wish they were called.

---

## The invariant

**Preserve the source as it arrived before transforming it, and make downstream
steps replay from that raw landing instead of reading the source directly.**
The storage product is your choice. Any substituted component must still uphold
the same pipeline contract, and the tradeoff belongs in `DECISIONS.md`.
