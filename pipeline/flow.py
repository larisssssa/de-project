"""Daily orchestration: land raw -> load bronze -> dbt build -> publish.

Usage (run from the repo root):
    python -m pipeline.flow           # serve the flow on a daily schedule (keep running)
    python -m pipeline.flow once      # run it one time right now

Retries happen at two levels: each HTTP call retries with backoff inside
extract.py, and each Prefect task retries as a whole if it still fails.
Every step is idempotent (date-keyed raw objects, CREATE OR REPLACE tables),
so a retry or a same-day re-run never duplicates data.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

from prefect import flow, get_run_logger, task

from pipeline.extract import extract_and_land, extract_decade_counts
from pipeline.load_bronze import DUCKDB_PATH, load_bronze
from pipeline.publish import publish

DBT_DIR = Path(__file__).resolve().parent.parent / "dbt"


@task(name="land-works", retries=2, retry_delay_seconds=60)
def land_works(subjects: list[str] | None) -> int:
    logger = get_run_logger()
    keys = extract_and_land(subjects, log=logger.info)
    logger.info(f"landed {len(keys)} work pages")
    return len(keys)


@task(name="land-decade-counts", retries=2, retry_delay_seconds=60)
def land_counts(subjects: list[str] | None) -> int:
    logger = get_run_logger()
    keys = extract_decade_counts(subjects, log=logger.info)
    logger.info(f"landed {len(keys)} decade counts")
    return len(keys)


@task(name="load-bronze", retries=1, retry_delay_seconds=30)
def load_bronze_task() -> int:
    logger = get_run_logger()
    rows = load_bronze(log=logger.info)
    logger.info(f"bronze rows: {rows}")
    return rows


@task(name="dbt-build")  # no retry: a failed data test should fail the run loudly
def dbt_build() -> None:
    logger = get_run_logger()
    dbt_bin = Path(sys.executable).with_name("dbt")
    cmd = [str(dbt_bin) if dbt_bin.exists() else "dbt", "build"]
    # Point dbt at the same warehouse file the Python loader wrote.
    env = {**os.environ, "DBT_DUCKDB_PATH": str(Path(DUCKDB_PATH).resolve())}
    result = subprocess.run(cmd, cwd=DBT_DIR, env=env, capture_output=True, text=True)
    for line in result.stdout.splitlines():
        logger.info(line)
    if result.returncode != 0:
        logger.error(result.stderr)
        raise RuntimeError(f"dbt build failed (exit {result.returncode})")


@task(name="publish-serving", retries=1, retry_delay_seconds=15)
def publish_task() -> dict[str, int]:
    logger = get_run_logger()
    return publish(log=logger.info)


@flow(name="openlibrary-daily", log_prints=True)
def openlibrary_daily(subjects: list[str] | None = None) -> None:
    """subjects=None runs all 15 default subjects; pass a short list for a quick run."""
    land_works(subjects)
    land_counts(subjects)
    load_bronze_task()
    dbt_build()
    publish_task()


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "once":
        openlibrary_daily()
    else:
        openlibrary_daily.serve(name="openlibrary-daily", cron="0 6 * * *")
