"""Copy the gold tables into a separate serving database for Streamlit.

DuckDB allows one writer at a time, and a dashboard holding the warehouse open
would block dbt. So the app reads data/serving.duckdb, which this step builds
beside the warehouse and swaps in atomically: readers see the old file or the
new one, never a half-written one.

Usage:
    python -m pipeline.publish
"""
from __future__ import annotations

import os
from pathlib import Path

import duckdb
from dotenv import load_dotenv

load_dotenv()

WAREHOUSE = os.getenv("DUCKDB_PATH", "data/warehouse.duckdb")
SERVING = os.getenv("SERVING_DB_PATH", "data/serving.duckdb")

GOLD_TABLES = [
    "gold_works_by_subject_decade",
    "gold_edition_popularity_by_subject",
]


def publish(log=print) -> dict[str, int]:
    tmp = f"{SERVING}.tmp"
    Path(SERVING).parent.mkdir(parents=True, exist_ok=True)
    Path(tmp).unlink(missing_ok=True)

    counts: dict[str, int] = {}
    con = duckdb.connect(tmp)
    try:
        con.execute(f"ATTACH '{WAREHOUSE}' AS wh (READ_ONLY)")
        for table in GOLD_TABLES:
            con.execute(f"CREATE TABLE {table} AS SELECT * FROM wh.gold.{table}")
            counts[table] = con.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
            log(f"published {table}: {counts[table]} rows")
        con.execute("CREATE TABLE published_at AS SELECT current_timestamp AS published_at")
        con.execute("DETACH wh")
    finally:
        con.close()

    os.replace(tmp, SERVING)  # atomic swap
    log(f"serving database ready: {SERVING}")
    return counts


if __name__ == "__main__":
    publish()
