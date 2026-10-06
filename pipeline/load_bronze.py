"""Load every raw Open Library landing from S3 into DuckDB as the bronze layer.

Usage:
    python -m pipeline.load_bronze

Bronze is rebuilt from ALL landed pages on every run (CREATE OR REPLACE), so the
step is idempotent: a retry or a re-run gives the same table, and the raw store
stays the single source of truth. Fields are kept as they arrived (arrays stay
arrays); only lineage columns are added.
"""

from __future__ import annotations

import os
from pathlib import Path
from urllib.parse import urlparse

import duckdb
from dotenv import load_dotenv

load_dotenv()

DUCKDB_PATH = os.getenv("DUCKDB_PATH", "data/warehouse.duckdb")
RAW_BUCKET = os.getenv("RAW_BUCKET", "raw")
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://localhost:9000")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "rustfsadmin")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY", "rustfsadmin")

GLOB = f"s3://{RAW_BUCKET}/openlibrary/subject=*/*/page-*.json"

BRONZE_SQL = f"""
CREATE OR REPLACE TABLE bronze.openlibrary_works AS
WITH pages AS (
    SELECT
        filename AS _source_file,
        unnest(docs) AS doc
    FROM read_json(
        '{GLOB}',
        filename = true,
        sample_size = -1,
        union_by_name = true
    )
)
SELECT
    doc.*,
    _source_file,
    regexp_extract(_source_file, 'subject=([^/]+)/', 1)              AS _queried_subject,
    regexp_extract(_source_file, '/(\\d{{4}}-\\d{{2}}-\\d{{2}})/', 1)::date AS _run_date,
    regexp_extract(_source_file, 'page-(\\d+)\\.json', 1)::int        AS _page,
    current_timestamp                                                 AS _loaded_at
FROM pages
"""


def connect() -> duckdb.DuckDBPyConnection:
    Path(DUCKDB_PATH).parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(DUCKDB_PATH)

    endpoint = urlparse(S3_ENDPOINT)
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute(f"SET s3_endpoint = '{endpoint.netloc}';")
    con.execute(
        f"SET s3_use_ssl = {'true' if endpoint.scheme == 'https' else 'false'};"
    )
    con.execute("SET s3_url_style = 'path';")
    con.execute("SET s3_region = 'us-east-1';")
    con.execute(f"SET s3_access_key_id = '{S3_ACCESS_KEY}';")
    con.execute(f"SET s3_secret_access_key = '{S3_SECRET_KEY}';")
    return con


def load_bronze(log=print) -> int:
    con = connect()
    try:
        con.execute("CREATE SCHEMA IF NOT EXISTS bronze;")
        con.execute(BRONZE_SQL)
        rows = con.execute("SELECT count(*) FROM bronze.openlibrary_works").fetchone()[
            0
        ]
        log(f"bronze.openlibrary_works: {rows} rows from {GLOB}")

        summary = con.execute("""
            SELECT _queried_subject, _run_date, count(*) AS works
            FROM bronze.openlibrary_works
            GROUP BY 1, 2 ORDER BY 1, 2
            """).fetchall()
        for subject, run_date, works in summary:
            log(f"  {subject:<22} {run_date}  {works:>6}")
        return rows
    finally:
        con.close()  # release the file lock so dbt can write


if __name__ == "__main__":
    load_bronze()
