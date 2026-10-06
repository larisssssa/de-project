"""Extract Open Library works by subject and land the raw JSON pages in S3.

Usage:
    python -m pipeline.extract profile "science fiction"   # sample call, field fill rates, no landing
    python -m pipeline.extract land                         # full run for DEFAULT_SUBJECTS
    python -m pipeline.extract land fantasy mystery         # specific subjects
    python -m pipeline.extract counts                       # true catalog counts per subject x decade

Raw pages are stored byte-for-byte as returned, keyed by run date, so a retry
or a re-run on the same day rewrites the same keys instead of duplicating data.
"""

from __future__ import annotations

import json
import os
import re
import sys
import time
from datetime import datetime, timezone

import boto3
import httpx
from botocore.exceptions import ClientError
from dotenv import load_dotenv
from tenacity import (
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

load_dotenv()

BASE_URL = "https://openlibrary.org/search.json"
PAGE_SIZE = 100
MAX_WORKS_PER_SUBJECT = int(os.getenv("MAX_WORKS_PER_SUBJECT", "1500"))
# Stay well under the identified-request limit (check the developer docs).
MIN_INTERVAL_SECONDS = float(os.getenv("MIN_INTERVAL_SECONDS", "0.5"))

# Confirm with `profile` which of these actually come back populated.
FIELDS = ",".join(
    [
        "key",
        "title",
        "author_name",
        "author_key",
        "first_publish_year",
        "edition_count",
        "subject",
        "language",
        "ratings_average",
        "ratings_count",
        "want_to_read_count",
        "already_read_count",
        "number_of_pages_median",
    ]
)

DEFAULT_SUBJECTS = [
    "fantasy",
    "science fiction",
    "mystery",
    "romance",
    "horror",
    "historical fiction",
    "biography",
    "history",
    "science",
    "philosophy",
    "poetry",
    "children",
    "cooking",
    "travel",
    "art",
]


def _user_agent() -> str:
    ua = os.getenv("OPENLIBRARY_USER_AGENT")
    if not ua:
        raise RuntimeError(
            "Set OPENLIBRARY_USER_AGENT in .env, e.g. 'BookPipeline (you@example.com)'"
        )
    return ua


def _is_retryable(exc: BaseException) -> bool:
    """Retry rate limits, server errors, and network problems; fail fast on other 4xx."""
    if isinstance(exc, httpx.HTTPStatusError):
        code = exc.response.status_code
        return code == 429 or code >= 500
    return isinstance(exc, httpx.TransportError)


@retry(
    retry=retry_if_exception(_is_retryable),
    stop=stop_after_attempt(5),
    wait=wait_exponential(min=1, max=30),
    reraise=True,
)
def fetch_page(client: httpx.Client, subject: str, page: int) -> httpx.Response:
    resp = client.get(
        BASE_URL,
        params={
            "q": f'subject:"{subject}"',
            "fields": FIELDS,
            "limit": PAGE_SIZE,
            "page": page,
        },
        timeout=30,
    )
    resp.raise_for_status()  # 429/5xx -> raises -> tenacity retries
    return resp


def _slug(subject: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", subject.lower()).strip("-")


def _s3():
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("S3_ENDPOINT", "http://localhost:9000"),
        aws_access_key_id=os.getenv("S3_ACCESS_KEY", "rustfsadmin"),
        aws_secret_access_key=os.getenv("S3_SECRET_KEY", "rustfsadmin"),
        region_name="us-east-1",
    )


def _ensure_bucket(s3, bucket: str) -> None:
    try:
        s3.head_bucket(Bucket=bucket)
    except ClientError:
        s3.create_bucket(Bucket=bucket)


def raw_key(subject: str, run_date: str, page: int) -> str:
    return f"openlibrary/subject={_slug(subject)}/{run_date}/page-{page:03d}.json"


def extract_and_land(
    subjects: list[str] | None = None,
    run_date: str | None = None,
    max_works: int = MAX_WORKS_PER_SUBJECT,
    log=print,
) -> list[str]:
    """Fetch every page for each subject and land it raw. Returns the S3 keys written."""
    subjects = subjects or DEFAULT_SUBJECTS
    run_date = run_date or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    bucket = os.getenv("RAW_BUCKET", "raw")

    s3 = _s3()
    _ensure_bucket(s3, bucket)
    keys: list[str] = []

    with httpx.Client(headers={"User-Agent": _user_agent()}) as client:
        for subject in subjects:
            fetched, page = 0, 1
            while fetched < max_works:
                resp = fetch_page(client, subject, page)
                docs = resp.json().get("docs", [])
                if not docs:  # no next page: stop. Never hardcode a page count.
                    break

                key = raw_key(subject, run_date, page)
                # resp.content is the response body exactly as received.
                s3.put_object(
                    Bucket=bucket,
                    Key=key,
                    Body=resp.content,
                    ContentType="application/json",
                )
                keys.append(key)
                fetched += len(docs)
                log(f"landed s3://{bucket}/{key} ({len(docs)} works)")

                if len(docs) < PAGE_SIZE:  # short page = last page
                    break
                page += 1
                time.sleep(MIN_INTERVAL_SECONDS)

            log(f"{subject}: {fetched} works over {page} page(s)")
            time.sleep(MIN_INTERVAL_SECONDS)

    return keys


DECADES = list(range(1900, 2030, 10))  # 1900s .. 2020s; pre-1900 is out of scope


@retry(
    retry=retry_if_exception(_is_retryable),
    stop=stop_after_attempt(5),
    wait=wait_exponential(min=1, max=30),
    reraise=True,
)
def fetch_decade_count(
    client: httpx.Client, subject: str, decade: int
) -> httpx.Response:
    """One tiny request: numFound for a subject within a decade of first publication."""
    resp = client.get(
        BASE_URL,
        params={
            "q": f'subject:"{subject}" AND first_publish_year:[{decade} TO {decade + 9}]',
            "fields": "key",
            "limit": 1,  # we only want numFound, not the records
        },
        timeout=30,
    )
    resp.raise_for_status()
    return resp


def counts_key(subject: str, run_date: str, decade: int) -> str:
    return (
        f"openlibrary_counts/subject={_slug(subject)}/{run_date}/decade-{decade}.json"
    )


def extract_decade_counts(
    subjects: list[str] | None = None,
    run_date: str | None = None,
    log=print,
) -> list[str]:
    """Land true catalog counts per subject and decade (one small request each).

    Unlike the sampled page pulls, numFound reflects the whole catalog, so these
    counts answer question 1 without the relevance-ranking bias of a capped sample.
    """
    subjects = subjects or DEFAULT_SUBJECTS
    run_date = run_date or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    bucket = os.getenv("RAW_BUCKET", "raw")

    s3 = _s3()
    _ensure_bucket(s3, bucket)
    keys: list[str] = []

    with httpx.Client(headers={"User-Agent": _user_agent()}) as client:
        for subject in subjects:
            for decade in DECADES:
                resp = fetch_decade_count(client, subject, decade)
                key = counts_key(subject, run_date, decade)
                s3.put_object(
                    Bucket=bucket,
                    Key=key,
                    Body=resp.content,
                    ContentType="application/json",
                )
                keys.append(key)
                time.sleep(MIN_INTERVAL_SECONDS)
            log(f"{subject}: landed {len(DECADES)} decade counts")

    return keys


def profile(subject: str) -> None:
    """One sample page: show which fields are populated. Nothing is landed."""
    with httpx.Client(headers={"User-Agent": _user_agent()}) as client:
        body = fetch_page(client, subject, 1).json()

    docs = body.get("docs", [])
    print(f"subject={subject!r}  numFound={body.get('numFound')}  sampled={len(docs)}")
    if not docs:
        return

    print(f"\n{'field':<26}{'filled':>8}")
    for field in FIELDS.split(","):
        filled = sum(1 for d in docs if d.get(field) not in (None, [], ""))
        print(f"{field:<26}{filled / len(docs):>8.0%}")

    print("\nfirst record:")
    print(json.dumps(docs[0], indent=2)[:1500])


if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "profile"
    args = sys.argv[2:]
    if cmd == "profile":
        profile(args[0] if args else "fantasy")
    elif cmd == "land":
        extract_and_land(args or None)
    elif cmd == "counts":
        extract_decade_counts(args or None)
    else:
        sys.exit(__doc__)
