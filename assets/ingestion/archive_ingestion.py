"""@bruin
name: archive_ingestion
type: python
image: python:3.13
connection: gcs-default
@bruin"""

import json
import os
import tempfile
from datetime import date, datetime, timedelta

import requests
from google.cloud import storage

BASE_URL = "https://data.gharchive.org"
USER_AGENT = "bruin-archive-ingestion/1.0"
CHUNK_SIZE = 8 * 1024 * 1024


def _parse_date(s: str) -> date:
    return datetime.strptime(s.strip()[:10], "%Y-%m-%d").date()


def _daterange(start: date, end: date):
    cur = start
    while cur <= end:
        yield cur
        cur += timedelta(days=1)


def _bucket_name() -> str:
    raw = os.environ.get("gcs-default")
    if raw:
        data = json.loads(raw)
        if data.get("bucket"):
            return data["bucket"]
    vars_ = json.loads(os.environ.get("BRUIN_VARS") or "{}")
    if vars_.get("gcs_bucket"):
        return vars_["gcs_bucket"]
    raise RuntimeError(
        "Set pipeline variable gcs_bucket (see pipeline.yml) or add bucket to the gcs connection."
    )


def _ingest_hour(bucket: storage.Bucket, d: date, hour: int) -> None:
    url = f"{BASE_URL}/{d.isoformat()}-{hour}.json.gz"
    rel = f"year={d.year}/month={d.month:02d}/day={d.day:02d}/{hour}.json.gz"
    blob = bucket.blob(rel)
    if blob.exists():
        print(f"skip (exists): gs://{bucket.name}/{rel}")
        return

    headers = {"User-Agent": USER_AGENT}
    r = requests.get(url, headers=headers, stream=True, timeout=600)
    try:
        if r.status_code == 404:
            print(f"skip (404): {url}")
            return
        r.raise_for_status()

        path = None
        try:
            with tempfile.NamedTemporaryFile(suffix=".json.gz", delete=False) as tmp:
                path = tmp.name
                for chunk in r.iter_content(chunk_size=CHUNK_SIZE):
                    if chunk:
                        tmp.write(chunk)
            blob.upload_from_filename(path, content_type="application/gzip")
        finally:
            if path and os.path.exists(path):
                os.unlink(path)
    finally:
        r.close()

    print(f"uploaded: gs://{bucket.name}/{rel}")


def run() -> None:
    start = _parse_date(os.environ["BRUIN_START_DATE"])
    end = _parse_date(os.environ["BRUIN_END_DATE"])
    bname = _bucket_name()
    client = storage.Client()
    bucket = client.bucket(bname)

    for d in _daterange(start, end):
        for hour in range(24):
            _ingest_hour(bucket, d, hour)


run()
