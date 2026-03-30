"""
Deprecated local implementation.

This file is intentionally NOT a Bruin asset anymore (no `@bruin` header).
Use `assets/raw_archive_github_events.sql` to compute/flatten in BigQuery.
"""

import gzip
import io
import json
import os
from datetime import date, datetime, timedelta

import pandas as pd
from google.cloud import storage


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


def _blob_paths_for_range(start: date, end: date):
    for d in _daterange(start, end):
        for hour in range(24):
            yield (
                f"year={d.year}/month={d.month:02d}/day={d.day:02d}/{hour}.json.gz",
                d,
                hour,
            )


def _normalize_frame(data: list[dict]) -> pd.DataFrame:
    if not data:
        return pd.DataFrame()
    df = pd.json_normalize(data)
    df["repo_name"] = df["repo.name"] if "repo.name" in df.columns else None
    df["actor_login"] = df["actor.login"] if "actor.login" in df.columns else None
    df["created_at"] = pd.to_datetime(df["created_at"], utc=True)
    df["date"] = df["created_at"].dt.date
    df["hour"] = df["created_at"].dt.hour
    cols = [
        "id",
        "type",
        "repo_name",
        "actor_login",
        "created_at",
        "date",
        "hour",
    ]
    for c in cols:
        if c not in df.columns and c not in ("repo_name", "actor_login"):
            df[c] = None
    df["id"] = df["id"].astype(str)
    return df[cols]


def materialize():
    start = _parse_date(os.environ["BRUIN_START_DATE"])
    end = _parse_date(os.environ["BRUIN_END_DATE"])
    bucket_name = _bucket_name()
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    extracted_at_run = pd.Timestamp.now(tz="UTC")

    frames = []
    for rel, _d, _h in _blob_paths_for_range(start, end):
        blob = bucket.blob(rel)
        if not blob.exists():
            print(f"skip (missing in GCS): gs://{bucket_name}/{rel}")
            continue
        raw = blob.download_as_bytes()
        with gzip.GzipFile(fileobj=io.BytesIO(raw)) as gz:
            lines = gz.read().splitlines()
        rows = []
        for line in lines:
            if not line.strip():
                continue
            rows.append(json.loads(line))
        if not rows:
            continue
        part = _normalize_frame(rows)
        if not part.empty:
            part = part.copy()
            part["extracted_at"] = extracted_at_run
            frames.append(part)
        print(f"processed: gs://{bucket_name}/{rel} ({len(rows)} events)")

    if not frames:
        return pd.DataFrame(
            columns=[
                "id",
                "type",
                "repo_name",
                "actor_login",
                "created_at",
                "date",
                "hour",
                "extracted_at",
            ]
        )

    out = pd.concat(frames, ignore_index=True)
    out = out.drop_duplicates(subset=["id"], keep="last")
    print(f"Total rows: {len(out)}")
    return out
