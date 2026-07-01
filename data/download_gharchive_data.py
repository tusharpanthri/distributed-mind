"""Download GH Archive hourly dumps, flatten to a fixed schema, and upload to MinIO.

Usage:
    python -m data.download_gharchive_data --date 2024-01-15 --hours 24
    python -m data.download_gharchive_data --date 2024-01-15 --hours 2 --sample-size 50000
"""

from __future__ import annotations

import gzip
import io
import json
import logging
import os
import sys
from datetime import datetime
from typing import Iterator

import click
import pyarrow as pa
import pyarrow.parquet as pq
import yaml
from minio import Minio
from pythonjsonlogger import jsonlogger

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logger = logging.getLogger("distributedmind.downloader")


def _setup_logging() -> None:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(jsonlogger.JsonFormatter("%(asctime)s %(name)s %(levelname)s %(message)s"))
    logging.root.setLevel(logging.INFO)
    logging.root.addHandler(handler)


# ---------------------------------------------------------------------------
# Fixed output schema
# ---------------------------------------------------------------------------

FLAT_SCHEMA = pa.schema(
    [
        pa.field("id", pa.string()),
        pa.field("type", pa.string()),
        pa.field("actor_login", pa.string()),
        pa.field("repo_id", pa.int64()),
        pa.field("repo_name", pa.string()),
        pa.field("created_at", pa.timestamp("us", tz="UTC")),
        pa.field("payload_size", pa.int64()),   # PushEvent: number of commits
        pa.field("payload_action", pa.string()),  # WatchEvent: action string
    ]
)

# ---------------------------------------------------------------------------
# Static repo-metadata lookup (hardcoded well-known repos)
# ---------------------------------------------------------------------------

REPO_METADATA: list[dict] = [
    {"repo_id": 1392255, "language": "Ruby", "repo_owner_type": "Organization"},       # rails/rails
    {"repo_id": 28457823, "language": "Python", "repo_owner_type": "Organization"},    # django/django
    {"repo_id": 83222441, "language": "TypeScript", "repo_owner_type": "Organization"},# microsoft/vscode
    {"repo_id": 10270250, "language": "JavaScript", "repo_owner_type": "Organization"},# facebook/react
    {"repo_id": 8514, "language": "Python", "repo_owner_type": "User"},                # torvalds/linux
    {"repo_id": 1217096, "language": "Go", "repo_owner_type": "Organization"},         # golang/go
    {"repo_id": 2126244, "language": "Rust", "repo_owner_type": "Organization"},       # rust-lang/rust
    {"repo_id": 54346799, "language": "Python", "repo_owner_type": "Organization"},    # tensorflow/tensorflow
    {"repo_id": 65600975, "language": "Python", "repo_owner_type": "Organization"},    # pytorch/pytorch
    {"repo_id": 20580498, "language": "Java", "repo_owner_type": "Organization"},      # elastic/elasticsearch
    {"repo_id": 507775, "language": "C", "repo_owner_type": "Organization"},           # git/git
    {"repo_id": 1863329, "language": "JavaScript", "repo_owner_type": "Organization"}, # nodejs/node
    {"repo_id": 13491895, "language": "Python", "repo_owner_type": "Organization"},    # ansible/ansible
    {"repo_id": 41881900, "language": "Go", "repo_owner_type": "Organization"},        # kubernetes/kubernetes
    {"repo_id": 20928900, "language": "Go", "repo_owner_type": "Organization"},        # docker/docker
    {"repo_id": 6207190, "language": "Scala", "repo_owner_type": "Organization"},      # apache/spark
    {"repo_id": 60246359, "language": "Python", "repo_owner_type": "Organization"},    # apache/airflow
    {"repo_id": 7508411, "language": "Java", "repo_owner_type": "Organization"},       # apache/kafka
    {"repo_id": 16563587, "language": "TypeScript", "repo_owner_type": "Organization"},# angular/angular
    {"repo_id": 24195339, "language": "JavaScript", "repo_owner_type": "User"},        # vuejs/vue
    {"repo_id": 10270341, "language": "Python", "repo_owner_type": "Organization"},    # scikit-learn/scikit-learn
    {"repo_id": 3544424, "language": "Python", "repo_owner_type": "Organization"},     # numpy/numpy
    {"repo_id": 6811994, "language": "Python", "repo_owner_type": "Organization"},     # pandas-dev/pandas
    {"repo_id": 45717250, "language": "Python", "repo_owner_type": "Organization"},    # ray-project/ray
    {"repo_id": 21351054, "language": "Python", "repo_owner_type": "Organization"},    # dask/dask
]

REPO_METADATA_SCHEMA = pa.schema(
    [
        pa.field("repo_id", pa.int64()),
        pa.field("language", pa.string()),
        pa.field("repo_owner_type", pa.string()),
    ]
)

# ---------------------------------------------------------------------------
# GH Archive helpers
# ---------------------------------------------------------------------------

GHARCHIVE_BASE = "https://data.gharchive.org"
ALLOWED_TYPES = {"PushEvent", "WatchEvent"}


def _gharchive_url(date: str, hour: int) -> str:
    return f"{GHARCHIVE_BASE}/{date}-{hour}.json.gz"


def _flatten_event(raw: dict) -> dict | None:
    """Return flattened record or None if the event type should be skipped."""
    event_type = raw.get("type", "")
    if event_type not in ALLOWED_TYPES:
        return None

    payload = raw.get("payload", {})
    payload_size: int | None = None
    payload_action: str | None = None

    if event_type == "PushEvent":
        commits = payload.get("commits")
        payload_size = len(commits) if isinstance(commits, list) else payload.get("size")
    elif event_type == "WatchEvent":
        payload_action = payload.get("action")

    repo = raw.get("repo", {})
    actor = raw.get("actor", {})

    created_raw = raw.get("created_at", "")
    try:
        created_at = datetime.fromisoformat(created_raw.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        created_at = None  # type: ignore[assignment]

    return {
        "id": str(raw.get("id", "")),
        "type": event_type,
        "actor_login": actor.get("login", ""),
        "repo_id": int(repo.get("id", 0)),
        "repo_name": repo.get("name", ""),
        "created_at": created_at,
        "payload_size": payload_size,
        "payload_action": payload_action,
    }


def _stream_hourly_file(url: str) -> Iterator[dict]:
    """Stream-decompress and yield flattened records from one hourly file."""
    import urllib.request

    logger.info("Downloading", extra={"url": url})
    req = urllib.request.Request(url, headers={"User-Agent": "distributedmind-benchmark/1.0"})  # noqa: S310
    with urllib.request.urlopen(req, timeout=120) as response:  # noqa: S310
        with gzip.GzipFile(fileobj=io.BytesIO(response.read())) as gz:
            for line in gz:
                line = line.strip()
                if not line:
                    continue
                try:
                    raw = json.loads(line)
                except json.JSONDecodeError:
                    continue
                record = _flatten_event(raw)
                if record is not None:
                    yield record


# ---------------------------------------------------------------------------
# MinIO helpers
# ---------------------------------------------------------------------------

def _build_minio_client(cfg: dict) -> Minio:
    endpoint = cfg["minio"]["endpoint"].replace("http://", "").replace("https://", "")
    return Minio(
        endpoint,
        access_key=cfg["minio"]["access_key"],
        secret_key=cfg["minio"]["secret_key"],
        secure=cfg["minio"]["endpoint"].startswith("https"),
    )


def _ensure_bucket(client: Minio, bucket: str) -> None:
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)
        logger.info("Created bucket", extra={"bucket": bucket})


def _upload_parquet(client: Minio, bucket: str, key: str, table: pa.Table) -> None:
    buf = io.BytesIO()
    pq.write_table(table, buf, compression="snappy")
    buf.seek(0)
    size = buf.getbuffer().nbytes
    client.put_object(bucket, key, buf, size, content_type="application/octet-stream")
    logger.info("Uploaded parquet", extra={"bucket": bucket, "key": key, "bytes": size})


# ---------------------------------------------------------------------------
# Main ingestion logic
# ---------------------------------------------------------------------------

def _resolve_env(value: str) -> str:
    """Resolve ${ENV_VAR:default} placeholders in config values."""
    import re

    def replace(m: re.Match) -> str:
        var, _, default = m.group(1).partition(":")
        return os.environ.get(var, default)

    return re.sub(r"\$\{([^}]+)\}", replace, value)


def _load_config(config_path: str) -> dict:
    with open(config_path) as f:
        raw = yaml.safe_load(f)

    def walk(node: object) -> object:
        if isinstance(node, dict):
            return {k: walk(v) for k, v in node.items()}
        if isinstance(node, list):
            return [walk(i) for i in node]
        if isinstance(node, str):
            return _resolve_env(node)
        return node

    return walk(raw)  # type: ignore[return-value]


def ingest(
    date: str,
    hours: int,
    sample_size: int,
    config_path: str,
) -> None:
    """Download, flatten, and upload GH Archive data + lookup table to MinIO."""
    cfg = _load_config(config_path)
    client = _build_minio_client(cfg)
    raw_bucket = cfg["minio"]["bucket_raw"]
    _ensure_bucket(client, raw_bucket)

    total_rows = 0
    hour_idx = 0
    records_by_date: dict[str, list[dict]] = {}

    while hour_idx < hours:
        if sample_size > 0 and total_rows >= sample_size:
            logger.info("Sample size cap reached", extra={"rows": total_rows})
            break

        url = _gharchive_url(date, hour_idx)
        try:
            for record in _stream_hourly_file(url):
                if sample_size > 0 and total_rows >= sample_size:
                    break
                event_date = date  # partition by the requested date
                records_by_date.setdefault(event_date, []).append(record)
                total_rows += 1
        except Exception as exc:
            logger.warning("Failed to fetch hour", extra={"url": url, "error": str(exc)})

        hour_idx += 1
        logger.info("Hour processed", extra={"hour": hour_idx, "total_rows": total_rows})

    # Write one Parquet file per date partition
    raw_prefix = cfg["data"]["raw_prefix"]
    for event_date, rows in records_by_date.items():
        table = pa.Table.from_pylist(rows, schema=FLAT_SCHEMA)
        key = f"{raw_prefix}/date={event_date}/events.parquet"
        _upload_parquet(client, raw_bucket, key, table)

    # Write repo-metadata lookup
    lookup_table = pa.Table.from_pylist(REPO_METADATA, schema=REPO_METADATA_SCHEMA)
    lookup_key = cfg["data"]["lookup_key"]
    _upload_parquet(client, raw_bucket, lookup_key, lookup_table)

    logger.info("Ingestion complete", extra={"total_rows": total_rows})


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

@click.command()
@click.option("--date", default="2024-01-15", show_default=True, help="Date to download (YYYY-MM-DD)")
@click.option("--hours", default=24, show_default=True, help="Number of hours to download (0-23)")
@click.option("--sample-size", default=0, show_default=True, help="Cap total rows (0 = no cap)")
@click.option("--config", "config_path", default="config/benchmark_config.yaml", show_default=True)
def main(date: str, hours: int, sample_size: int, config_path: str) -> None:
    """Download GH Archive data and upload to MinIO."""
    _setup_logging()
    ingest(date=date, hours=hours, sample_size=sample_size, config_path=config_path)


if __name__ == "__main__":
    main()
