from __future__ import annotations

import argparse
import gzip
import io
import json
from typing import Iterable

import boto3

from cbbd_etl.config import load_config
from cbbd_etl.orchestrate import RAW_PREFIX_OVERRIDES
from cbbd_etl.s3_io import make_part_key


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="List non-empty raw JSON.gz parts for an endpoint/ingest date.")
    parser.add_argument("--endpoint", required=True, help="Endpoint name (e.g., plays_game)")
    parser.add_argument("--ingested-at", required=True, help="Raw ingest date (YYYY-MM-DD)")
    parser.add_argument("--output", required=True, help="Output file path (one key per line)")
    parser.add_argument("--limit", type=int, help="Optional limit on number of keys to scan")
    parser.add_argument("--min-records", type=int, default=1, help="Minimum JSON records required to keep a key")
    parser.add_argument("--require-field", help="Require a JSON field to be present (e.g., gameId)")
    return parser.parse_args()


def _iter_non_empty_lines(body: bytes) -> Iterable[bytes]:
    with gzip.GzipFile(fileobj=io.BytesIO(body), mode="rb") as gz:
        for line in gz:
            line = line.strip()
            if line:
                yield line


def _line_has_field(line: bytes, field: str | None) -> bool:
    if field is None:
        return True
    try:
        obj = json.loads(line)
    except Exception:
        return False
    if isinstance(obj, dict):
        return field in obj
    if isinstance(obj, list):
        return any(isinstance(item, dict) and field in item for item in obj)
    return False


def _count_records(client, bucket: str, key: str, require_field: str | None) -> int:
    obj = client.get_object(Bucket=bucket, Key=key)
    data = obj["Body"].read()
    count = 0
    for line in _iter_non_empty_lines(data):
        if _line_has_field(line, require_field):
            count += 1
    return count


def main() -> None:
    args = _parse_args()
    cfg = load_config()

    raw_endpoint = RAW_PREFIX_OVERRIDES.get(args.endpoint, args.endpoint)
    raw_prefix = make_part_key(cfg.s3_layout["raw_prefix"], raw_endpoint, f"ingested_at={args.ingested_at}")

    client = boto3.client("s3", region_name=cfg.region)
    paginator = client.get_paginator("list_objects_v2")
    keys = []
    for page in paginator.paginate(Bucket=cfg.bucket, Prefix=raw_prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".json.gz"):
                keys.append(key)

    keys.sort()
    if args.limit:
        keys = keys[: args.limit]

    non_empty: list[str] = []
    total_records = 0
    kept_records = 0
    for idx, key in enumerate(keys, start=1):
        record_count = _count_records(client, cfg.bucket, key, args.require_field)
        total_records += record_count
        if record_count >= args.min_records:
            non_empty.append(key)
            kept_records += record_count
        if idx % 100 == 0 or idx == len(keys):
            print(
                f"scanned {idx}/{len(keys)} kept={len(non_empty)} "
                f"records={total_records} kept_records={kept_records}"
            )

    with open(args.output, "w", encoding="utf-8") as f:
        for key in non_empty:
            f.write(f"{key}\n")

    print(
        f"done: kept={len(non_empty)} records={total_records} kept_records={kept_records} "
        f"output={args.output}"
    )


if __name__ == "__main__":
    main()
