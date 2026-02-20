from __future__ import annotations

import argparse
import gzip
import json
from typing import Set

import boto3


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Extract gameIds from raw lineups files.")
    parser.add_argument("--ingested-at", required=True, help="ingested_at partition (YYYY-MM-DD)")
    parser.add_argument("--output", required=True, help="Output file path for gameIds")
    parser.add_argument("--bucket", default="hoops-edge", help="S3 bucket name")
    parser.add_argument("--prefix", default="raw/lineups/game", help="Raw prefix for lineups")
    parser.add_argument("--log-every", type=int, default=500, help="Progress log interval")
    return parser.parse_args()


def _iter_keys(s3, bucket: str, prefix: str):
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".json.gz"):
                yield key


def _read_first_record(s3, bucket: str, key: str) -> dict | None:
    resp = s3.get_object(Bucket=bucket, Key=key)
    body = resp["Body"]
    try:
        with gzip.GzipFile(fileobj=body) as gz:
            line = gz.readline()
    finally:
        body.close()
    if not line:
        return None
    try:
        return json.loads(line)
    except Exception:
        return None


def main() -> None:
    args = _parse_args()
    s3 = boto3.client("s3")
    prefix = f"{args.prefix}/ingested_at={args.ingested_at}/"
    game_ids: Set[int] = set()
    scanned = 0

    for key in _iter_keys(s3, args.bucket, prefix):
        scanned += 1
        rec = _read_first_record(s3, args.bucket, key)
        if isinstance(rec, dict):
            gid = rec.get("gameId") or rec.get("gameid")
            if gid is not None:
                try:
                    game_ids.add(int(gid))
                except Exception:
                    pass
        if args.log_every and scanned % args.log_every == 0:
            print(f"scanned {scanned} keys, game_ids={len(game_ids)}")

    with open(args.output, "w", encoding="utf-8") as f:
        for gid in sorted(game_ids):
            f.write(f"{gid}\n")

    print(f"done: scanned={scanned} game_ids={len(game_ids)} output={args.output}")


if __name__ == "__main__":
    main()
