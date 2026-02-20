import argparse
import boto3
import gzip
import io
import json
import re

import pyarrow.parquet as pq


bucket = "hoops-edge"
s3 = boto3.client("s3", region_name="us-east-1")


def latest(prefix, n=1):
    keys = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            keys.append((obj["LastModified"], obj["Key"], obj["Size"]))
    keys.sort(key=lambda x: x[0], reverse=True)
    return keys[:n]


def read_raw(key):
    obj = s3.get_object(Bucket=bucket, Key=key)
    data = gzip.decompress(obj["Body"].read())
    try:
        payload = json.loads(data)
        if isinstance(payload, list):
            return len(payload), payload[:1]
        return 1, [payload]
    except json.JSONDecodeError:
        lines = [line for line in data.splitlines() if line.strip()]
        if not lines:
            return 0, []
        sample = json.loads(lines[0])
        return len(lines), [sample]


def read_parquet(key):
    obj = s3.get_object(Bucket=bucket, Key=key)
    table = pq.read_table(io.BytesIO(obj["Body"].read()))
    return table.num_rows, table.schema, table.slice(0, 1).to_pylist()


def inspect(prefix, kind, n):
    keys = latest(prefix, n=n)
    print(f"\n{kind} latest ({len(keys)}):")
    if not keys:
        print("  (no objects)")
        return
    for lm, key, size in keys:
        print(f"  {lm} {size} {key}")
    lm, key, size = keys[0]
    if kind == "raw":
        rows, sample = read_raw(key)
        print("  rows:", rows)
        print("  sample:", sample)
    else:
        rows, schema, sample = read_parquet(key)
        print("  rows:", rows)
        print("  schema:", schema)
        print("  sample:", sample)


def part_hash(key):
    match = re.search(r"part-([0-9a-f]{8})", key)
    return match.group(1) if match else None


def compare_latest(raw_prefix, bronze_prefix, silver_prefix, n):
    raw_keys = latest(raw_prefix, n=n)
    bronze_keys = latest(bronze_prefix, n=n)
    silver_keys = latest(silver_prefix, n=n)
    raw_map = {part_hash(k): k for _, k, _ in raw_keys if part_hash(k)}
    bronze_map = {part_hash(k): k for _, k, _ in bronze_keys if part_hash(k)}
    silver_map = {part_hash(k): k for _, k, _ in silver_keys if part_hash(k)}
    common = sorted(set(raw_map) & set(bronze_map) & set(silver_map))
    print(f"\nmatched hashes (raw/bronze/silver): {len(common)}")
    for h in common[:5]:
        r_rows, _ = read_raw(raw_map[h])
        b_rows, _, _ = read_parquet(bronze_map[h])
        s_rows, _, _ = read_parquet(silver_map[h])
        print(f"  part-{h}: raw={r_rows} bronze={b_rows} silver={s_rows}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--season", type=int, default=2024)
    parser.add_argument("--latest", type=int, default=3)
    args = parser.parse_args()
    raw_prefix = "raw/lineups/game/"
    bronze_prefix = f"bronze/lineups_game/season={args.season}/"
    silver_prefix = f"silver/fct_lineups/season={args.season}/"
    inspect(raw_prefix, "raw", args.latest)
    inspect(bronze_prefix, "bronze", args.latest)
    inspect(silver_prefix, "silver", args.latest)
    compare_latest(raw_prefix, bronze_prefix, silver_prefix, args.latest)


if __name__ == "__main__":
    main()
