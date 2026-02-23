#!/usr/bin/env python3
"""Fix Glue catalog partition keys after silver-layer dedup.

The dedup script consolidated files and removed asof= sub-partitions from
seasonal fact tables. This script detects the actual S3 layout for each
table and recreates the Glue table with the correct partition keys.

Usage:
    poetry run python scripts/fix_glue_catalog.py                  # fix all
    poetry run python scripts/fix_glue_catalog.py --dry-run        # report only
    poetry run python scripts/fix_glue_catalog.py --table fct_games  # single table
    poetry run python scripts/fix_glue_catalog.py --verify         # count rows via Athena
"""

from __future__ import annotations

import argparse
import io
import re
import sys
import time
from typing import Dict, List, Optional, Set, Tuple

import boto3
import pyarrow.parquet as pq

sys.path.insert(0, "src")

from cbbd_etl.config import load_config
from cbbd_etl.s3_io import S3IO

DATABASE = "cbbd_silver"
REGION = "us-east-1"


def _pa_to_glue(dtype) -> str:
    import pyarrow as pa

    if pa.types.is_int64(dtype):
        return "bigint"
    if pa.types.is_int32(dtype):
        return "int"
    if pa.types.is_float64(dtype) or pa.types.is_float32(dtype):
        return "double"
    if pa.types.is_boolean(dtype):
        return "boolean"
    if pa.types.is_timestamp(dtype):
        return "timestamp"
    return "string"


def detect_partition_keys(s3: S3IO, table_prefix: str) -> List[str]:
    """Detect partition key names from actual S3 layout."""
    keys = s3.list_keys(table_prefix)
    parquet_keys = [k for k in keys if k.endswith(".parquet")]

    if not parquet_keys:
        return []

    # Parse partition segments from each key
    pk_sets: Set[Tuple[str, ...]] = set()
    for key in parquet_keys:
        rel = key[len(table_prefix):]
        parts = rel.split("/")
        # Everything before the filename that contains = is a partition segment
        partitions = [p.split("=")[0] for p in parts[:-1] if "=" in p]
        pk_sets.add(tuple(partitions))

    if len(pk_sets) == 1:
        return list(pk_sets.pop())

    # Mixed layouts — use the most common
    from collections import Counter

    counts = Counter(pk_sets)
    return list(counts.most_common(1)[0][0])


def discover_partitions(
    s3: S3IO, table_prefix: str, partition_keys: List[str]
) -> List[Dict[str, str]]:
    """Discover all partition value combinations from S3 keys."""
    if not partition_keys:
        return []

    keys = s3.list_keys(table_prefix)
    parquet_keys = [k for k in keys if k.endswith(".parquet")]

    partitions: Set[Tuple[Tuple[str, str], ...]] = set()
    for key in parquet_keys:
        rel = key[len(table_prefix):]
        parts = rel.split("/")
        kv_pairs = {}
        for p in parts[:-1]:
            if "=" in p:
                k, v = p.split("=", 1)
                kv_pairs[k] = v

        if all(pk in kv_pairs for pk in partition_keys):
            combo = tuple((pk, kv_pairs[pk]) for pk in partition_keys)
            partitions.add(combo)

    return [dict(combo) for combo in sorted(partitions)]


def read_parquet_schema(s3: S3IO, table_prefix: str):
    """Read Parquet schema from the first file found."""
    keys = s3.list_keys(table_prefix)
    parquet_keys = [k for k in keys if k.endswith(".parquet")]

    if not parquet_keys:
        return None

    data = s3.get_object_bytes(parquet_keys[0])
    return pq.read_schema(io.BytesIO(data))


def get_glue_partition_keys(glue, table_name: str) -> Optional[List[str]]:
    """Get current Glue partition keys for a table."""
    try:
        resp = glue.get_table(DatabaseName=DATABASE, Name=table_name)
        return [pk["Name"] for pk in resp["Table"].get("PartitionKeys", [])]
    except glue.exceptions.EntityNotFoundException:
        return None


def fix_table(
    s3: S3IO,
    glue,
    table_name: str,
    silver_prefix: str,
    dry_run: bool,
) -> None:
    """Fix a single Glue table to match actual S3 partition layout."""
    table_prefix = f"{silver_prefix}/{table_name}/"
    bucket = s3.bucket

    # 1. Detect actual layout
    actual_pks = detect_partition_keys(s3, table_prefix)
    current_pks = get_glue_partition_keys(glue, table_name)

    if current_pks is None:
        print(f"  {table_name}: not in Glue catalog, skipping")
        return

    if current_pks == actual_pks:
        print(f"  {table_name}: OK (partition keys {actual_pks} match)")
        return

    print(
        f"  {table_name}: MISMATCH — Glue has {current_pks}, S3 has {actual_pks}"
    )

    if dry_run:
        return

    # 2. Read schema from Parquet
    schema = read_parquet_schema(s3, table_prefix)
    if schema is None:
        print(f"    no Parquet files found, skipping")
        return

    # Build column list (exclude partition key columns from data columns)
    pk_set = set(actual_pks)
    columns = []
    for field in schema:
        if field.name.lower() not in pk_set:
            columns.append({"Name": field.name, "Type": _pa_to_glue(field.type)})

    partition_key_defs = [{"Name": pk, "Type": "string"} for pk in actual_pks]
    location = f"s3://{bucket}/{table_prefix.rstrip('/')}"

    # 3. Delete old table (and its partitions)
    try:
        glue.delete_table(DatabaseName=DATABASE, Name=table_name)
        print(f"    dropped old table")
    except glue.exceptions.EntityNotFoundException:
        pass

    # 4. Create new table
    table_input = {
        "Name": table_name,
        "TableType": "EXTERNAL_TABLE",
        "Parameters": {"classification": "parquet", "EXTERNAL": "TRUE"},
        "StorageDescriptor": {
            "Columns": columns,
            "Location": location,
            "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
            "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
            "SerdeInfo": {
                "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
            },
        },
        "PartitionKeys": partition_key_defs,
    }
    glue.create_table(DatabaseName=DATABASE, TableInput=table_input)
    print(f"    created table with partition keys {actual_pks}")

    # 5. Register partitions
    if not actual_pks:
        print(f"    no partitions to register (unpartitioned table)")
        return

    combos = discover_partitions(s3, table_prefix, actual_pks)
    if not combos:
        print(f"    no partition values found")
        return

    # Batch create partitions (max 100 per call)
    partition_inputs = []
    for combo in combos:
        values = [combo[pk] for pk in actual_pks]
        partition_path = "/".join(f"{pk}={combo[pk]}" for pk in actual_pks)
        partition_inputs.append(
            {
                "Values": values,
                "StorageDescriptor": {
                    "Columns": columns,
                    "Location": f"{location}/{partition_path}",
                    "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                    "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                    "SerdeInfo": {
                        "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
                    },
                },
            }
        )

    for i in range(0, len(partition_inputs), 100):
        batch = partition_inputs[i : i + 100]
        resp = glue.batch_create_partition(
            DatabaseName=DATABASE,
            TableName=table_name,
            PartitionInputList=batch,
        )
        errors = resp.get("Errors", [])
        if errors:
            for err in errors:
                print(f"    partition error: {err}")

    print(f"    registered {len(combos)} partitions")


def verify_with_athena(athena, table_name: str) -> None:
    """Run a count query via Athena to verify the table works."""
    # Detect if table has season partition
    glue = boto3.client("glue", region_name=REGION)
    pks = get_glue_partition_keys(glue, table_name)

    if pks and "season" in pks:
        query = f'SELECT season, COUNT(*) as cnt FROM {DATABASE}.{table_name} GROUP BY season ORDER BY season'
    else:
        query = f"SELECT COUNT(*) as cnt FROM {DATABASE}.{table_name}"

    resp = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": DATABASE},
        WorkGroup="cbbd",
    )
    query_id = resp["QueryExecutionId"]

    # Poll for completion
    for _ in range(60):
        status = athena.get_query_execution(QueryExecutionId=query_id)
        state = status["QueryExecution"]["Status"]["State"]
        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(2)

    if state != "SUCCEEDED":
        reason = status["QueryExecution"]["Status"].get("StateChangeReason", "")
        print(f"  {table_name}: query {state} — {reason}")
        return

    results = athena.get_query_results(QueryExecutionId=query_id)
    rows = results["ResultSet"]["Rows"]

    print(f"  {table_name}:")
    # Print header
    header = [col["VarCharValue"] for col in rows[0]["Data"]]
    print(f"    {' | '.join(header)}")
    # Print data rows
    for row in rows[1:]:
        values = [col.get("VarCharValue", "") for col in row["Data"]]
        print(f"    {' | '.join(values)}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Fix Glue catalog after silver dedup")
    parser.add_argument("--table", help="Fix a single table")
    parser.add_argument("--dry-run", action="store_true", help="Report mismatches only")
    parser.add_argument("--verify", action="store_true", help="Verify tables via Athena")
    args = parser.parse_args()

    cfg = load_config()
    s3 = S3IO(cfg.bucket, cfg.region)
    glue = boto3.client("glue", region_name=cfg.region)
    silver_prefix = cfg.s3_layout["silver_prefix"]

    # All silver tables currently in Glue
    all_tables_resp = glue.get_tables(DatabaseName=DATABASE)
    all_table_names = [t["Name"] for t in all_tables_resp["TableList"]]

    if args.verify:
        athena = boto3.client("athena", region_name=cfg.region)
        tables = [args.table] if args.table else all_table_names
        print(f"\n=== Athena Verification ===\n")
        for table in sorted(tables):
            verify_with_athena(athena, table)
        return

    tables = [args.table] if args.table else all_table_names
    mode = "DRY RUN" if args.dry_run else "FIX"
    print(f"\n=== Glue Catalog Fix ({mode}) ===\n")

    for table in sorted(tables):
        fix_table(s3, glue, table, silver_prefix, args.dry_run)

    print("\nDone.")


if __name__ == "__main__":
    main()
