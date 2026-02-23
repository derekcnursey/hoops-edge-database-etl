#!/usr/bin/env python3
"""Deduplicate silver-layer Parquet files in S3.

Removes duplicate rows caused by overlapping asof= partitions from
multiple pipeline runs.

Usage:
    # Dry-run a single table
    poetry run python scripts/deduplicate_silver.py --table fct_games --season 2025 --dry-run

    # Deduplicate all tables for a season
    poetry run python scripts/deduplicate_silver.py --all --season 2025

    # Deduplicate a single table
    poetry run python scripts/deduplicate_silver.py --table fct_games --season 2025
"""

from __future__ import annotations

import argparse
import io
import sys
from typing import Dict, List, Set

import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, "src")

from cbbd_etl.config import load_config
from cbbd_etl.normalize import TABLE_SPECS
from cbbd_etl.s3_io import S3IO
from cbbd_etl.utils import stable_hash

# ---------------------------------------------------------------------------
# Table categories
# ---------------------------------------------------------------------------

DIMENSION_TABLES = {
    "dim_teams",
    "dim_conferences",
    "dim_venues",
    "dim_lines_providers",
    "dim_play_types",
}

ASOF_ONLY_TABLES = {
    "fct_ratings_adjusted",
    "fct_ratings_srs",
}

DAILY_SNAPSHOT_TABLES = {
    "fct_pbp_game_team_stats",
    "fct_pbp_plays_enriched",
    "fct_pbp_game_teams_flat",
    "fct_pbp_game_teams_flat_garbage_removed",
    "fct_pbp_team_daily_rollup",
    "fct_pbp_team_daily_rollup_garbage_removed",
    "fct_pbp_team_daily_rollup_adj",
    "fct_pbp_team_daily_rollup_adj_garbage_removed",
}

GOLD_TABLES = {
    "team_power_rankings",
    "game_predictions_features",
    "player_season_impact",
    "market_lines_analysis",
    "team_season_summary",
}


def classify_table(name: str) -> str:
    if name in DIMENSION_TABLES:
        return "dimension"
    if name in ASOF_ONLY_TABLES:
        return "asof_only"
    if name in DAILY_SNAPSHOT_TABLES:
        return "daily_snapshot"
    return "seasonal_fact"


def silver_table_names() -> List[str]:
    return [t for t in TABLE_SPECS if t not in GOLD_TABLES]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _concat_with_unified_schema(tables: List[pa.Table]) -> pa.Table:
    try:
        return pa.concat_tables(tables, promote_options="permissive")
    except (pa.ArrowInvalid, pa.ArrowTypeError):
        col_types: Dict[str, pa.DataType] = {}
        for tbl in tables:
            for field in tbl.schema:
                existing = col_types.get(field.name)
                if existing is None:
                    col_types[field.name] = field.type
                elif existing != field.type:
                    numeric = {
                        pa.int8(), pa.int16(), pa.int32(), pa.int64(),
                        pa.float16(), pa.float32(), pa.float64(),
                    }
                    if existing in numeric and field.type in numeric:
                        col_types[field.name] = pa.float64()
                    else:
                        col_types[field.name] = pa.string()

        unified = pa.schema([(n, t) for n, t in col_types.items()])
        aligned: List[pa.Table] = []
        for tbl in tables:
            columns = {}
            for f in unified:
                if f.name in tbl.column_names:
                    col = tbl.column(f.name)
                    if col.type != f.type:
                        col = col.cast(f.type, safe=False)
                    columns[f.name] = col
                else:
                    columns[f.name] = pa.nulls(tbl.num_rows, type=f.type)
            aligned.append(pa.table(columns))
        return pa.concat_tables(aligned)


def _dedup_table(table: pa.Table, key_cols: List[str]) -> pa.Table:
    if table.num_rows == 0:
        return table
    missing = [c for c in key_cols if c not in table.column_names]
    if missing:
        print(f"  WARNING: key columns {missing} missing, skipping dedup")
        return table
    seen: Set = set()
    indices: List[int] = []
    key_data = [table.column(c).to_pylist() for c in key_cols]
    for i in range(table.num_rows):
        key = tuple(key_data[j][i] for j in range(len(key_cols)))
        if key not in seen:
            seen.add(key)
            indices.append(i)
    return table.take(indices)


def _read_parquet_files(s3: S3IO, keys: List[str]) -> pa.Table:
    tables: List[pa.Table] = []
    for key in keys:
        data = s3.get_object_bytes(key)
        tbl = pq.read_table(io.BytesIO(data))
        tables.append(tbl)
    if not tables:
        return pa.table({})
    return _concat_with_unified_schema(tables)


def _write_parquet(s3: S3IO, key: str, table: pa.Table) -> None:
    sink = io.BytesIO()
    pq.write_table(table, sink, compression="snappy")
    sink.seek(0)
    s3._put_with_retry(key, sink.read())


def _discover_date_partitions(keys: List[str]) -> Dict[str, List[str]]:
    partitions: Dict[str, List[str]] = {}
    for key in keys:
        parts = key.split("/")
        date_val = None
        for part in parts:
            if part.startswith("date="):
                date_val = part
                break
        if date_val:
            partitions.setdefault(date_val, []).append(key)
    return partitions


# ---------------------------------------------------------------------------
# Per-category dedup logic
# ---------------------------------------------------------------------------


def dedup_seasonal_fact(
    s3: S3IO, silver_prefix: str, table: str, season: int, dry_run: bool
) -> None:
    spec = TABLE_SPECS[table]
    prefix = f"{silver_prefix}/{table}/season={season}/"
    keys = [k for k in s3.list_keys(prefix) if k.endswith(".parquet")]

    if not keys:
        print(f"  {table}/season={season}: no parquet files found")
        return

    combined = _read_parquet_files(s3, keys)
    before = combined.num_rows
    deduped = _dedup_table(combined, list(spec.primary_keys))
    after = deduped.num_rows
    removed = before - after

    print(f"  {table}/season={season}: {len(keys)} files, {before} rows -> {after} rows ({removed} dupes)")

    if dry_run or (removed == 0 and len(keys) == 1):
        return

    content_hash = stable_hash({"table": table, "season": season, "rows": after})[:8]
    out_key = f"{silver_prefix}/{table}/season={season}/part-{content_hash}.parquet"
    _write_parquet(s3, out_key, deduped)

    to_delete = [k for k in keys if k != out_key]
    if to_delete:
        s3.delete_keys(to_delete)
        print(f"    wrote {out_key}, deleted {len(to_delete)} old files")


def dedup_dimension_or_asof(
    s3: S3IO, silver_prefix: str, table: str, dry_run: bool
) -> None:
    spec = TABLE_SPECS[table]
    prefix = f"{silver_prefix}/{table}/"
    keys = [k for k in s3.list_keys(prefix) if k.endswith(".parquet")]

    if not keys:
        print(f"  {table}: no parquet files found")
        return

    combined = _read_parquet_files(s3, keys)
    before = combined.num_rows
    deduped = _dedup_table(combined, list(spec.primary_keys))
    after = deduped.num_rows
    removed = before - after

    print(f"  {table}: {len(keys)} files, {before} rows -> {after} rows ({removed} dupes)")

    if dry_run or (removed == 0 and len(keys) == 1):
        return

    content_hash = stable_hash({"table": table, "rows": after})[:8]
    out_key = f"{silver_prefix}/{table}/part-{content_hash}.parquet"
    _write_parquet(s3, out_key, deduped)

    to_delete = [k for k in keys if k != out_key]
    if to_delete:
        s3.delete_keys(to_delete)
        print(f"    wrote {out_key}, deleted {len(to_delete)} old files")


def dedup_daily_snapshot(
    s3: S3IO, silver_prefix: str, table: str, season: int, dry_run: bool
) -> None:
    spec = TABLE_SPECS[table]
    prefix = f"{silver_prefix}/{table}/season={season}/"
    keys = [k for k in s3.list_keys(prefix) if k.endswith(".parquet")]

    if not keys:
        print(f"  {table}/season={season}: no parquet files found")
        return

    date_groups = _discover_date_partitions(keys)
    total_removed = 0

    for date_part, date_keys in sorted(date_groups.items()):
        combined = _read_parquet_files(s3, date_keys)
        before = combined.num_rows
        deduped = _dedup_table(combined, list(spec.primary_keys))
        after = deduped.num_rows
        removed = before - after
        total_removed += removed

        if not dry_run and (removed > 0 or len(date_keys) > 1):
            content_hash = stable_hash(
                {"table": table, "season": season, "date": date_part, "rows": after}
            )[:8]
            out_key = f"{silver_prefix}/{table}/season={season}/{date_part}/part-{content_hash}.parquet"
            _write_parquet(s3, out_key, deduped)

            to_delete = [k for k in date_keys if k != out_key]
            if to_delete:
                s3.delete_keys(to_delete)

    print(
        f"  {table}/season={season}: {len(keys)} files across {len(date_groups)} dates, "
        f"{total_removed} total dupes removed"
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(description="Deduplicate silver-layer Parquet files")
    parser.add_argument("--table", help="Single table to process")
    parser.add_argument("--season", type=int, help="Season to process")
    parser.add_argument("--all", action="store_true", help="Process all silver tables")
    parser.add_argument("--dry-run", action="store_true", help="Report only, no writes")
    args = parser.parse_args()

    if not args.table and not args.all:
        parser.error("Specify --table TABLE or --all")

    cfg = load_config()
    s3 = S3IO(cfg.bucket, cfg.region)
    silver_prefix = cfg.s3_layout["silver_prefix"]

    tables = [args.table] if args.table else silver_table_names()
    mode = "DRY RUN" if args.dry_run else "DEDUP"
    print(f"\n=== Silver Layer Dedup ({mode}) ===\n")

    for table in tables:
        if table not in TABLE_SPECS:
            print(f"  {table}: not in TABLE_SPECS, skipping")
            continue

        category = classify_table(table)

        if category == "seasonal_fact":
            if not args.season:
                print(f"  {table}: --season required for seasonal fact, skipping")
                continue
            dedup_seasonal_fact(s3, silver_prefix, table, args.season, args.dry_run)

        elif category in ("dimension", "asof_only"):
            dedup_dimension_or_asof(s3, silver_prefix, table, args.dry_run)

        elif category == "daily_snapshot":
            if not args.season:
                print(f"  {table}: --season required for daily snapshot, skipping")
                continue
            dedup_daily_snapshot(s3, silver_prefix, table, args.season, args.dry_run)

    print("\nDone.")


if __name__ == "__main__":
    main()
