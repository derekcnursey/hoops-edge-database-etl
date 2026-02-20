from __future__ import annotations

import argparse
import gzip
import io
import json
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

from cbbd_etl.config import load_config
from cbbd_etl.extractors import build_registry
from cbbd_etl.glue_catalog import GlueCatalog
from cbbd_etl.normalize import TABLE_SPECS, dedupe_records, normalize_records
from cbbd_etl.orchestrate import (
    BRONZE_TABLES,
    RAW_PREFIX_OVERRIDES,
    SILVER_TABLES,
    _apply_key_aliases,
    _bronze_partition,
    _silver_partition,
)
from cbbd_etl.s3_io import S3IO, make_part_key


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Rebuild bronze/silver Parquet from raw JSON.gz in S3.")
    parser.add_argument("--endpoint", required=True, help="Endpoint name (e.g., games, plays_game)")
    parser.add_argument("--ingested-at", required=True, help="Raw ingest date (YYYY-MM-DD)")
    parser.add_argument("--season", type=int, help="Optional season override for partitioning")
    parser.add_argument("--date", help="Optional date override for partitioning (YYYY-MM-DD)")
    parser.add_argument("--limit-parts", type=int, help="Optional limit on number of raw parts to process")
    parser.add_argument("--keys-file", help="Optional file with raw S3 keys (one per line)")
    parser.add_argument("--filter-season", action="store_true", help="Filter records to the --season value")
    parser.add_argument("--dry-run", action="store_true", help="List actions without writing Parquet")
    parser.add_argument("--no-silver", action="store_true", help="Skip silver writes")
    parser.add_argument("--no-bronze", action="store_true", help="Skip bronze writes")
    parser.add_argument("--skip-existing", action="store_true", help="Skip writing if target Parquet already exists")
    parser.add_argument("--allow-mixed-season", action="store_true", help="Allow mixed seasons in a raw part")
    parser.add_argument(
        "--use-games-meta-from-raw",
        action="store_true",
        help="For plays_game, derive season/date from raw games ingest",
    )
    parser.add_argument(
        "--games-ingested-at",
        help="Raw games ingest date (YYYY-MM-DD) for meta lookup (defaults to --ingested-at)",
    )
    parser.add_argument(
        "--dedupe-games-across-raw",
        action="store_true",
        help="For games, dedupe across all raw parts and write one row per gameId",
    )
    return parser.parse_args()


def _iter_raw_records(blob: bytes) -> List[Dict[str, Any]]:
    records: List[Dict[str, Any]] = []
    with gzip.GzipFile(fileobj=io.BytesIO(blob), mode="rb") as gz:
        for line in gz:
            line = line.strip()
            if not line:
                continue
            records.append(json.loads(line))
    return records


def _infer_season(records: List[Dict[str, Any]]) -> Optional[int]:
    seasons: Set[int] = set()
    for rec in records:
        val = rec.get("season", rec.get("year"))
        if val is None:
            continue
        try:
            seasons.add(int(val))
        except Exception:
            continue
    if not seasons:
        return None
    if len(seasons) == 1:
        return next(iter(seasons))
    return None


def _infer_date(records: List[Dict[str, Any]]) -> Optional[str]:
    for rec in records:
        for key in ("date", "startDate", "startTime"):
            val = rec.get(key)
            if val:
                return str(val)[:10]
    return None


def _part_hash_from_key(key: str) -> str:
    name = key.rsplit("/", 1)[-1]
    if not name.startswith("part-") or not name.endswith(".json.gz"):
        return ""
    return name[len("part-") : -len(".json.gz")]


def _ensure_tables(glue: GlueCatalog, bucket: str, layout: dict, layer: str, table: str, schema, partition: str) -> None:
    db = f"cbbd_{layer}"
    glue.ensure_database(db)
    location = f"s3://{bucket}/{layout[layer + '_prefix']}/{table}/"
    partitions = [p.split("=")[0] for p in partition.split("/")]
    if partitions:
        for p in partitions:
            if p in schema.names:
                idx = schema.get_field_index(p)
                if idx >= 0:
                    schema = schema.remove(idx)
    glue.ensure_table(db, table, location, schema, partitions)


def _build_game_meta_from_raw(s3: S3IO, cfg, ingested_at: str) -> Dict[int, Tuple[Optional[int], Optional[str]]]:
    raw_prefix = make_part_key(cfg.s3_layout["raw_prefix"], "games", f"ingested_at={ingested_at}")
    keys = [k for k in s3.list_keys(raw_prefix) if k.endswith(".json.gz")]
    keys.sort()
    meta: Dict[int, Tuple[Optional[int], Optional[str]]] = {}
    for key in keys:
        blob = s3.get_object_bytes(key)
        records = _iter_raw_records(blob)
        for rec in records:
            gid = rec.get("id") or rec.get("gameId")
            if gid is None:
                continue
            try:
                gid = int(gid)
            except Exception:
                continue
            season = rec.get("season", rec.get("year"))
            try:
                season = int(season) if season is not None else None
            except Exception:
                season = None
            date = None
            for k in ("date", "startDate", "startTime"):
                val = rec.get(k)
                if val:
                    date = str(val)[:10]
                    break
            meta[gid] = (season, date)
    return meta


def _stable_part_name(seed: str) -> str:
    import hashlib

    return hashlib.sha256(seed.encode("utf-8")).hexdigest()[:8]


def _load_keys_file(path: str, bucket: str) -> list[str]:
    keys: list[str] = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if line.startswith("s3://"):
                parts = line[5:].split("/", 1)
                if len(parts) == 2:
                    line_bucket, key = parts
                    if line_bucket != bucket:
                        continue
                    keys.append(key)
            else:
                keys.append(line)
    return keys


def main() -> None:
    args = _parse_args()
    cfg = load_config()

    if args.endpoint not in cfg.endpoints:
        raise ValueError(f"Unknown endpoint: {args.endpoint}")
    if args.no_bronze and args.no_silver:
        raise ValueError("Nothing to do: both bronze and silver are disabled")

    registry = build_registry(cfg.endpoints)
    if args.endpoint not in registry:
        raise ValueError(f"Unknown endpoint: {args.endpoint}")
    spec = registry[args.endpoint]
    s3 = S3IO(cfg.bucket, cfg.region)
    glue = GlueCatalog(cfg.region)

    raw_endpoint = RAW_PREFIX_OVERRIDES.get(args.endpoint, args.endpoint)
    raw_prefix = make_part_key(cfg.s3_layout["raw_prefix"], raw_endpoint, f"ingested_at={args.ingested_at}")
    if args.keys_file:
        keys = _load_keys_file(args.keys_file, cfg.bucket)
    else:
        keys = [k for k in s3.list_keys(raw_prefix) if k.endswith(".json.gz")]
        keys.sort()

    if args.limit_parts:
        keys = keys[: args.limit_parts]

    if not keys:
        if args.keys_file:
            print(f"No raw keys found in {args.keys_file}")
        else:
            print(f"No raw objects found under s3://{cfg.bucket}/{raw_prefix}/")
        return

    if args.dedupe_games_across_raw and args.endpoint != "games":
        raise ValueError("--dedupe-games-across-raw is only supported for games")

    game_meta: Dict[int, Tuple[Optional[int], Optional[str]]] = {}
    if args.use_games_meta_from_raw:
        if args.endpoint != "plays_game":
            raise ValueError("--use-games-meta-from-raw is only supported for plays_game")
        games_ingested_at = args.games_ingested_at or args.ingested_at
        print(f"loading game meta from raw games ingested_at={games_ingested_at}")
        game_meta = _build_game_meta_from_raw(s3, cfg, games_ingested_at)
        print(f"loaded game meta rows={len(game_meta)}")

    if args.dedupe_games_across_raw:
        all_records: Dict[int, Dict[str, Any]] = {}
        for key in keys:
            blob = s3.get_object_bytes(key)
            records = _iter_raw_records(blob)
            for rec in records:
                gid = rec.get("id") or rec.get("gameId")
                if gid is None:
                    continue
                try:
                    gid = int(gid)
                except Exception:
                    continue
                if args.filter_season and args.season is not None:
                    season_val = rec.get("season", rec.get("year"))
                    if season_val != args.season:
                        continue
                all_records[gid] = rec

        if not all_records:
            print("no records after dedupe/filter")
            return

        grouped: Dict[Tuple[int, str], List[Dict[str, Any]]] = {}
        for rec in all_records.values():
            season = args.season if args.season is not None else _infer_season([rec])
            if season is None:
                season_val = rec.get("season", rec.get("year"))
                season = int(season_val) if season_val is not None else None
            if season is None:
                continue
            date = args.date or _infer_date([rec]) or args.ingested_at
            grouped.setdefault((season, date), []).append(rec)

        for (season, date), records in sorted(grouped.items()):
            ingested_at = args.ingested_at
            if not args.no_bronze:
                bronze_table = BRONZE_TABLES[args.endpoint]
                bronze_partition = _bronze_partition(spec, season=season, date=None, asof=ingested_at)
                part_name = _stable_part_name(f"{season}:{date}:{ingested_at}")
                bronze_key = make_part_key(
                    cfg.s3_layout["bronze_prefix"],
                    bronze_table,
                    bronze_partition,
                    f"part-{part_name}.parquet",
                )
                bronze_table_pa = normalize_records(bronze_table, records)
                if not args.dry_run:
                    if args.skip_existing and s3.exists(bronze_key):
                        print(f"skip: bronze exists s3://{cfg.bucket}/{bronze_key}")
                    else:
                        s3.put_parquet(bronze_key, bronze_table_pa)
                        _ensure_tables(glue, cfg.bucket, cfg.s3_layout, "bronze", bronze_table, bronze_table_pa.schema, bronze_partition)
                print(f"bronze: s3://{cfg.bucket}/{bronze_key} rows={bronze_table_pa.num_rows}")

            if not args.no_silver and args.endpoint in SILVER_TABLES:
                silver_table = SILVER_TABLES[args.endpoint]
                records_s = _apply_key_aliases(silver_table, records)
                spec_def = TABLE_SPECS.get(silver_table)
                if spec_def:
                    records_s = dedupe_records(records_s, spec_def.primary_keys)
                silver_partition = _silver_partition(silver_table, season=season, date=date, asof=ingested_at)
                part_name = _stable_part_name(f"{season}:{date}:{ingested_at}")
                silver_key = make_part_key(
                    cfg.s3_layout["silver_prefix"],
                    silver_table,
                    silver_partition,
                    f"part-{part_name}.parquet",
                )
                silver_table_pa = normalize_records(silver_table, records_s)
                if not args.dry_run:
                    if args.skip_existing and s3.exists(silver_key):
                        print(f"skip: silver exists s3://{cfg.bucket}/{silver_key}")
                    else:
                        s3.put_parquet(silver_key, silver_table_pa)
                        _ensure_tables(glue, cfg.bucket, cfg.s3_layout, "silver", silver_table, silver_table_pa.schema, silver_partition)
                print(f"silver: s3://{cfg.bucket}/{silver_key} rows={silver_table_pa.num_rows}")

        print(f"done: parts={len(grouped)} games={len(all_records)}")
        return

    total = 0
    for key in keys:
        part_hash = _part_hash_from_key(key)
        if not part_hash:
            print(f"skip: unrecognized key {key}")
            continue

        blob = s3.get_object_bytes(key)
        records = _iter_raw_records(blob)
        if args.filter_season and args.season is not None:
            target = str(args.season)
            filtered: List[Dict[str, Any]] = []
            for r in records:
                val = r.get("season", r.get("year"))
                if val is None:
                    continue
                if str(val) == target:
                    filtered.append(r)
            records = filtered

        if not records:
            print(f"skip: empty records for {key}")
            continue

        if args.season is not None and args.filter_season:
            grouped_records = {args.season: records}
        else:
            grouped_records: Dict[int, List[Dict[str, Any]]] = {}
            for rec in records:
                val = rec.get("season", rec.get("year"))
                if val is None:
                    continue
                try:
                    season_val = int(val)
                except Exception:
                    continue
                grouped_records.setdefault(season_val, []).append(rec)
            if not grouped_records:
                print(f"skip: no season values in {key}")
                continue
            if args.allow_mixed_season:
                inferred = _infer_season(records)
                if inferred is not None:
                    grouped_records = {inferred: records}

        ingested_at = args.ingested_at

        for season, season_records in sorted(grouped_records.items()):
            if not season_records:
                continue
            date = args.date or _infer_date(season_records)
            if game_meta:
                sample_gid = None
                for rec in season_records:
                    gid = rec.get("gameId") or rec.get("id")
                    if gid is None:
                        continue
                    try:
                        sample_gid = int(gid)
                    except Exception:
                        continue
                    break
                if sample_gid is not None and sample_gid in game_meta:
                    meta_season, meta_date = game_meta[sample_gid]
                    if args.season is None and meta_season is not None:
                        season = meta_season
                    if args.date is None and meta_date is not None:
                        date = meta_date

            if not args.no_bronze:
                bronze_table = BRONZE_TABLES[args.endpoint]
                bronze_partition = _bronze_partition(spec, season=season, date=date, asof=ingested_at)
                bronze_key = make_part_key(
                    cfg.s3_layout["bronze_prefix"],
                    bronze_table,
                    bronze_partition,
                    f"part-{part_hash}.parquet",
                )
                bronze_table_pa = normalize_records(bronze_table, season_records)
                if not args.dry_run:
                    if args.skip_existing and s3.exists(bronze_key):
                        print(f"skip: bronze exists s3://{cfg.bucket}/{bronze_key}")
                    else:
                        s3.put_parquet(bronze_key, bronze_table_pa)
                        _ensure_tables(glue, cfg.bucket, cfg.s3_layout, "bronze", bronze_table, bronze_table_pa.schema, bronze_partition)
                print(f"bronze: s3://{cfg.bucket}/{bronze_key} rows={bronze_table_pa.num_rows}")

            if not args.no_silver and args.endpoint in SILVER_TABLES:
                silver_table = SILVER_TABLES[args.endpoint]
                records_s = _apply_key_aliases(silver_table, season_records)
                spec_def = TABLE_SPECS.get(silver_table)
                if spec_def:
                    records_s = dedupe_records(records_s, spec_def.primary_keys)
                silver_partition = _silver_partition(silver_table, season=season, date=date, asof=ingested_at)
                silver_key = make_part_key(
                    cfg.s3_layout["silver_prefix"],
                    silver_table,
                    silver_partition,
                    f"part-{part_hash}.parquet",
                )
                silver_table_pa = normalize_records(silver_table, records_s)
                if not args.dry_run:
                    if args.skip_existing and s3.exists(silver_key):
                        print(f"skip: silver exists s3://{cfg.bucket}/{silver_key}")
                    else:
                        s3.put_parquet(silver_key, silver_table_pa)
                        _ensure_tables(glue, cfg.bucket, cfg.s3_layout, "silver", silver_table, silver_table_pa.schema, silver_partition)
                print(f"silver: s3://{cfg.bucket}/{silver_key} rows={silver_table_pa.num_rows}")

        total += 1

    print(f"done: parts={total}")


if __name__ == "__main__":
    main()
