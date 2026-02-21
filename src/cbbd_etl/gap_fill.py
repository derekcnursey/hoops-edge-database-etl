"""Generic gap-fill framework for fan-out endpoints.

Pattern: Athena/S3 discovery → missing ID list → targeted API calls → write raw/bronze/silver.

Supports: plays_game, substitutions_game, lineups_game (any game_fanout endpoint).

Usage:
    poetry run python -m cbbd_etl.gap_fill --endpoint plays_game --season 2024
    poetry run python -m cbbd_etl.gap_fill --endpoint plays_game --discover --season 2024
    poetry run python -m cbbd_etl.gap_fill --endpoint plays_game --missing-ids-file ids.txt --season 2024
"""
from __future__ import annotations

import argparse
import asyncio
import io
import json
import os
import subprocess
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import httpx
import pyarrow.parquet as pq

from .checkpoint import CheckpointStore
from .config import Config, get_api_token, load_config
from .extractors import build_registry
from .glue_catalog import GlueCatalog
from .normalize import TABLE_SPECS, dedupe_records, normalize_records
from .orchestrate import (
    BRONZE_TABLES,
    RAW_PREFIX_OVERRIDES,
    SILVER_TABLES,
    _apply_key_aliases,
    _bronze_partition,
    _silver_partition,
)
from .s3_io import S3IO, make_part_key
from .utils import stable_hash

# Athena configuration
ATHENA_DB = "cbbd_silver"
ATHENA_WORKGROUP = "cbbd"
ATHENA_OUTPUT = "s3://hoops-edge/athena/"

# Map endpoint names to their silver target table for discovery queries
_FANOUT_TARGET_TABLE: Dict[str, str] = {
    "plays_game": "fct_plays",
    "substitutions_game": "fct_substitutions",
    "lineups_game": "fct_lineups",
}


def discover_missing_game_ids(
    endpoint: str,
    season: int,
    use_athena: bool = True,
) -> List[Tuple[int, Optional[str]]]:
    """Discover game IDs missing data for a given fan-out endpoint and season.

    Args:
        endpoint: Fan-out endpoint name (plays_game, substitutions_game, lineups_game).
        season: Season year to check.
        use_athena: If True, query Athena. If False, return empty (caller provides IDs).

    Returns:
        List of (game_id, game_date) tuples for games missing data.
    """
    if not use_athena:
        return []

    target_table = _FANOUT_TARGET_TABLE.get(endpoint)
    if not target_table:
        raise ValueError(f"No discovery query defined for endpoint: {endpoint}")

    query = f"""
    SELECT
        g."gameId",
        SUBSTR(CAST(g."startDate" AS VARCHAR), 1, 10) AS game_date
    FROM fct_games g
    LEFT JOIN (SELECT DISTINCT "gameId" FROM {target_table}) t
        ON g."gameId" = t."gameId"
    WHERE g.season = {season} AND t."gameId" IS NULL
    ORDER BY g."gameId"
    """

    qid = _athena_start(query)
    state, meta = _athena_wait(qid)
    if state != "SUCCEEDED":
        reason = meta["QueryExecution"]["Status"].get("StateChangeReason", "unknown")
        raise RuntimeError(f"Athena query failed: {reason}")

    rows = _athena_rows(qid)
    result: List[Tuple[int, Optional[str]]] = []
    for row in rows:
        if not row or not row[0]:
            continue
        game_id = int(row[0])
        game_date = row[1] if len(row) > 1 else None
        result.append((game_id, game_date))
    return result


def discover_missing_from_s3(
    endpoint: str,
    season: int,
    cfg: Config,
) -> List[Tuple[int, Optional[str]]]:
    """Discover missing game IDs by comparing S3 data (no Athena needed).

    Reads game IDs from fct_games silver Parquet and compares against
    the target table's Parquet files.

    Args:
        endpoint: Fan-out endpoint name.
        season: Season year to check.
        cfg: Pipeline config.

    Returns:
        List of (game_id, game_date) tuples for games missing data.
    """
    s3 = S3IO(cfg.bucket, cfg.region)
    silver_prefix = cfg.s3_layout["silver_prefix"]

    # Load all game IDs from fct_games
    games_prefix = f"{silver_prefix}/fct_games/season={season}/"
    game_ids: Dict[int, Optional[str]] = {}
    for key in s3.list_keys(games_prefix):
        if not key.endswith(".parquet"):
            continue
        try:
            data = s3.get_object_bytes(key)
            table = pq.read_table(io.BytesIO(data))
        except Exception:
            continue
        cols = set(table.schema.names)
        id_col = "gameId" if "gameId" in cols else ("id" if "id" in cols else None)
        if not id_col:
            continue
        ids = table.column(id_col).to_pylist()
        dates = table.column("startDate").to_pylist() if "startDate" in cols else [None] * len(ids)
        for gid, d in zip(ids, dates):
            if gid is not None:
                game_ids[int(gid)] = str(d)[:10] if d else None

    # Load game IDs that already have data in the target table
    target_table = _FANOUT_TARGET_TABLE.get(endpoint)
    if not target_table:
        raise ValueError(f"No target table for endpoint: {endpoint}")

    target_prefix = f"{silver_prefix}/{target_table}/season={season}/"
    existing_ids: Set[int] = set()
    for key in s3.list_keys(target_prefix):
        if not key.endswith(".parquet"):
            continue
        try:
            data = s3.get_object_bytes(key)
            table = pq.read_table(io.BytesIO(data), columns=["gameId"] if "gameId" in pq.read_schema(io.BytesIO(data)).names else None)
        except Exception:
            continue
        if "gameId" in table.schema.names:
            for gid in table.column("gameId").to_pylist():
                if gid is not None:
                    existing_ids.add(int(gid))

    # Compute difference
    missing = [(gid, game_ids[gid]) for gid in sorted(game_ids.keys()) if gid not in existing_ids]
    return missing


def load_missing_ids_file(path: str) -> List[Tuple[int, Optional[str]]]:
    """Load missing game IDs from a file (one per line, optionally with date).

    File format: one game ID per line, or 'gameId,date' per line.

    Args:
        path: Path to the file.

    Returns:
        List of (game_id, game_date) tuples.
    """
    result: List[Tuple[int, Optional[str]]] = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            parts = line.split(",", 1)
            try:
                game_id = int(parts[0].strip())
            except ValueError:
                continue
            game_date = parts[1].strip() if len(parts) > 1 else None
            result.append((game_id, game_date))
    return result


async def fill_gaps(
    endpoint: str,
    missing: List[Tuple[int, Optional[str]]],
    cfg: Config,
    season: int,
    dry_run: bool = False,
    concurrency: int = 10,
    batch_size: int = 100,
    resume_file: Optional[str] = None,
    mark_empty: bool = False,
    log_every: int = 25,
    use_checkpoints: bool = True,
) -> Dict[str, int]:
    """Fill data gaps for a fan-out endpoint by fetching missing game IDs from the API.

    Args:
        endpoint: Fan-out endpoint name (e.g., plays_game).
        missing: List of (game_id, game_date) tuples to fetch.
        cfg: Pipeline config.
        season: Season for partitioning.
        dry_run: If True, fetch but don't write to S3.
        concurrency: Max concurrent API requests.
        batch_size: Batch size for concurrent fetching.
        resume_file: Path to resume file for tracking completed IDs.
        mark_empty: If True, mark empty API responses as done in resume file.
        log_every: Log progress every N games.
        use_checkpoints: If True, use DynamoDB checkpoints for dedup.

    Returns:
        Dict with counts: written, empty, errors, skipped.
    """
    registry = build_registry(cfg.endpoints)
    if endpoint not in registry:
        raise ValueError(f"Unknown endpoint: {endpoint}")
    spec = registry[endpoint]
    s3 = S3IO(cfg.bucket, cfg.region)
    glue = GlueCatalog(cfg.region)
    checkpoints = CheckpointStore(cfg.region) if use_checkpoints else None

    token = get_api_token()
    raw_endpoint = RAW_PREFIX_OVERRIDES.get(endpoint, endpoint)
    ingested_at = datetime.utcnow().date().isoformat()

    # Load resume state
    resume_path = Path(resume_file) if resume_file else Path("tmp") / f"gap_fill_{endpoint}_{season}.txt"
    done_ids: Set[int] = set()
    if resume_path.exists():
        for line in resume_path.read_text().splitlines():
            line = line.strip()
            if line.isdigit():
                done_ids.add(int(line))

    # Filter already-done IDs
    pending = [(gid, gdate) for gid, gdate in missing if gid not in done_ids]

    stats = {"written": 0, "empty": 0, "errors": 0, "skipped": len(missing) - len(pending)}
    if not pending:
        return stats

    resume_path.parent.mkdir(parents=True, exist_ok=True)
    bronze_registered = False
    silver_registered = False
    sem = asyncio.Semaphore(max(1, concurrency))

    async with httpx.AsyncClient(
        base_url=cfg.api["base_url"],
        headers={"Authorization": f"Bearer {token}"},
        timeout=cfg.api.get("timeout_seconds", 30),
    ) as client:
        for batch_start in range(0, len(pending), batch_size):
            batch = pending[batch_start : batch_start + batch_size]
            tasks = [
                _fetch_one(client, sem, gid, gdate, cfg.api)
                for gid, gdate in batch
            ]
            results = await asyncio.gather(*tasks)

            for game_id, game_date, records, err in results:
                if err:
                    stats["errors"] += 1
                    continue
                if not records:
                    stats["empty"] += 1
                    if mark_empty and not dry_run:
                        _append_resume(resume_path, game_id)
                    continue

                params = {"gameId": game_id}
                payload_hash = stable_hash(params)

                if not dry_run:
                    # Raw layer
                    raw_key = make_part_key(
                        cfg.s3_layout["raw_prefix"],
                        raw_endpoint,
                        f"ingested_at={ingested_at}",
                        f"part-{payload_hash[:8]}.json.gz",
                    )
                    s3.put_json_gz(raw_key, records)

                    # Bronze layer
                    bronze_table = BRONZE_TABLES[endpoint]
                    bronze_partition = _bronze_partition(spec, season=season, date=game_date, asof=ingested_at)
                    bronze_key = make_part_key(
                        cfg.s3_layout["bronze_prefix"],
                        bronze_table,
                        bronze_partition,
                        f"part-{payload_hash[:8]}.parquet",
                    )
                    bronze_pa = normalize_records(bronze_table, records)
                    s3.put_parquet(bronze_key, bronze_pa)
                    if not bronze_registered:
                        _ensure_glue(glue, cfg, "bronze", bronze_table, bronze_pa.schema, bronze_partition)
                        bronze_registered = True

                    # Silver layer
                    if endpoint in SILVER_TABLES:
                        silver_table = SILVER_TABLES[endpoint]
                        records_s = _apply_key_aliases(silver_table, records)
                        spec_def = TABLE_SPECS.get(silver_table)
                        if spec_def:
                            records_s = dedupe_records(records_s, spec_def.primary_keys)
                        silver_partition = _silver_partition(silver_table, season=season, date=game_date, asof=ingested_at)
                        silver_key = make_part_key(
                            cfg.s3_layout["silver_prefix"],
                            silver_table,
                            silver_partition,
                            f"part-{payload_hash[:8]}.parquet",
                        )
                        silver_pa = normalize_records(silver_table, records_s)
                        s3.put_parquet(silver_key, silver_pa)
                        if not silver_registered:
                            _ensure_glue(glue, cfg, "silver", silver_table, silver_pa.schema, silver_partition)
                            silver_registered = True

                    _append_resume(resume_path, game_id)

                    if checkpoints:
                        checkpoints.put(
                            f"gap_fill:{endpoint}",
                            payload_hash,
                            {"game_id": game_id, "season": season, "ingested_at": ingested_at},
                        )

                stats["written"] += 1

            processed = batch_start + len(batch)
            if processed % max(1, log_every) == 0 or processed == len(pending):
                print(
                    f"[gap_fill:{endpoint}] {processed}/{len(pending)} "
                    f"written={stats['written']} empty={stats['empty']} errors={stats['errors']}"
                )

    return stats


async def _fetch_one(
    client: httpx.AsyncClient,
    sem: asyncio.Semaphore,
    game_id: int,
    game_date: Optional[str],
    api_cfg: Dict[str, Any],
) -> Tuple[int, Optional[str], List[Dict[str, Any]], Optional[str]]:
    """Fetch data for a single game ID with retry logic.

    Args:
        client: httpx async client.
        sem: Concurrency semaphore.
        game_id: The game ID to fetch.
        game_date: Game date for context.
        api_cfg: API configuration dict.

    Returns:
        Tuple of (game_id, game_date, records, error_string_or_none).
    """
    max_attempts = api_cfg.get("retry", {}).get("max_attempts", 5)
    base_delay = api_cfg.get("retry", {}).get("base_delay_seconds", 0.5)
    max_delay = api_cfg.get("retry", {}).get("max_delay_seconds", 8.0)

    async with sem:
        for attempt in range(1, max_attempts + 1):
            try:
                resp = await client.get(f"/plays/game/{game_id}")
            except (httpx.ReadTimeout, httpx.RequestError) as exc:
                if attempt >= max_attempts:
                    return game_id, game_date, [], f"error:{type(exc).__name__}"
                await asyncio.sleep(min(max_delay, base_delay * (2 ** (attempt - 1))))
                continue

            if resp.status_code == 200:
                data = resp.json()
                records = data if isinstance(data, list) else (data.get("data", [data]) if isinstance(data, dict) else [])
                return game_id, game_date, records, None
            if resp.status_code in (429, 500, 502, 503, 504):
                if attempt >= max_attempts:
                    return game_id, game_date, [], f"status:{resp.status_code}"
                retry_after = resp.headers.get("Retry-After")
                delay = min(max_delay, float(retry_after)) if retry_after else min(max_delay, base_delay * (2 ** (attempt - 1)))
                await asyncio.sleep(delay)
                continue
            return game_id, game_date, [], f"status:{resp.status_code}"

    return game_id, game_date, [], "error:unreachable"


def _ensure_glue(
    glue: GlueCatalog,
    cfg: Config,
    layer: str,
    table: str,
    schema: Any,
    partition: str,
) -> None:
    """Register or update a Glue table for the given layer."""
    db = f"cbbd_{layer}"
    glue.ensure_database(db)
    location = f"s3://{cfg.bucket}/{cfg.s3_layout[layer + '_prefix']}/{table}/"
    partitions = [p.split("=")[0] for p in partition.split("/")]
    for p in partitions:
        if p in schema.names:
            idx = schema.get_field_index(p)
            if idx >= 0:
                schema = schema.remove(idx)
    glue.ensure_table(db, table, location, schema, partitions)


def _append_resume(path: Path, game_id: int) -> None:
    """Append a game ID to the resume file."""
    with path.open("a", encoding="utf-8") as f:
        f.write(f"{game_id}\n")


def _athena_start(query: str) -> str:
    """Start an Athena query execution."""
    payload = {
        "QueryString": query,
        "QueryExecutionContext": {"Database": ATHENA_DB},
        "ResultConfiguration": {"OutputLocation": ATHENA_OUTPUT},
        "WorkGroup": ATHENA_WORKGROUP,
    }
    out = subprocess.check_output(
        ["aws", "athena", "start-query-execution", "--cli-input-json", json.dumps(payload)]
    )
    return json.loads(out)["QueryExecutionId"]


def _athena_wait(qid: str) -> Tuple[str, Dict[str, Any]]:
    """Poll Athena until query completes."""
    while True:
        out = subprocess.check_output(["aws", "athena", "get-query-execution", "--query-execution-id", qid])
        data = json.loads(out)
        state = data["QueryExecution"]["Status"]["State"]
        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            return state, data
        time.sleep(1)


def _athena_rows(qid: str) -> List[List[Optional[str]]]:
    """Fetch all result rows from an Athena query."""
    rows: List[List[Optional[str]]] = []
    token: Optional[str] = None
    first = True
    while True:
        args = ["athena", "get-query-results", "--query-execution-id", qid, "--max-results", "1000"]
        if token:
            args += ["--next-token", token]
        out = subprocess.check_output(["aws"] + args)
        data = json.loads(out)
        result_rows = data["ResultSet"]["Rows"]
        if first:
            result_rows = result_rows[1:]  # skip header
            first = False
        for r in result_rows:
            rows.append([c.get("VarCharValue") for c in r["Data"]])
        token = data.get("NextToken")
        if not token:
            break
    return rows


def validate_partitions(cfg: Config, season: int) -> Dict[str, List[str]]:
    """Validate S3 partition consistency for bronze/silver layers.

    Checks that partition prefixes follow expected Hive-style patterns
    and identifies inconsistencies.

    Args:
        cfg: Pipeline config.
        season: Season to validate.

    Returns:
        Dict mapping table names to lists of issue descriptions.
    """
    s3 = S3IO(cfg.bucket, cfg.region)
    issues: Dict[str, List[str]] = {}

    # Check silver fact tables that should have season partitions
    silver_prefix = cfg.s3_layout["silver_prefix"]
    season_tables = [
        "fct_games", "fct_game_teams", "fct_game_players", "fct_game_media",
        "fct_lines", "fct_plays", "fct_substitutions", "fct_lineups",
        "fct_rankings", "fct_ratings_adjusted", "fct_ratings_srs",
    ]

    for table in season_tables:
        table_issues: List[str] = []
        prefix = f"{silver_prefix}/{table}/season={season}/"
        keys = s3.list_keys(prefix)
        if not keys:
            table_issues.append(f"No data for season={season}")
        else:
            # Check for mixed partition patterns
            has_asof = any("/asof=" in k for k in keys)
            has_date = any("/date=" in k for k in keys)
            if has_asof and has_date:
                table_issues.append(f"Mixed partition patterns: both asof and date found under season={season}")
        if table_issues:
            issues[table] = table_issues

    # Check dim tables (should have asof partitions only)
    dim_tables = ["dim_teams", "dim_conferences", "dim_venues", "dim_lines_providers", "dim_play_types"]
    for table in dim_tables:
        table_issues = []
        prefix = f"{silver_prefix}/{table}/"
        keys = s3.list_keys(prefix)
        if not keys:
            table_issues.append("No data found")
        elif any("/season=" in k for k in keys):
            table_issues.append("Unexpected season partition in dimension table")
        if table_issues:
            issues[table] = table_issues

    return issues


def _parse_args() -> argparse.Namespace:
    """Parse command-line arguments for gap-fill CLI."""
    parser = argparse.ArgumentParser(
        description="Generic gap-fill for fan-out endpoints.",
        epilog="Example: poetry run python -m cbbd_etl.gap_fill --endpoint plays_game --season 2024",
    )
    parser.add_argument("--endpoint", required=True, help="Fan-out endpoint (plays_game, substitutions_game, lineups_game)")
    parser.add_argument("--season", type=int, required=True, help="Season year")
    parser.add_argument("--discover", action="store_true", help="Discover missing IDs via Athena query")
    parser.add_argument("--discover-s3", action="store_true", help="Discover missing IDs by comparing S3 Parquet files")
    parser.add_argument("--missing-ids-file", help="File with missing game IDs (one per line)")
    parser.add_argument("--limit", type=int, help="Limit number of games to process")
    parser.add_argument("--concurrency", type=int, default=10, help="Max concurrent API requests")
    parser.add_argument("--batch-size", type=int, default=100, help="Batch size for concurrent requests")
    parser.add_argument("--resume-file", help="Path to resume file for completed IDs")
    parser.add_argument("--mark-empty", action="store_true", help="Mark empty API responses as done")
    parser.add_argument("--log-every", type=int, default=25, help="Log progress every N games")
    parser.add_argument("--dry-run", action="store_true", help="Fetch but don't write to S3")
    parser.add_argument("--validate", action="store_true", help="Validate partition consistency instead of filling gaps")
    parser.add_argument("--season-range", help="Season range (e.g., 2020-2026) for multi-season gap-fill")
    return parser.parse_args()


def main() -> None:
    """CLI entry point for gap-fill."""
    args = _parse_args()

    # Load env
    env_path = Path(".env")
    if env_path.exists():
        for line in env_path.read_text().splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, val = line.split("=", 1)
            os.environ.setdefault(key.strip(), val.strip().strip("\"'"))

    cfg = load_config()

    # Handle validation mode
    if args.validate:
        seasons = _parse_season_range(args.season_range) if args.season_range else [args.season]
        for season in seasons:
            print(f"\n=== Validating partitions for season {season} ===")
            issues = validate_partitions(cfg, season)
            if not issues:
                print("  No issues found.")
            else:
                for table, table_issues in sorted(issues.items()):
                    for issue in table_issues:
                        print(f"  {table}: {issue}")
        return

    # Determine seasons to process
    seasons = _parse_season_range(args.season_range) if args.season_range else [args.season]

    for season in seasons:
        print(f"\n=== Gap-fill: {args.endpoint} season={season} ===")

        # Discover or load missing IDs
        if args.missing_ids_file:
            missing = load_missing_ids_file(args.missing_ids_file)
        elif args.discover:
            print("Discovering missing IDs via Athena...")
            missing = discover_missing_game_ids(args.endpoint, season)
        elif args.discover_s3:
            print("Discovering missing IDs from S3...")
            missing = discover_missing_from_s3(args.endpoint, season, cfg)
        else:
            print("ERROR: Must specify --discover, --discover-s3, or --missing-ids-file")
            return

        if args.limit:
            missing = missing[: args.limit]

        print(f"Found {len(missing)} games to process")

        if not missing:
            continue

        stats = asyncio.run(
            fill_gaps(
                endpoint=args.endpoint,
                missing=missing,
                cfg=cfg,
                season=season,
                dry_run=args.dry_run,
                concurrency=args.concurrency,
                batch_size=args.batch_size,
                resume_file=args.resume_file,
                mark_empty=args.mark_empty,
                log_every=args.log_every,
            )
        )
        print(f"Done: {stats}")


def _parse_season_range(range_str: str) -> List[int]:
    """Parse a season range string like '2020-2026' into a list of integers."""
    parts = range_str.split("-")
    if len(parts) == 2:
        return list(range(int(parts[0]), int(parts[1]) + 1))
    return [int(parts[0])]


if __name__ == "__main__":
    main()
