from __future__ import annotations

import argparse
import asyncio
import json
import os
import subprocess
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import httpx

from cbbd_etl.config import load_config
from cbbd_etl.extractors import build_registry
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
from cbbd_etl.utils import stable_hash


ATHENA_DB = "cbbd_silver"
ATHENA_WORKGROUP = "cbbd"
ATHENA_OUTPUT = "s3://hoops-edge/athena/"


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backfill plays for games missing play-by-play.")
    parser.add_argument("--season", required=True, help="Season (e.g., 2024)")
    parser.add_argument("--limit", type=int, help="Limit number of games to backfill")
    parser.add_argument("--ingested-at", help="Override ingested_at date (YYYY-MM-DD)")
    parser.add_argument("--dry-run", action="store_true", help="Fetch counts without writing")
    parser.add_argument("--resume-file", help="Path to local resume file for completed gameIds")
    parser.add_argument("--concurrency", type=int, default=10, help="Max concurrent API requests")
    parser.add_argument("--batch-size", type=int, default=100, help="Batch size for concurrent requests")
    parser.add_argument("--mark-empty", action="store_true", help="Record empty responses as done to skip re-fetching")
    parser.add_argument("--log-every", type=int, default=25, help="Log progress every N processed games")
    return parser.parse_args()


def _load_env(path: str = ".env") -> Dict[str, str]:
    env: Dict[str, str] = {}
    if not os.path.exists(path):
        return env
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, val = line.split("=", 1)
            env[key.strip()] = val.strip().strip("\"'")
    return env


def _aws_json(args: List[str]) -> Dict[str, Any]:
    out = subprocess.check_output(["aws"] + args)
    return json.loads(out)


def _athena_start(query: str) -> str:
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
    while True:
        data = _aws_json(["athena", "get-query-execution", "--query-execution-id", qid])
        state = data["QueryExecution"]["Status"]["State"]
        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            return state, data
        time.sleep(1)


def _athena_rows(qid: str) -> List[List[Optional[str]]]:
    rows: List[List[Optional[str]]] = []
    token: Optional[str] = None
    first = True
    while True:
        args = ["athena", "get-query-results", "--query-execution-id", qid, "--max-results", "1000"]
        if token:
            args += ["--next-token", token]
        data = _aws_json(args)
        result_rows = data["ResultSet"]["Rows"]
        if first:
            result_rows = result_rows[1:]
            first = False
        for r in result_rows:
            rows.append([c.get("VarCharValue") for c in r["Data"]])
        token = data.get("NextToken")
        if not token:
            break
    return rows


def _missing_games(season: str) -> List[Tuple[int, str]]:
    query = f"""
    SELECT
        g.gameId,
        substr(g.startDate, 1, 10) AS game_date
    FROM fct_games g
    LEFT JOIN fct_plays p ON g.gameId = p.gameId
    WHERE g.season = '{season}' AND p.gameId IS NULL
    """
    qid = _athena_start(query)
    state, meta = _athena_wait(qid)
    if state != "SUCCEEDED":
        raise RuntimeError(meta["QueryExecution"]["Status"].get("StateChangeReason"))
    rows = _athena_rows(qid)
    out = []
    for r in rows:
        if not r or not r[0]:
            continue
        out.append((int(r[0]), r[1]))
    return out


def _coerce_records(resp: Any) -> List[Dict[str, Any]]:
    if resp is None:
        return []
    if isinstance(resp, list):
        return resp
    if isinstance(resp, dict):
        if "data" in resp and isinstance(resp["data"], list):
            return resp["data"]
        return [resp]
    return []


def _ensure_tables(glue, bucket: str, layout: dict, layer: str, table: str, schema, partition: str) -> None:
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


async def _fetch_play_by_play(client: httpx.AsyncClient, game_id: int) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    max_attempts = 5
    base_delay = 0.5
    max_delay = 8.0
    for attempt in range(1, max_attempts + 1):
        try:
            resp = await client.get(f"/plays/game/{game_id}")
        except (httpx.ReadTimeout, httpx.RequestError) as exc:
            if attempt >= max_attempts:
                return [], f"error:{type(exc).__name__}"
            await asyncio.sleep(min(max_delay, base_delay * (2 ** (attempt - 1))))
            continue

        if resp.status_code == 200:
            return _coerce_records(resp.json()), None
        if resp.status_code in (429, 500, 502, 503, 504):
            if attempt >= max_attempts:
                return [], f"status:{resp.status_code}"
            retry_after = resp.headers.get("Retry-After")
            if retry_after:
                delay = min(max_delay, float(retry_after))
            else:
                delay = min(max_delay, base_delay * (2 ** (attempt - 1)))
            await asyncio.sleep(delay)
            continue
        return [], f"status:{resp.status_code}"
    return [], "error:unknown"


async def _fetch_one(
    client: httpx.AsyncClient,
    sem: asyncio.Semaphore,
    game_id: int,
    game_date: str,
) -> Tuple[int, str, List[Dict[str, Any]], Optional[str]]:
    async with sem:
        records, err = await _fetch_play_by_play(client, game_id)
        return game_id, game_date, records, err


def _chunked(rows: List[Tuple[int, str]], size: int) -> List[List[Tuple[int, str]]]:
    return [rows[i : i + size] for i in range(0, len(rows), size)]


async def _run(
    game_rows: List[Tuple[int, str]],
    cfg,
    ingested_at: str,
    season: int,
    dry_run: bool,
    resume_path: Path,
    concurrency: int,
    batch_size: int,
    mark_empty: bool,
    log_every: int,
) -> None:
    from cbbd_etl.glue_catalog import GlueCatalog

    registry = build_registry(cfg.endpoints)
    spec = registry["plays_game"]
    s3 = S3IO(cfg.bucket, cfg.region)
    glue = GlueCatalog(cfg.region)
    raw_endpoint = RAW_PREFIX_OVERRIDES.get("plays_game", "plays_game")

    token = os.getenv("CBBD_API_KEY") or os.getenv("BEARER_TOKEN")
    done_ids: set[int] = set()
    if resume_path.exists():
        for line in resume_path.read_text().splitlines():
            line = line.strip()
            if line.isdigit():
                done_ids.add(int(line))

    resume_path.parent.mkdir(parents=True, exist_ok=True)

    async with httpx.AsyncClient(
        base_url=cfg.api["base_url"],
        headers={"Authorization": f"Bearer {token}"},
        timeout=cfg.api.get("timeout_seconds", 30),
    ) as client:
        pending_rows = [(gid, gdate) for gid, gdate in game_rows if gid not in done_ids]
        total = len(pending_rows)
        processed = 0
        written = 0
        empty = 0
        errors = 0
        err_counts: Dict[str, int] = {}
        bronze_ready = False
        silver_ready = False
        sem = asyncio.Semaphore(max(1, concurrency))

        for batch_idx, batch in enumerate(_chunked(pending_rows, max(1, batch_size)), start=1):
            batch_start = time.time()
            print(f"starting batch {batch_idx} size={len(batch)}")
            tasks = [asyncio.create_task(_fetch_one(client, sem, gid, gdate)) for gid, gdate in batch]
            results = await asyncio.gather(*tasks)
            batch_elapsed = time.time() - batch_start
            print(f"finished batch {batch_idx} in {batch_elapsed:.1f}s")
            for game_id, game_date, records, err in results:
                processed += 1
                if err:
                    errors += 1
                    err_counts[err] = err_counts.get(err, 0) + 1
                    continue
                if not records:
                    empty += 1
                    if mark_empty and not dry_run:
                        with resume_path.open("a", encoding="utf-8") as f:
                            f.write(f"{game_id}\n")
                    continue

                params = {"gameId": game_id}
                payload_hash = stable_hash(params)

                raw_key = make_part_key(
                    cfg.s3_layout["raw_prefix"],
                    raw_endpoint,
                    f"ingested_at={ingested_at}",
                    f"part-{payload_hash[:8]}.json.gz",
                )
                bronze_table = BRONZE_TABLES["plays_game"]
                bronze_partition = _bronze_partition(spec, season=season, date=game_date, asof=ingested_at)
                bronze_key = make_part_key(
                    cfg.s3_layout["bronze_prefix"],
                    bronze_table,
                    bronze_partition,
                    f"part-{payload_hash[:8]}.parquet",
                )
                silver_table = SILVER_TABLES["plays_game"]
                silver_partition = _silver_partition(silver_table, season=season, date=game_date, asof=ingested_at)
                silver_key = make_part_key(
                    cfg.s3_layout["silver_prefix"],
                    silver_table,
                    silver_partition,
                    f"part-{payload_hash[:8]}.parquet",
                )

                if not dry_run:
                    s3.put_json_gz(raw_key, records)
                    bronze_table_pa = normalize_records(bronze_table, records)
                    s3.put_parquet(bronze_key, bronze_table_pa)
                    if not bronze_ready:
                        _ensure_tables(
                            glue,
                            cfg.bucket,
                            cfg.s3_layout,
                            "bronze",
                            bronze_table,
                            bronze_table_pa.schema,
                            bronze_partition,
                        )
                        bronze_ready = True

                    records_s = _apply_key_aliases(silver_table, records)
                    spec_def = TABLE_SPECS.get(silver_table)
                    if spec_def:
                        records_s = dedupe_records(records_s, spec_def.primary_keys)
                    silver_table_pa = normalize_records(silver_table, records_s)
                    s3.put_parquet(silver_key, silver_table_pa)
                    if not silver_ready:
                        _ensure_tables(
                            glue,
                            cfg.bucket,
                            cfg.s3_layout,
                            "silver",
                            silver_table,
                            silver_table_pa.schema,
                            silver_partition,
                        )
                        silver_ready = True

                    with resume_path.open("a", encoding="utf-8") as f:
                        f.write(f"{game_id}\n")

                written += 1

            if processed % max(1, log_every) == 0 or processed == total:
                err_summary = ", ".join(f"{k}={v}" for k, v in sorted(err_counts.items()))
                print(
                    f"processed {processed}/{total} written={written} empty={empty} errors={errors}"
                    + (f" [{err_summary}]" if err_summary else "")
                )


def main() -> None:
    args = _parse_args()

    env = _load_env()
    for k, v in env.items():
        os.environ.setdefault(k, v)

    cfg = load_config()
    token = os.getenv("CBBD_API_KEY") or os.getenv("BEARER_TOKEN")
    if not token:
        raise RuntimeError("Missing API token; set CBBD_API_KEY or BEARER_TOKEN in .env or env vars")

    ingested_at = args.ingested_at or time.strftime("%Y-%m-%d")
    season = int(args.season)
    rows = _missing_games(args.season)
    if args.limit:
        rows = rows[: args.limit]
    print(f"missing plays games: {len(rows)}")

    resume_path = Path(args.resume_file) if args.resume_file else Path("tmp") / f"missing_plays_done_{season}.txt"
    asyncio.run(
        _run(
            rows,
            cfg,
            ingested_at,
            season,
            args.dry_run,
            resume_path,
            args.concurrency,
            args.batch_size,
            args.mark_empty,
            args.log_every,
        )
    )


if __name__ == "__main__":
    main()
