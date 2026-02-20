from __future__ import annotations

import asyncio
import json
import os
import subprocess
import time
from typing import Any, Dict, List, Optional, Tuple

import httpx


ATHENA_DB = "cbbd_silver"
ATHENA_WORKGROUP = "cbbd"
ATHENA_OUTPUT = "s3://hoops-edge/athena/"


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
            result_rows = result_rows[1:]  # skip header
            first = False
        for r in result_rows:
            rows.append([c.get("VarCharValue") for c in r["Data"]])
        token = data.get("NextToken")
        if not token:
            break
    return rows


def _get_game_ids_d1_vs_d1_missing_plays(season: str) -> List[int]:
    query = f"""
    SELECT g.gameId
    FROM fct_games g
    LEFT JOIN fct_plays p ON g.gameId = p.gameId
    JOIN dim_teams ht ON g.homeTeamId = ht.teamId
    JOIN dim_teams at ON g.awayTeamId = at.teamId
    WHERE g.season = '{season}' AND p.gameId IS NULL
    """
    qid = _athena_start(query)
    state, meta = _athena_wait(qid)
    if state != "SUCCEEDED":
        raise RuntimeError(meta["QueryExecution"]["Status"].get("StateChangeReason"))
    rows = _athena_rows(qid)
    return [int(r[0]) for r in rows if r and r[0]]


def _get_game_ids_lower_div_sample(season: str, limit: int = 10) -> List[int]:
    query = f"""
    SELECT g.gameId
    FROM fct_games g
    LEFT JOIN fct_plays p ON g.gameId = p.gameId
    LEFT JOIN dim_teams ht ON g.homeTeamId = ht.teamId
    LEFT JOIN dim_teams at ON g.awayTeamId = at.teamId
    WHERE g.season = '{season}' AND p.gameId IS NULL
      AND (ht.teamId IS NULL OR at.teamId IS NULL)
    LIMIT {limit}
    """
    qid = _athena_start(query)
    state, meta = _athena_wait(qid)
    if state != "SUCCEEDED":
        raise RuntimeError(meta["QueryExecution"]["Status"].get("StateChangeReason"))
    rows = _athena_rows(qid)
    return [int(r[0]) for r in rows if r and r[0]]


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


async def _check_game(
    client: httpx.AsyncClient,
    sem: asyncio.Semaphore,
    game_id: int,
    rate_limit: float,
) -> Tuple[int, bool, Optional[str]]:
    async with sem:
        await asyncio.sleep(rate_limit)
        try:
            resp = await client.get(f"/plays/game/{game_id}")
        except Exception as exc:
            return game_id, False, f"error:{exc}"
        if resp.status_code != 200:
            return game_id, False, f"status:{resp.status_code}"
        data = resp.json()
        records = _coerce_records(data)
        return game_id, len(records) > 0, None


async def _run_checks(game_ids: List[int], base_url: str, token: str) -> Dict[str, Any]:
    # keep in line with pipeline config: 3 req/sec
    rate_per_sec = 3
    rate_limit = 1.0 / rate_per_sec
    sem = asyncio.Semaphore(10)
    async with httpx.AsyncClient(base_url=base_url, headers={"Authorization": f"Bearer {token}"}) as client:
        tasks = [_check_game(client, sem, gid, rate_limit) for gid in game_ids]
        results = []
        for coro in asyncio.as_completed(tasks):
            results.append(await coro)
    with_plays = [gid for gid, has, err in results if has]
    errors = {gid: err for gid, has, err in results if err}
    return {
        "total": len(game_ids),
        "with_plays": len(with_plays),
        "without_plays": len(game_ids) - len(with_plays) - len(errors),
        "errors": errors,
        "sample_with_plays": with_plays[:20],
    }


def main() -> None:
    env = _load_env()
    token = env.get("CBBD_API_KEY") or env.get("BEARER_TOKEN") or os.getenv("CBBD_API_KEY") or os.getenv("BEARER_TOKEN")
    if not token:
        raise RuntimeError("Missing API token; set CBBD_API_KEY or BEARER_TOKEN in .env or env vars")
    base_url = env.get("CBBD_API_BASE_URL", "https://api.collegebasketballdata.com")

    season = "2024"
    print("fetching D1 vs D1 missing plays gameIds...")
    d1_ids = _get_game_ids_d1_vs_d1_missing_plays(season)
    print(f"D1 vs D1 missing plays: {len(d1_ids)} gameIds")

    print("fetching lower-division sample gameIds...")
    lower_sample = _get_game_ids_lower_div_sample(season, limit=10)
    print(f"lower-division sample: {lower_sample}")

    print("checking D1 vs D1 games via API (this may take a while)...")
    d1_summary = asyncio.run(_run_checks(d1_ids, base_url, token))
    print(json.dumps({"d1_vs_d1": d1_summary}, indent=2))

    if lower_sample:
        print("checking lower-division sample via API...")
        lower_summary = asyncio.run(_run_checks(lower_sample, base_url, token))
        print(json.dumps({"lower_division_sample": lower_summary}, indent=2))


if __name__ == "__main__":
    main()
