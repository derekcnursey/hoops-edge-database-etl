from __future__ import annotations

import argparse
import asyncio
import os
import time
from pathlib import Path

from cbbd_etl.config import load_config
from cbbd_etl.logging_utils import setup_logging
from cbbd_etl.orchestrate import Orchestrator


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run games endpoint for a season range.")
    parser.add_argument("--season-start", type=int, default=2010)
    parser.add_argument("--season-end", type=int, default=2026)
    parser.add_argument("--max-attempts", type=int, default=3, help="Max attempts per season")
    parser.add_argument("--base-delay", type=float, default=5.0, help="Base delay (seconds) between retries")
    parser.add_argument("--resume-file", default="tmp/games_seasons_done.txt")
    parser.add_argument("--skip-seasons", default="2024", help="Comma-separated seasons to skip")
    return parser.parse_args()


def _load_env(path: str = ".env") -> None:
    if not os.path.exists(path):
        return
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, val = line.split("=", 1)
            os.environ.setdefault(key.strip(), val.strip().strip("\"'"))


def _load_done(path: Path) -> set[int]:
    done: set[int] = set()
    if not path.exists():
        return done
    for line in path.read_text().splitlines():
        line = line.strip()
        if line.isdigit():
            done.add(int(line))
    return done


def _mark_done(path: Path, season: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(f"{season}\n")


async def _run_season(orchestrator: Orchestrator, season: int, max_attempts: int, base_delay: float) -> None:
    for attempt in range(1, max_attempts + 1):
        try:
            await orchestrator.run_backfill(seasons=[season], skip_fanout=True, only_endpoints=["games"])
            return
        except Exception:
            if attempt >= max_attempts:
                raise
            delay = base_delay * (2 ** (attempt - 1))
            time.sleep(delay)


def main() -> None:
    args = _parse_args()
    _load_env()
    logger = setup_logging()
    cfg = load_config()
    orchestrator = Orchestrator(cfg, logger)
    resume_path = Path(args.resume_file)
    done = _load_done(resume_path)
    skip: set[int] = set()
    if args.skip_seasons:
        for raw in args.skip_seasons.split(","):
            raw = raw.strip()
            if raw.isdigit():
                skip.add(int(raw))

    async def _run() -> None:
        try:
            for season in range(args.season_start, args.season_end + 1):
                if season in skip:
                    print(f"skip season {season} (skip list)")
                    continue
                if season in done:
                    print(f"skip season {season} (resume)")
                    continue
                print(f"running season {season}")
                await _run_season(orchestrator, season, args.max_attempts, args.base_delay)
                _mark_done(resume_path, season)
        finally:
            await orchestrator.close()

    asyncio.run(_run())


if __name__ == "__main__":
    main()
