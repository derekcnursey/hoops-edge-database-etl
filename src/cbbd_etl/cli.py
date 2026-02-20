from __future__ import annotations

import argparse
import asyncio
import json

from .config import load_config
from .logging_utils import setup_logging
from .orchestrate import Orchestrator


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="cbbd_etl")
    sub = parser.add_subparsers(dest="command", required=True)

    backfill = sub.add_parser("backfill")
    backfill.add_argument("--season-start", type=int)
    backfill.add_argument("--season-end", type=int)
    backfill.add_argument("--skip-fanout", action="store_true")
    backfill.add_argument("--only-endpoints", help="Comma-separated endpoint names")

    incremental = sub.add_parser("incremental")
    incremental.add_argument("--season-start", type=int)
    incremental.add_argument("--season-end", type=int)
    incremental.add_argument("--skip-fanout", action="store_true")
    incremental.add_argument("--only-endpoints", help="Comma-separated endpoint names")

    one = sub.add_parser("one")
    one.add_argument("--endpoint", required=True)
    one.add_argument("--params", default="{}", help="JSON string of params")

    fanout = sub.add_parser("fanout")
    fanout.add_argument("--endpoint", help="Optional fanout endpoint (lineups_game, plays_game, substitutions_game, plays_player, substitutions_player)")
    fanout.add_argument("--season-start", type=int)
    fanout.add_argument("--season-end", type=int)
    fanout.add_argument("--limit", type=int)
    fanout.add_argument("--batch-size", type=int)
    fanout.add_argument("--games-from-s3", action="store_true")
    fanout.add_argument("--resume-file", help="Path to file with completed gameIds to skip")

    sub.add_parser("validate")

    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    logger = setup_logging()
    cfg = load_config()
    orchestrator = Orchestrator(cfg, logger)

    async def _run() -> None:
        try:
            if args.command == "backfill":
                seasons = _seasons_from_args(args) if args.season_start or args.season_end else None
                only = _parse_only(args.only_endpoints)
                await orchestrator.run_backfill(seasons=seasons, skip_fanout=args.skip_fanout, only_endpoints=only)
            elif args.command == "incremental":
                seasons = _seasons_from_args(args) if args.season_start or args.season_end else None
                only = _parse_only(args.only_endpoints)
                await orchestrator.run_incremental(seasons=seasons, skip_fanout=args.skip_fanout, only_endpoints=only)
            elif args.command == "one":
                params = json.loads(args.params)
                await orchestrator.run_one(args.endpoint, params)
            elif args.command == "fanout":
                seasons = _seasons_from_args(args) if args.season_start or args.season_end else None
                await orchestrator.run_fanout_only(
                    seasons=seasons,
                    endpoint=args.endpoint,
                    limit=args.limit,
                    batch_size=args.batch_size,
                    games_from_s3=args.games_from_s3,
                    resume_file=args.resume_file,
                )
            elif args.command == "validate":
                await orchestrator.validate()
        finally:
            await orchestrator.close()

    asyncio.run(_run())


def _seasons_from_args(args: argparse.Namespace) -> list[int]:
    start = args.season_start
    end = args.season_end or start
    if start is None:
        raise ValueError("--season-start is required when using season filters")
    if end is None:
        end = start
    if end < start:
        raise ValueError("--season-end must be >= --season-start")
    return list(range(start, end + 1))


def _parse_only(raw: str | None) -> list[str] | None:
    if not raw:
        return None
    return [x.strip() for x in raw.split(",") if x.strip()]
