"""CLI runner for gold-layer transforms.

Usage::

    poetry run python -m cbbd_etl.gold.runner --season 2024
    poetry run python -m cbbd_etl.gold.runner --season 2024 --table team_power_rankings
    poetry run python -m cbbd_etl.gold.runner --season 2024 --dry-run

Reads config.yaml, runs selected gold transforms, writes Parquet to S3,
and registers tables in the ``cbbd_gold`` Glue database.
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime
from typing import List, Optional

from ..config import load_config
from ..glue_catalog import GlueCatalog
from ..logging_utils import log_json, setup_logging
from ..s3_io import S3IO, make_part_key
from ..utils import stable_hash
from . import GOLD_TRANSFORMS

GOLD_GLUE_DB = "cbbd_gold"


def main(argv: Optional[List[str]] = None) -> None:
    """Entry point for the gold layer CLI runner.

    Args:
        argv: Command-line arguments. Defaults to ``sys.argv[1:]``.
    """
    parser = argparse.ArgumentParser(
        description="Build gold-layer analytics tables from silver data.",
    )
    parser.add_argument(
        "--season",
        type=int,
        required=True,
        help="Season year to build gold tables for (e.g. 2024).",
    )
    parser.add_argument(
        "--table",
        type=str,
        default=None,
        help="Specific gold table to build. If omitted, all tables are built.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="Run transforms but do not write to S3 or register in Glue.",
    )
    parser.add_argument(
        "--config",
        type=str,
        default="config.yaml",
        help="Path to config.yaml (default: config.yaml).",
    )
    args = parser.parse_args(argv)

    logger = setup_logging()
    cfg = load_config(args.config)
    season = args.season
    asof = datetime.utcnow().date().isoformat()

    # Determine which tables to build
    if args.table:
        if args.table not in GOLD_TRANSFORMS:
            available = ", ".join(sorted(GOLD_TRANSFORMS.keys()))
            logger.error(f"Unknown gold table: {args.table}. Available: {available}")
            sys.exit(1)
        tables_to_build = [args.table]
    else:
        tables_to_build = list(GOLD_TRANSFORMS.keys())

    s3 = S3IO(cfg.bucket, cfg.region)
    glue = GlueCatalog(cfg.region)
    gold_prefix = cfg.s3_layout["gold_prefix"]

    log_json(
        logger,
        "gold_run_start",
        season=season,
        tables=tables_to_build,
        dry_run=args.dry_run,
        asof=asof,
    )

    for table_name in tables_to_build:
        build_fn = GOLD_TRANSFORMS[table_name]
        log_json(logger, "gold_build_start", table=table_name, season=season)

        try:
            result_table = build_fn(cfg, season)
        except Exception as exc:
            log_json(
                logger,
                "gold_build_error",
                table=table_name,
                season=season,
                error=str(exc),
            )
            continue

        num_rows = result_table.num_rows
        log_json(
            logger,
            "gold_build_done",
            table=table_name,
            season=season,
            rows=num_rows,
        )

        if num_rows == 0:
            log_json(logger, "gold_skip_empty", table=table_name, season=season)
            continue

        if args.dry_run:
            log_json(
                logger,
                "gold_dry_run_skip_write",
                table=table_name,
                season=season,
                rows=num_rows,
            )
            continue

        # Write to S3
        payload_hash = stable_hash({"table": table_name, "season": season})
        partition = f"season={season}/asof={asof}"
        s3_key = make_part_key(
            gold_prefix,
            table_name,
            partition,
            f"part-{payload_hash[:8]}.parquet",
        )

        s3.put_parquet(s3_key, result_table)
        log_json(
            logger,
            "gold_s3_write",
            table=table_name,
            key=s3_key,
            rows=num_rows,
        )

        # Register in Glue
        location = f"s3://{cfg.bucket}/{gold_prefix}/{table_name}/"
        schema = result_table.schema
        partition_keys = ["season", "asof"]

        # Remove partition columns from schema to avoid duplicates in Glue
        for pk in partition_keys:
            if pk in schema.names:
                idx = schema.get_field_index(pk)
                if idx >= 0:
                    schema = schema.remove(idx)

        glue.ensure_database(GOLD_GLUE_DB)
        glue.ensure_table(GOLD_GLUE_DB, table_name, location, schema, partition_keys)
        log_json(
            logger,
            "gold_glue_registered",
            database=GOLD_GLUE_DB,
            table=table_name,
        )

    log_json(logger, "gold_run_done", season=season, tables_built=len(tables_to_build))


if __name__ == "__main__":
    main()
