"""Sweep SOS parameters by running the solver directly.

For each parameter combo, runs _run_per_date_ratings and saves results
as parquet files to a local directory for the predictor to evaluate.
"""
from __future__ import annotations

import itertools
import json
import logging
import sys
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from cbbd_etl.config import load_config
from cbbd_etl.s3_io import S3IO
from cbbd_etl.gold.adjusted_efficiencies import (
    _get_rating_params,
    _get_margin_cap,
    _load_d1_team_ids,
    _load_team_info,
    _load_pbp_no_garbage_games,
    _run_per_date_ratings,
    _apply_margin_cap,
)
from cbbd_etl.normalize import normalize_records

logging.basicConfig(level=logging.INFO, format="%(message)s")

SEASON = 2025
OUTPUT_DIR = Path(__file__).resolve().parent.parent.parent / "hoops-edge-predictor" / "features" / "sos_sweep"

VARIANTS = [
    {"sos_exponent": 1.0,  "shrinkage": 0.0,  "label": "baseline"},
    {"sos_exponent": 0.85, "shrinkage": 0.0,  "label": "sos_0.85"},
    {"sos_exponent": 0.7,  "shrinkage": 0.0,  "label": "sos_0.70"},
    {"sos_exponent": 0.5,  "shrinkage": 0.0,  "label": "sos_0.50"},
    {"sos_exponent": 1.0,  "shrinkage": 0.05, "label": "shrink_0.05"},
    {"sos_exponent": 0.85, "shrinkage": 0.05, "label": "sos_0.85_shrink_0.05"},
]


def main():
    cfg_path = str(Path(__file__).resolve().parent.parent / "config.yaml")
    cfg = load_config(cfg_path)
    s3 = S3IO(cfg.bucket, cfg.region)
    params = _get_rating_params(cfg)
    margin_cap = _get_margin_cap(cfg)

    print("Loading D1 teams, team info, and game data...")
    d1_ids = _load_d1_team_ids(s3, cfg)
    team_info = _load_team_info(s3, cfg)
    games_by_date = _load_pbp_no_garbage_games(s3, cfg, SEASON, d1_ids)
    print(f"  Game dates: {len(games_by_date)}")

    if margin_cap is not None:
        games_by_date = _apply_margin_cap(games_by_date, margin_cap)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    for i, variant in enumerate(VARIANTS):
        label = variant["label"]
        print(f"\n[{i+1}/{len(VARIANTS)}] {label} (sos_exp={variant['sos_exponent']}, shrink={variant['shrinkage']})")

        records = _run_per_date_ratings(
            games_by_date, team_info, SEASON,
            half_life=params.get("half_life"),
            hca_oe=params["hca_oe"],
            hca_de=params["hca_de"],
            barthag_exp=params["barthag_exp"],
            sos_exponent=variant["sos_exponent"],
            shrinkage=variant["shrinkage"],
        )

        print(f"  Records: {len(records)}")

        if not records:
            continue

        # Save as parquet using pyarrow
        tbl = normalize_records("team_adjusted_efficiencies_no_garbage", records)
        out_path = OUTPUT_DIR / f"ratings_{label}.parquet"
        pq.write_table(tbl, str(out_path))
        print(f"  Saved: {out_path} ({tbl.num_rows} rows)")

    print(f"\nAll variants saved to {OUTPUT_DIR}")


if __name__ == "__main__":
    main()
