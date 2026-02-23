#!/usr/bin/env python3
"""Tune adjusted efficiency rating parameters using ONLY seasons 2015-2023.

For each parameter combo (half_life, margin_cap), builds end-of-season ratings
for each holdout season and computes the raw-ratings spread prediction MAE
against actual game margins (where book spreads are available).

This ensures 2024-2026 are completely unseen during parameter selection.

Usage:
    poetry run python scripts/tune_ratings_holdout.py
"""

from __future__ import annotations

import itertools
import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from cbbd_etl.config import Config, load_config
from cbbd_etl.gold.adjusted_efficiencies import (
    _apply_margin_cap,
    _load_box_score_games,
    _load_d1_team_ids,
    _load_team_info,
)
from cbbd_etl.gold.iterative_ratings import (
    GameObs,
    exponential_decay_weight,
    solve_ratings,
)
from cbbd_etl.s3_io import S3IO
from cbbd_etl.gold._io_helpers import pydict_get, pydict_get_first, read_silver_table, dedup_by

logging.basicConfig(level=logging.WARNING, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

HOLDOUT_SEASONS = list(range(2016, 2024))  # 2016-2023 (2015 too sparse)

# Grid
HALF_LIVES = [15, 20, 30, 45, 60]
MARGIN_CAPS = [10, 15, 20, None]  # None = no cap


def compute_empirical_hca(
    all_games_by_date: Dict[str, List[GameObs]],
) -> Tuple[float, float]:
    """Compute HCA from training data."""
    home_pts = 0.0
    home_poss = 0.0
    away_pts = 0.0
    away_poss = 0.0
    for day_games in all_games_by_date.values():
        for g in day_games:
            if g.is_neutral or g.team_poss <= 0:
                continue
            if g.is_home:
                home_pts += g.team_pts
                home_poss += g.team_poss
            else:
                away_pts += g.team_pts
                away_poss += g.team_poss
    if home_poss == 0 or away_poss == 0:
        return 1.4, 1.4
    home_rate = home_pts / home_poss * 100
    away_rate = away_pts / away_poss * 100
    hca_total = home_rate - away_rate
    return hca_total / 2, hca_total / 2


def run_end_of_season_ratings(
    games_by_date: Dict[str, List[GameObs]],
    half_life: float,
    hca_oe: float,
    hca_de: float,
) -> Dict[int, Dict]:
    """Run ratings solver for end-of-season snapshot."""
    if not games_by_date:
        return {}
    latest = max(games_by_date.keys())
    rd = datetime.strptime(latest, "%Y-%m-%d").date()

    all_games: List[GameObs] = []
    for dt_str, day_games in games_by_date.items():
        gd = datetime.strptime(dt_str, "%Y-%m-%d").date()
        days_ago = (rd - gd).days
        w = exponential_decay_weight(days_ago, half_life=half_life)
        for g in day_games:
            all_games.append(GameObs(
                game_id=g.game_id, team_id=g.team_id, opp_id=g.opp_id,
                team_pts=g.team_pts, team_poss=g.team_poss,
                opp_pts=g.opp_pts, opp_poss=g.opp_poss,
                is_home=g.is_home, is_neutral=g.is_neutral,
                game_date=g.game_date, weight=w,
            ))
    return solve_ratings(all_games, hca_oe=hca_oe, hca_de=hca_de)


def load_actual_margins(s3: S3IO, cfg: Config, season: int) -> Dict[int, Tuple[float, int, int, bool]]:
    """Load actual game margins: {gameId: (margin, homeTeamId, awayTeamId, neutral)}.

    margin = homeScore - awayScore
    """
    fct_games = dedup_by(
        read_silver_table(s3, cfg, "fct_games", season=season), ["gameId"]
    )
    if fct_games.num_rows == 0:
        return {}
    gids = pydict_get(fct_games, "gameId")
    home_pts = pydict_get_first(fct_games, ["homePoints", "homeScore"])
    away_pts = pydict_get_first(fct_games, ["awayPoints", "awayScore"])
    home_tids = pydict_get(fct_games, "homeTeamId")
    away_tids = pydict_get(fct_games, "awayTeamId")
    neutrals = pydict_get(fct_games, "neutralSite")

    result = {}
    for i, gid in enumerate(gids):
        if gid is None or home_pts[i] is None or away_pts[i] is None:
            continue
        try:
            margin = float(home_pts[i]) - float(away_pts[i])
            result[int(gid)] = (
                margin,
                int(home_tids[i]),
                int(away_tids[i]),
                bool(neutrals[i]) if neutrals[i] is not None else False,
            )
        except (ValueError, TypeError):
            continue
    return result


def compute_ratings_mae(
    ratings: Dict[int, Dict],
    game_margins: Dict[int, Tuple[float, int, int, bool]],
    hca_oe: float,
    hca_de: float,
) -> Tuple[float, int]:
    """Compute MAE of ratings-based spread predictions vs actual margins.

    Predicted spread = (home_adj_oe - home_adj_de) - (away_adj_oe - away_adj_de)
                     + HCA adjustment for non-neutral games.

    Returns (mae, n_games).
    """
    errors = []
    for gid, (actual_margin, home_tid, away_tid, is_neutral) in game_margins.items():
        home_r = ratings.get(home_tid)
        away_r = ratings.get(away_tid)
        if home_r is None or away_r is None:
            continue
        if home_r["games_played"] == 0 or away_r["games_played"] == 0:
            continue

        # Predicted margin = home efficiency margin - away efficiency margin + HCA
        home_margin = home_r["adj_oe"] - home_r["adj_de"]
        away_margin = away_r["adj_oe"] - away_r["adj_de"]
        predicted = home_margin - away_margin
        if not is_neutral:
            predicted += (hca_oe + hca_de)  # home advantage in points per 100 poss

        errors.append(abs(predicted - actual_margin))

    if not errors:
        return float("inf"), 0
    return sum(errors) / len(errors), len(errors)


def main():
    cfg = load_config("config.yaml")
    s3 = S3IO(cfg.bucket, cfg.region)

    d1_ids = _load_d1_team_ids(s3, cfg)
    team_info = _load_team_info(s3, cfg)

    print(f"D1 teams: {len(d1_ids)}")
    print(f"Holdout seasons: {HOLDOUT_SEASONS}")
    print(f"Grid: half_life={HALF_LIVES}, margin_cap={MARGIN_CAPS}")
    print()

    # Load data for all holdout seasons
    print("Loading game data for all holdout seasons...")
    season_data: Dict[int, Dict[str, List[GameObs]]] = {}
    season_margins: Dict[int, Dict[int, Tuple[float, int, int, bool]]] = {}

    for season in HOLDOUT_SEASONS:
        games_by_date = _load_box_score_games(s3, cfg, season, d1_ids)
        n_obs = sum(len(v) for v in games_by_date.values())
        margins = load_actual_margins(s3, cfg, season)
        print(f"  Season {season}: {n_obs} obs across {len(games_by_date)} dates, {len(margins)} games with margins")
        season_data[season] = games_by_date
        season_margins[season] = margins

    # Compute empirical HCA from ALL holdout seasons
    print("\nComputing empirical HCA from holdout seasons...")
    all_games: Dict[str, List[GameObs]] = {}
    for season in HOLDOUT_SEASONS:
        for dt, games in season_data[season].items():
            key = f"{season}_{dt}"
            all_games[key] = games
    hca_oe, hca_de = compute_empirical_hca(all_games)
    print(f"  HCA OE: {hca_oe:.4f}")
    print(f"  HCA DE: {hca_de:.4f}")
    print(f"  HCA Total: {hca_oe + hca_de:.4f}")

    # Grid search
    print("\nRunning grid search...\n")
    results = []

    for half_life, margin_cap in itertools.product(HALF_LIVES, MARGIN_CAPS):
        total_errors = 0.0
        total_games = 0

        for season in HOLDOUT_SEASONS:
            gbd = season_data[season]
            if not gbd:
                continue

            # Apply margin cap if set
            if margin_cap is not None:
                gbd = _apply_margin_cap(gbd, margin_cap)

            # Get end-of-season ratings
            ratings = run_end_of_season_ratings(gbd, half_life, hca_oe, hca_de)
            if not ratings:
                continue

            mae, n = compute_ratings_mae(ratings, season_margins[season], hca_oe, hca_de)
            if n > 0:
                total_errors += mae * n
                total_games += n

        agg_mae = total_errors / total_games if total_games > 0 else float("inf")
        cap_str = str(margin_cap) if margin_cap is not None else "None"
        results.append({
            "half_life": half_life,
            "margin_cap": margin_cap,
            "mae": agg_mae,
            "n_games": total_games,
        })
        print(f"  hl={half_life:>3}  cap={cap_str:>4}  MAE={agg_mae:.4f}  games={total_games}")

    # Find best
    results.sort(key=lambda x: x["mae"])
    best = results[0]
    best_cap_str = str(best["margin_cap"]) if best["margin_cap"] is not None else "None"

    print(f"\n{'='*60}")
    print(f"  BEST PARAMETERS (holdout tuned on 2016-2023)")
    print(f"{'='*60}")
    print(f"  half_life:  {best['half_life']}")
    print(f"  margin_cap: {best_cap_str}")
    print(f"  hca_oe:     {hca_oe:.4f}")
    print(f"  hca_de:     {hca_de:.4f}")
    print(f"  MAE:        {best['mae']:.4f}")
    print(f"  Games:      {best['n_games']}")

    print(f"\n  Top 5 parameter combos:")
    for i, r in enumerate(results[:5]):
        cap = str(r["margin_cap"]) if r["margin_cap"] is not None else "None"
        print(f"    {i+1}. hl={r['half_life']:>3} cap={cap:>4} MAE={r['mae']:.4f}")

    print(f"\n  Holdout-tuned params: half_life={best['half_life']}, margin_cap={best_cap_str}, HCA={hca_oe + hca_de:.4f}")

    # Print config.yaml snippet
    print(f"\n  config.yaml snippet:")
    print(f"    gold:")
    print(f"      adjusted_efficiencies:")
    print(f"        half_life: {float(best['half_life'])}")
    print(f"        hca_oe: {round(hca_oe, 4)}")
    print(f"        hca_de: {round(hca_de, 4)}")
    print(f"        barthag_exp: 11.5")
    if best["margin_cap"] is not None:
        print(f"        margin_cap: {best['margin_cap']}")
    else:
        print(f"        # margin_cap: null  (no cap)")


if __name__ == "__main__":
    main()
