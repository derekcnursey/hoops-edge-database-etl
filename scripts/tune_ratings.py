#!/usr/bin/env python3
"""Tune adjusted efficiency rating parameters.

Computes empirical HCA, tests multiple half-life values and BARTHAG
exponents, and optionally correlates against BartTorvik reference data.

Usage:
    poetry run python scripts/tune_ratings.py --season 2025
    poetry run python scripts/tune_ratings.py --season 2025 --barttorvik-csv barttorvik_2025.csv
"""

from __future__ import annotations

import argparse
import csv
import logging
import math
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from cbbd_etl.config import Config, load_config
from cbbd_etl.gold.adjusted_efficiencies import (
    _load_box_score_games,
    _load_d1_team_ids,
    _load_team_info,
)
from cbbd_etl.gold.iterative_ratings import (
    GameObs,
    compute_barthag,
    exponential_decay_weight,
    solve_ratings,
)
from cbbd_etl.s3_io import S3IO

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def pearson_r(x: List[float], y: List[float]) -> float:
    """Pearson correlation coefficient (no scipy dependency)."""
    n = len(x)
    if n < 3:
        return float("nan")
    mx = sum(x) / n
    my = sum(y) / n
    sxx = sum((xi - mx) ** 2 for xi in x)
    syy = sum((yi - my) ** 2 for yi in y)
    sxy = sum((xi - mx) * (yi - my) for xi, yi in zip(x, y))
    denom = (sxx * syy) ** 0.5
    if denom == 0:
        return float("nan")
    return sxy / denom


def compute_empirical_hca(
    games_by_date: Dict[str, List[GameObs]],
) -> Tuple[float, float, float, int]:
    """Compute home court advantage from data.

    Returns (hca_oe, hca_de, hca_total, n_games).
    hca_oe: pts/100 poss added to home OE.
    hca_de: pts/100 poss subtracted from home DE.
    hca_total: total home margin per 100 poss.
    """
    home_pts = 0.0
    home_poss = 0.0
    away_pts = 0.0
    away_poss = 0.0
    n_games = 0

    for day_games in games_by_date.values():
        for g in day_games:
            if g.is_neutral or g.team_poss <= 0:
                continue
            if g.is_home:
                home_pts += g.team_pts
                home_poss += g.team_poss
            else:
                away_pts += g.team_pts
                away_poss += g.team_poss
            n_games += 1

    if home_poss == 0 or away_poss == 0:
        return 1.4, 1.4, 2.8, 0

    home_rate = home_pts / home_poss * 100
    away_rate = away_pts / away_poss * 100
    hca_total = home_rate - away_rate
    hca_oe = hca_total / 2
    hca_de = hca_total / 2
    return hca_oe, hca_de, hca_total, n_games // 2  # //2 because each game has 2 obs


def run_end_of_season(
    games_by_date: Dict[str, List[GameObs]],
    half_life: float,
    hca_oe: float,
    hca_de: float,
) -> Dict[int, Dict]:
    """Run solver for end-of-season ratings with given parameters."""
    latest = max(games_by_date.keys())
    rd = datetime.strptime(latest, "%Y-%m-%d").date()

    all_games: List[GameObs] = []
    for dt_str, day_games in games_by_date.items():
        gd = datetime.strptime(dt_str, "%Y-%m-%d").date()
        days_ago = (rd - gd).days
        w = exponential_decay_weight(days_ago, half_life=half_life)
        for g in day_games:
            all_games.append(
                GameObs(
                    game_id=g.game_id,
                    team_id=g.team_id,
                    opp_id=g.opp_id,
                    team_pts=g.team_pts,
                    team_poss=g.team_poss,
                    opp_pts=g.opp_pts,
                    opp_poss=g.opp_poss,
                    is_home=g.is_home,
                    is_neutral=g.is_neutral,
                    game_date=g.game_date,
                    weight=w,
                )
            )

    return solve_ratings(all_games, hca_oe=hca_oe, hca_de=hca_de)


def load_barttorvik_csv(path: str) -> Dict[str, Dict[str, float]]:
    """Load BartTorvik reference data from CSV.

    Expects columns: team, adj_oe (or adjoe), adj_de (or adjde),
    barthag, adj_tempo (or adjtempo).

    Returns {team_name_lower: {adj_oe, adj_de, barthag, adj_tempo}}.
    """
    result: Dict[str, Dict[str, float]] = {}
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        # Normalize column names
        if reader.fieldnames is None:
            return result

        col_map: Dict[str, str] = {}
        for col in reader.fieldnames:
            cl = col.strip().lower().replace(" ", "_")
            if cl in ("team", "school", "team_name"):
                col_map["team"] = col
            elif cl in ("adj_oe", "adjoe", "adjoe_", "oe"):
                col_map["adj_oe"] = col
            elif cl in ("adj_de", "adjde", "adjde_", "de"):
                col_map["adj_de"] = col
            elif cl in ("barthag", "barthag_"):
                col_map["barthag"] = col
            elif cl in ("adj_tempo", "adjtempo", "adj_t", "tempo"):
                col_map["adj_tempo"] = col

        if "team" not in col_map:
            logger.error("BartTorvik CSV missing team column. Found: %s", reader.fieldnames)
            return result

        for row in reader:
            team = row[col_map["team"]].strip().lower()
            entry: Dict[str, float] = {}
            for key in ("adj_oe", "adj_de", "barthag", "adj_tempo"):
                if key in col_map:
                    try:
                        entry[key] = float(row[col_map[key]])
                    except (ValueError, KeyError):
                        pass
            if entry:
                result[team] = entry

    logger.info("Loaded %d teams from BartTorvik CSV", len(result))
    return result


def match_teams(
    our_ratings: Dict[int, Dict],
    team_info: Dict[int, Dict[str, Any]],
    bt_data: Dict[str, Dict[str, float]],
) -> List[Tuple[int, str, Dict, Dict[str, float]]]:
    """Match our team IDs to BartTorvik entries by name."""
    matched = []
    bt_keys = set(bt_data.keys())

    for tid, vals in our_ratings.items():
        info = team_info.get(tid, {})
        school = info.get("school")
        if not school:
            continue
        key = school.strip().lower()
        if key in bt_keys:
            matched.append((tid, school, vals, bt_data[key]))
        else:
            # Try common name variations
            for variant in [
                key.replace("state", "st."),
                key.replace("st.", "state"),
                key.replace("-", " "),
            ]:
                if variant in bt_keys:
                    matched.append((tid, school, vals, bt_data[variant]))
                    break

    return matched


def print_results(
    ratings: Dict[int, Dict],
    team_info: Dict[int, Dict[str, Any]],
    label: str,
    barthag_exp: float = 11.5,
):
    """Print top 25, league averages, and Duke's row."""
    # Add BARTHAG and team name
    enriched = []
    for tid, vals in ratings.items():
        if vals["games_played"] == 0:
            continue
        info = team_info.get(tid, {})
        barthag = compute_barthag(vals["adj_oe"], vals["adj_de"], exp=barthag_exp)
        enriched.append({
            "tid": tid,
            "team": info.get("school", f"Team {tid}"),
            "conf": info.get("conference", ""),
            "adj_oe": vals["adj_oe"],
            "adj_de": vals["adj_de"],
            "adj_tempo": vals.get("adj_tempo", 0),
            "barthag": barthag,
            "adj_margin": vals["adj_oe"] - vals["adj_de"],
            "gp": vals["games_played"],
            "sos_oe": vals.get("sos_oe", 0),
            "sos_de": vals.get("sos_de", 0),
            "iters": vals.get("iterations", 0),
        })

    enriched.sort(key=lambda x: x["barthag"], reverse=True)

    # League averages (games-weighted)
    total_gp = sum(e["gp"] for e in enriched)
    avg_oe = sum(e["adj_oe"] * e["gp"] for e in enriched) / total_gp if total_gp > 0 else 0
    avg_de = sum(e["adj_de"] * e["gp"] for e in enriched) / total_gp if total_gp > 0 else 0
    simple_avg_oe = sum(e["adj_oe"] for e in enriched) / len(enriched) if enriched else 0
    simple_avg_de = sum(e["adj_de"] for e in enriched) / len(enriched) if enriched else 0

    print(f"\n{'='*80}")
    print(f"  {label}")
    print(f"{'='*80}")
    print(f"  Teams: {len(enriched)}  |  Iterations: {enriched[0]['iters'] if enriched else 0}")
    print(f"  League avg (games-weighted): adj_oe={avg_oe:.4f}  adj_de={avg_de:.4f}")
    print(f"  League avg (simple mean):    adj_oe={simple_avg_oe:.4f}  adj_de={simple_avg_de:.4f}")

    # Top 25
    print(f"\n  {'Rk':>3} {'Team':<25} {'Conf':<8} {'BARTHAG':>8} {'AdjOE':>7} {'AdjDE':>7} {'Margin':>7} {'Tempo':>6} {'GP':>4}")
    print(f"  {'-'*3} {'-'*25} {'-'*8} {'-'*8} {'-'*7} {'-'*7} {'-'*7} {'-'*6} {'-'*4}")
    for i, e in enumerate(enriched[:25]):
        print(
            f"  {i+1:>3} {e['team']:<25} {e['conf']:<8} {e['barthag']:>8.4f} "
            f"{e['adj_oe']:>7.2f} {e['adj_de']:>7.2f} {e['adj_margin']:>7.2f} "
            f"{e['adj_tempo']:>6.1f} {e['gp']:>4}"
        )

    # Duke's row
    duke = [e for e in enriched if "duke" in e["team"].lower()]
    if duke:
        d = duke[0]
        rank = next(i + 1 for i, e in enumerate(enriched) if e["tid"] == d["tid"])
        print(f"\n  Duke: Rank #{rank}  BARTHAG={d['barthag']:.4f}  "
              f"AdjOE={d['adj_oe']:.2f}  AdjDE={d['adj_de']:.2f}  "
              f"Margin={d['adj_margin']:.2f}  Tempo={d['adj_tempo']:.1f}  GP={d['gp']}")

    return enriched


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser(description="Tune adjusted efficiency parameters")
    parser.add_argument("--season", type=int, default=2025)
    parser.add_argument("--barttorvik-csv", type=str, default=None,
                        help="Path to BartTorvik CSV for correlation analysis")
    parser.add_argument("--config", type=str, default="config.yaml")
    args = parser.parse_args()

    cfg = load_config(args.config)
    s3 = S3IO(cfg.bucket, cfg.region)

    # --- Load data ---
    print("\n[1/6] Loading data from S3...")
    d1_ids = _load_d1_team_ids(s3, cfg)
    team_info = _load_team_info(s3, cfg)
    games_by_date = _load_box_score_games(s3, cfg, args.season, d1_ids)
    total_games = sum(len(v) for v in games_by_date.values())
    print(f"  Loaded {total_games} game observations across {len(games_by_date)} dates")
    print(f"  D1 teams: {len(d1_ids)}")

    # --- Compute empirical HCA ---
    print("\n[2/6] Computing empirical home court advantage...")
    hca_oe, hca_de, hca_total, n_hca_games = compute_empirical_hca(games_by_date)
    print(f"  Non-neutral games: {n_hca_games}")
    print(f"  HCA total: {hca_total:.3f} pts/100 poss")
    print(f"  HCA OE:    +{hca_oe:.3f} (home team OE boost)")
    print(f"  HCA DE:    -{hca_de:.3f} (home team DE reduction)")

    # --- Load BartTorvik reference (optional) ---
    bt_data: Dict[str, Dict[str, float]] = {}
    if args.barttorvik_csv:
        print(f"\n[3/6] Loading BartTorvik reference from {args.barttorvik_csv}...")
        bt_data = load_barttorvik_csv(args.barttorvik_csv)
        if bt_data:
            print(f"  Loaded {len(bt_data)} teams")
        else:
            print("  WARNING: Failed to load BartTorvik data")
    else:
        print("\n[3/6] No BartTorvik CSV provided (use --barttorvik-csv to enable correlation)")

    # --- Half-life grid search ---
    print("\n[4/6] Half-life grid search...")
    half_lives = [15, 20, 30, 45, 60, 999]
    hl_results: Dict[float, Dict[int, Dict]] = {}

    for hl in half_lives:
        result = run_end_of_season(games_by_date, hl, hca_oe, hca_de)
        hl_results[hl] = result

        # Compute metrics
        teams_with_games = {tid: v for tid, v in result.items() if v["games_played"] > 0}
        n = len(teams_with_games)
        total_gp = sum(v["games_played"] for v in teams_with_games.values())
        avg_oe = sum(v["adj_oe"] * v["games_played"] for v in teams_with_games.values()) / total_gp
        avg_de = sum(v["adj_de"] * v["games_played"] for v in teams_with_games.values()) / total_gp
        iters = next(iter(teams_with_games.values()))["iterations"]
        max_oe = max(v["adj_oe"] for v in teams_with_games.values())
        min_oe = min(v["adj_oe"] for v in teams_with_games.values())
        oe_spread = max_oe - min_oe

        # Correlation with BartTorvik if available
        corr_str = ""
        if bt_data:
            matched = match_teams(result, team_info, bt_data)
            if len(matched) >= 10:
                our_oe = [m[2]["adj_oe"] for m in matched]
                bt_oe = [m[3].get("adj_oe", 0) for m in matched]
                our_de = [m[2]["adj_de"] for m in matched]
                bt_de = [m[3].get("adj_de", 0) for m in matched]
                r_oe = pearson_r(our_oe, bt_oe)
                r_de = pearson_r(our_de, bt_de)
                corr_str = f"  r(OE)={r_oe:.4f}  r(DE)={r_de:.4f}  matched={len(matched)}"

        hl_label = f"{hl:>5}" if hl < 999 else "  inf"
        print(f"  hl={hl_label}d: teams={n} iters={iters:>3} "
              f"avg_oe={avg_oe:.4f} avg_de={avg_de:.4f} "
              f"spread={oe_spread:.2f}{corr_str}")

    # --- BARTHAG exponent search ---
    print("\n[5/6] BARTHAG exponent comparison...")
    barthag_exps = [10.0, 10.5, 11.0, 11.5, 12.0, 12.5, 13.0]

    # Use default half-life (30d) for BARTHAG comparison
    base_result = hl_results[30]
    teams_sorted = sorted(
        [(tid, v) for tid, v in base_result.items() if v["games_played"] > 0],
        key=lambda x: x[1]["adj_oe"] - x[1]["adj_de"],
        reverse=True,
    )

    for bexp in barthag_exps:
        barthags = [compute_barthag(v["adj_oe"], v["adj_de"], exp=bexp) for _, v in teams_sorted]
        top1 = barthags[0]
        bottom1 = barthags[-1]
        median = sorted(barthags)[len(barthags) // 2]

        # Correlation with BartTorvik BARTHAG
        corr_str = ""
        if bt_data:
            matched = match_teams(base_result, team_info, bt_data)
            if len(matched) >= 10:
                our_b = [compute_barthag(m[2]["adj_oe"], m[2]["adj_de"], exp=bexp) for m in matched]
                bt_b = [m[3].get("barthag", 0) for m in matched if "barthag" in m[3]]
                if len(bt_b) >= 10:
                    r_b = pearson_r(our_b[:len(bt_b)], bt_b)
                    corr_str = f"  r(BARTHAG)={r_b:.4f}"

        print(f"  exp={bexp:>5.1f}: top={top1:.4f} median={median:.4f} "
              f"bottom={bottom1:.4f} spread={top1-bottom1:.4f}{corr_str}")

    # --- Print final results ---
    print("\n[6/6] Final results (half_life=30d, barthag_exp=11.5)...")

    # Determine best half-life
    best_hl = 30  # default
    if bt_data:
        best_r = -1
        for hl, result in hl_results.items():
            matched = match_teams(result, team_info, bt_data)
            if len(matched) >= 10:
                our_oe = [m[2]["adj_oe"] for m in matched]
                bt_oe = [m[3].get("adj_oe", 0) for m in matched]
                r = pearson_r(our_oe, bt_oe)
                if r > best_r:
                    best_r = r
                    best_hl = hl
        print(f"  Best half-life by BartTorvik correlation: {best_hl}d (r={best_r:.4f})")
    else:
        print(f"  Using default half_life=30d (provide --barttorvik-csv for optimization)")

    best_barthag_exp = 11.5  # Pomeroy standard

    # Print full results for the best parameters
    best_result = hl_results[best_hl]
    enriched = print_results(
        best_result, team_info,
        f"Final Ratings (hl={best_hl}d, hca={hca_total:.2f}, barthag_exp={best_barthag_exp})",
        barthag_exp=best_barthag_exp,
    )

    # --- Summary of recommended parameters ---
    print(f"\n{'='*80}")
    print("  RECOMMENDED PARAMETERS")
    print(f"{'='*80}")
    print(f"  half_life:    {best_hl}")
    print(f"  hca_oe:       {hca_oe:.4f}")
    print(f"  hca_de:       {hca_de:.4f}")
    print(f"  barthag_exp:  {best_barthag_exp}")
    print(f"\n  config.yaml snippet:")
    print(f"    gold:")
    print(f"      adjusted_efficiencies:")
    print(f"        half_life: {float(best_hl)}")
    print(f"        hca_oe: {round(hca_oe, 4)}")
    print(f"        hca_de: {round(hca_de, 4)}")
    print(f"        barthag_exp: {best_barthag_exp}")


if __name__ == "__main__":
    main()
