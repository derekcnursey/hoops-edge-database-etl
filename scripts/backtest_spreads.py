#!/usr/bin/env python3
"""Backtest adjusted efficiency ratings against actual spreads.

Loads ratings, lines, and game results, then computes:
- Spread prediction accuracy (MAE)
- Error distribution
- Closing line value
- Simulated betting ROI at various thresholds
- Garbage time impact comparison
- Margin cap optimization

Usage:
    poetry run python scripts/backtest_spreads.py --season 2025
"""

from __future__ import annotations

import io
import logging
import math
import sys
from bisect import bisect_right
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from cbbd_etl.config import load_config
from cbbd_etl.gold._io_helpers import (
    dedup_by,
    pydict_get,
    pydict_get_first,
    read_silver_table,
)
from cbbd_etl.gold.adjusted_efficiencies import (
    _get_rating_params,
    _load_box_score_games,
    _load_d1_team_ids,
    _load_team_info,
    _run_per_date_ratings,
)
from cbbd_etl.gold.iterative_ratings import GameObs
from cbbd_etl.s3_io import S3IO

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------


def read_gold_table(s3: S3IO, cfg, table_name: str, season: int) -> pa.Table:
    """Read a gold-layer Parquet table from S3 (latest asof)."""
    prefix = f"{cfg.s3_layout['gold_prefix']}/{table_name}/season={season}/"
    keys = s3.list_keys(prefix)
    parquet_keys = sorted(
        [k for k in keys if k.endswith(".parquet")], reverse=True
    )
    if not parquet_keys:
        return pa.table({})

    # Pick latest asof
    latest_asof = None
    for k in parquet_keys:
        for part in k.split("/"):
            if part.startswith("asof="):
                asof = part.split("=")[1]
                if latest_asof is None or asof > latest_asof:
                    latest_asof = asof
                break
    if latest_asof:
        parquet_keys = [k for k in parquet_keys if f"asof={latest_asof}" in k]

    tables = []
    for k in parquet_keys:
        data = s3.get_object_bytes(k)
        tables.append(pq.read_table(io.BytesIO(data)))
    return pa.concat_tables(tables, promote_options="permissive") if tables else pa.table({})


def build_rating_lookup(
    gold_table: pa.Table,
) -> Dict[int, List[Tuple[str, Dict[str, float]]]]:
    """Build {teamId: [(rating_date, {adj_oe, adj_de, adj_tempo})]} sorted by date."""
    lookup: Dict[int, List[Tuple[str, Dict[str, float]]]] = defaultdict(list)
    tids = pydict_get(gold_table, "teamId")
    dates = pydict_get(gold_table, "rating_date")
    adj_oes = pydict_get(gold_table, "adj_oe")
    adj_des = pydict_get(gold_table, "adj_de")
    adj_tempos = pydict_get(gold_table, "adj_tempo")

    for i in range(len(tids)):
        if tids[i] is None or dates[i] is None:
            continue
        tid = int(tids[i])
        dt = str(dates[i])[:10]
        lookup[tid].append((dt, {
            "adj_oe": float(adj_oes[i]) if adj_oes[i] is not None else 100.0,
            "adj_de": float(adj_des[i]) if adj_des[i] is not None else 100.0,
            "adj_tempo": float(adj_tempos[i]) if adj_tempos[i] is not None else 67.0,
        }))

    for tid in lookup:
        lookup[tid].sort(key=lambda x: x[0])
    return dict(lookup)


def build_lookup_from_solver(
    games_by_date: Dict[str, List[GameObs]],
    team_info: Dict[int, Dict],
    season: int,
    params: Dict[str, float],
) -> Dict[int, List[Tuple[str, Dict[str, float]]]]:
    """Run full per-date solver and return a rating lookup."""
    records = _run_per_date_ratings(games_by_date, team_info, season, **params)
    lookup: Dict[int, List[Tuple[str, Dict[str, float]]]] = defaultdict(list)
    for r in records:
        tid = r["teamId"]
        dt = r["rating_date"]
        lookup[tid].append((dt, {
            "adj_oe": r["adj_oe"],
            "adj_de": r["adj_de"],
            "adj_tempo": r["adj_tempo"],
        }))
    for tid in lookup:
        lookup[tid].sort(key=lambda x: x[0])
    return dict(lookup)


def get_rating_before_date(
    lookup: Dict[int, List[Tuple[str, Dict[str, float]]]],
    team_id: int,
    game_date: str,
) -> Optional[Dict[str, float]]:
    """Get team's rating from the most recent date strictly before game_date."""
    if team_id not in lookup:
        return None
    timeline = lookup[team_id]
    dates = [t[0] for t in timeline]
    idx = bisect_right(dates, game_date) - 1
    # Must be strictly before game_date
    while idx >= 0 and dates[idx] >= game_date:
        idx -= 1
    if idx < 0:
        return None
    return timeline[idx][1]


def load_matchups(
    s3: S3IO, cfg, season: int, d1_ids: Set[int]
) -> Tuple[List[Dict], float]:
    """Load fct_games + fct_lines, return (matchups, hca_game_pts)."""
    fct_games = dedup_by(
        read_silver_table(s3, cfg, "fct_games", season=season), ["gameId"]
    )
    fct_lines = read_silver_table(s3, cfg, "fct_lines", season=season)

    g_ids = pydict_get(fct_games, "gameId")
    g_starts = pydict_get_first(fct_games, ["startDate", "startTime", "date"])
    g_home = pydict_get(fct_games, "homeTeamId")
    g_away = pydict_get(fct_games, "awayTeamId")
    g_neutral = pydict_get(fct_games, "neutralSite")
    g_hs = pydict_get_first(fct_games, ["homeScore", "homePoints"])
    g_as = pydict_get_first(fct_games, ["awayScore", "awayPoints"])

    games: Dict[int, Dict] = {}
    for i, gid_raw in enumerate(g_ids):
        if gid_raw is None:
            continue
        gid = int(gid_raw)
        home_id = int(g_home[i]) if g_home[i] is not None else 0
        away_id = int(g_away[i]) if g_away[i] is not None else 0
        if home_id not in d1_ids or away_id not in d1_ids:
            continue
        hp, ap = g_hs[i], g_as[i]
        if hp is None or ap is None:
            continue
        dt = str(g_starts[i])[:10] if g_starts[i] else None
        if dt is None:
            continue
        games[gid] = {
            "gameId": gid, "homeTeamId": home_id, "awayTeamId": away_id,
            "homePoints": float(hp), "awayPoints": float(ap),
            "neutralSite": bool(g_neutral[i]) if g_neutral[i] is not None else False,
            "date": dt,
        }

    # Average spread across providers per game
    l_gids = pydict_get(fct_lines, "gameId")
    l_spreads = pydict_get(fct_lines, "spread")
    spread_accum: Dict[int, List[float]] = defaultdict(list)
    for i in range(len(l_gids)):
        if l_gids[i] is None or l_spreads[i] is None:
            continue
        try:
            spread_accum[int(l_gids[i])].append(float(l_spreads[i]))
        except (ValueError, TypeError):
            pass

    matchups = []
    for gid, g in games.items():
        if gid in spread_accum:
            g["book_spread"] = sum(spread_accum[gid]) / len(spread_accum[gid])
            g["actual_margin"] = g["homePoints"] - g["awayPoints"]
            matchups.append(g)

    # HCA in game points
    margins = [m["actual_margin"] for m in matchups if not m["neutralSite"]]
    hca_game = sum(margins) / len(margins) if margins else 3.5
    return matchups, hca_game


# ---------------------------------------------------------------------------
# Prediction & backtest
# ---------------------------------------------------------------------------


def compute_league_avg_eff(lookup: Dict) -> float:
    """Get league average efficiency from the last available date per team."""
    vals = []
    for tid, timeline in lookup.items():
        if timeline:
            r = timeline[-1][1]
            vals.append(r["adj_oe"])
            vals.append(r["adj_de"])
    return sum(vals) / len(vals) if vals else 100.0


def predict_home_margin(
    home_oe: float, home_de: float, home_tempo: float,
    away_oe: float, away_de: float, away_tempo: float,
    league_avg_tempo: float, hca_pts: float,
    league_avg_eff: float = 100.0,
) -> float:
    """Pomeroy-style predicted home margin in game points."""
    home_expected = home_oe * away_de / league_avg_eff
    away_expected = away_oe * home_de / league_avg_eff
    margin_per_100 = home_expected - away_expected
    pace = home_tempo * away_tempo / league_avg_tempo if league_avg_tempo > 0 else 67.0
    return margin_per_100 * pace / 100.0 + hca_pts


def compute_league_avg_tempo(lookup: Dict) -> float:
    """Get league average tempo from the last available date per team."""
    tempos = []
    for tid, timeline in lookup.items():
        if timeline:
            tempos.append(timeline[-1][1]["adj_tempo"])
    return sum(tempos) / len(tempos) if tempos else 67.0


def run_backtest(
    matchups: List[Dict],
    lookup: Dict,
    league_avg_tempo: float,
    hca_game_pts: float,
    label: str = "",
    league_avg_eff: float = 100.0,
) -> Dict:
    """Run full backtest against closing lines. Returns metrics dict."""
    predictions = []
    for g in matchups:
        hr = get_rating_before_date(lookup, g["homeTeamId"], g["date"])
        ar = get_rating_before_date(lookup, g["awayTeamId"], g["date"])
        if hr is None or ar is None:
            continue
        hca = 0.0 if g["neutralSite"] else hca_game_pts
        model_margin = predict_home_margin(
            hr["adj_oe"], hr["adj_de"], hr["adj_tempo"],
            ar["adj_oe"], ar["adj_de"], ar["adj_tempo"],
            league_avg_tempo, hca, league_avg_eff,
        )
        predictions.append({
            "model_margin": model_margin,
            "book_spread": g["book_spread"],
            "actual_margin": g["actual_margin"],
            "book_predicted_margin": -g["book_spread"],
        })

    if not predictions:
        return {"label": label, "n_games": 0, "roi_table": []}

    # Filter out NaN/Inf predictions (can happen with 0-possession games)
    predictions = [
        p for p in predictions
        if math.isfinite(p["model_margin"])
    ]

    n = len(predictions)
    if n == 0:
        return {"label": label, "n_games": 0, "roi_table": []}

    model_errors = [p["model_margin"] - p["actual_margin"] for p in predictions]
    book_errors = [p["book_predicted_margin"] - p["actual_margin"] for p in predictions]

    model_mae = sum(abs(e) for e in model_errors) / n
    book_mae = sum(abs(e) for e in book_errors) / n
    model_mean = sum(model_errors) / n
    book_mean = sum(book_errors) / n
    model_std = (sum((e - model_mean) ** 2 for e in model_errors) / n) ** 0.5
    book_std = (sum((e - book_mean) ** 2 for e in book_errors) / n) ** 0.5

    # CLV: games where |model - book| > 3
    clv_games = [
        p for p in predictions
        if abs(p["model_margin"] - p["book_predicted_margin"]) > 3
    ]
    clv_wins = sum(
        1 for p in clv_games
        if abs(p["model_margin"] - p["actual_margin"])
        < abs(p["book_predicted_margin"] - p["actual_margin"])
    )
    clv_total = len(clv_games)

    # Simulated betting ROI
    roi_table = []
    for thresh in [1, 2, 3, 4, 5, 6, 7]:
        wins, losses, pushes = 0, 0, 0
        for p in predictions:
            diff = p["model_margin"] - p["book_predicted_margin"]
            if abs(diff) < thresh:
                continue
            ats = p["actual_margin"] + p["book_spread"]
            if diff > 0:  # bet home to cover
                if ats > 0:
                    wins += 1
                elif ats < 0:
                    losses += 1
                else:
                    pushes += 1
            else:  # bet away to cover
                if ats < 0:
                    wins += 1
                elif ats > 0:
                    losses += 1
                else:
                    pushes += 1
        resolved = wins + losses
        profit = wins * 100 - losses * 110
        risked = resolved * 110
        roi_table.append({
            "threshold": thresh,
            "bets": wins + losses + pushes,
            "wins": wins, "losses": losses, "pushes": pushes,
            "win_rate": wins / resolved * 100 if resolved else 0,
            "roi": profit / risked * 100 if risked else 0,
        })

    return {
        "label": label, "n_games": n,
        "model_mae": model_mae, "book_mae": book_mae,
        "model_mean": model_mean, "book_mean": book_mean,
        "model_std": model_std, "book_std": book_std,
        "clv_total": clv_total, "clv_wins": clv_wins,
        "clv_rate": clv_wins / clv_total if clv_total else 0,
        "roi_table": roi_table,
    }


# ---------------------------------------------------------------------------
# Margin cap
# ---------------------------------------------------------------------------


def apply_margin_cap(
    games_by_date: Dict[str, List[GameObs]], cap: float
) -> Dict[str, List[GameObs]]:
    """Cap point margin at ±cap, splitting excess evenly between teams."""
    capped: Dict[str, List[GameObs]] = {}
    for dt, day_games in games_by_date.items():
        cg = []
        for g in day_games:
            margin = g.team_pts - g.opp_pts
            if abs(margin) > cap:
                excess = abs(margin) - cap
                if margin > 0:
                    tp, op = g.team_pts - excess / 2, g.opp_pts + excess / 2
                else:
                    tp, op = g.team_pts + excess / 2, g.opp_pts - excess / 2
                cg.append(GameObs(
                    game_id=g.game_id, team_id=g.team_id, opp_id=g.opp_id,
                    team_pts=tp, team_poss=g.team_poss,
                    opp_pts=op, opp_poss=g.opp_poss,
                    is_home=g.is_home, is_neutral=g.is_neutral,
                    game_date=g.game_date, weight=g.weight,
                ))
            else:
                cg.append(g)
        capped[dt] = cg
    return capped


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------


def print_backtest(r: Dict):
    """Print backtest results for one variant."""
    if r["n_games"] == 0:
        print(f"  {r['label']}: no games")
        return
    print(f"\n  --- {r['label']} ({r['n_games']} games) ---")
    edge = "YES" if r["model_mae"] < r["book_mae"] else "no"
    print(f"  MAE: model={r['model_mae']:.3f}  book={r['book_mae']:.3f}  edge={edge}")
    print(f"  Error mean: model={r['model_mean']:.3f}  book={r['book_mean']:.3f}")
    print(f"  Error std:  model={r['model_std']:.3f}  book={r['book_std']:.3f}")
    print(f"  CLV (|diff|>3): {r['clv_wins']}/{r['clv_total']}"
          f" = {r['clv_rate']:.1%}")
    print(f"\n  {'Thresh':>6} {'Bets':>5} {'W':>4} {'L':>4} {'P':>3}"
          f" {'Win%':>6} {'ROI%':>7}")
    print(f"  {'-'*6} {'-'*5} {'-'*4} {'-'*4} {'-'*3} {'-'*6} {'-'*7}")
    for row in r["roi_table"]:
        print(f"  {row['threshold']:>6} {row['bets']:>5} {row['wins']:>4}"
              f" {row['losses']:>4} {row['pushes']:>3}"
              f" {row['win_rate']:>5.1f}% {row['roi']:>6.1f}%")


def save_multi_season_report(
    season_results: Dict[int, Dict],
    path: str,
):
    """Save multi-season backtest report to markdown."""
    lines = ["# Multi-Season Backtest: Adjusted Efficiency Ratings vs Spreads\n\n"]

    # Aggregate across seasons
    agg_games = 0
    agg_model_abs_err = 0.0
    agg_book_abs_err = 0.0
    agg_clv_wins = 0
    agg_clv_total = 0
    agg_roi_bets = defaultdict(lambda: {"w": 0, "l": 0, "p": 0})

    lines.append("## Per-Season Summary\n\n")
    lines.append("| Season | Games | Model MAE | Book MAE | CLV% | ROI@3 | ROI@5 | HCA |\n")
    lines.append("|--------|-------|-----------|----------|------|-------|-------|-----|\n")

    for season in sorted(season_results.keys()):
        sr = season_results[season]
        r = sr["result"]
        if r["n_games"] == 0:
            continue
        roi3 = next((x["roi"] for x in r["roi_table"] if x["threshold"] == 3), 0)
        roi5 = next((x["roi"] for x in r["roi_table"] if x["threshold"] == 5), 0)
        lines.append(
            f"| {season} | {r['n_games']} | {r['model_mae']:.3f} |"
            f" {r['book_mae']:.3f} | {r['clv_rate']:.1%} |"
            f" {roi3:.1f}% | {roi5:.1f}% | {sr['hca']:.2f} |\n"
        )

        agg_games += r["n_games"]
        agg_model_abs_err += r["model_mae"] * r["n_games"]
        agg_book_abs_err += r["book_mae"] * r["n_games"]
        agg_clv_wins += r["clv_wins"]
        agg_clv_total += r["clv_total"]
        for row in r["roi_table"]:
            t = row["threshold"]
            agg_roi_bets[t]["w"] += row["wins"]
            agg_roi_bets[t]["l"] += row["losses"]
            agg_roi_bets[t]["p"] += row["pushes"]

    # Aggregate row
    if agg_games > 0:
        agg_model_mae = agg_model_abs_err / agg_games
        agg_book_mae = agg_book_abs_err / agg_games
        agg_clv_rate = agg_clv_wins / agg_clv_total if agg_clv_total else 0
        agg_roi3 = _compute_roi(agg_roi_bets[3])
        agg_roi5 = _compute_roi(agg_roi_bets[5])
        lines.append(
            f"| **TOTAL** | **{agg_games}** | **{agg_model_mae:.3f}** |"
            f" **{agg_book_mae:.3f}** | **{agg_clv_rate:.1%}** |"
            f" **{agg_roi3:.1f}%** | **{agg_roi5:.1f}%** | — |\n"
        )

    # Full ROI table across all seasons
    lines.append("\n## Aggregate Betting ROI (-110 juice, all seasons)\n\n")
    lines.append("| Threshold | Bets | W | L | P | Win% | ROI% |\n")
    lines.append("|-----------|------|---|---|---|------|------|\n")
    for thresh in [1, 2, 3, 4, 5, 6, 7]:
        d = agg_roi_bets[thresh]
        w, l, p = d["w"], d["l"], d["p"]
        resolved = w + l
        win_pct = w / resolved * 100 if resolved else 0
        profit = w * 100 - l * 110
        risked = resolved * 110
        roi = profit / risked * 100 if risked else 0
        lines.append(f"| {thresh} | {w+l+p} | {w} | {l} | {p} | {win_pct:.1f}% | {roi:.1f}% |\n")

    with open(path, "w") as f:
        f.writelines(lines)
    print(f"\n  Report saved to {path}")


def _compute_roi(d: Dict) -> float:
    """Compute ROI from {w, l, p} dict."""
    w, l = d["w"], d["l"]
    resolved = w + l
    if resolved == 0:
        return 0.0
    return (w * 100 - l * 110) / (resolved * 110) * 100


def save_report(results: List[Dict], hca_game: float, path: str, season: int = 2025):
    """Save backtest report to markdown."""
    lines = [f"# Backtest: Adjusted Efficiency Ratings vs Spreads (Season {season})\n"]
    lines.append(f"HCA (game points): {hca_game:.2f}\n")

    for r in results:
        if r["n_games"] == 0:
            continue
        lines.append(f"\n## {r['label']}\n")
        lines.append(f"Games with spreads: {r['n_games']}\n")
        edge = "**YES**" if r["model_mae"] < r["book_mae"] else "no"
        lines.append(f"### Spread Prediction Accuracy\n")
        lines.append(f"| Metric | Model | Book | Edge |\n|--------|-------|------|------|\n")
        lines.append(f"| MAE | {r['model_mae']:.3f} | {r['book_mae']:.3f} | {edge} |\n")
        lines.append(f"| Mean error | {r['model_mean']:.3f} | {r['book_mean']:.3f} | |\n")
        lines.append(f"| Std dev | {r['model_std']:.3f} | {r['book_std']:.3f} | |\n")

        lines.append(f"\n### Closing Line Value (|diff| > 3 pts)\n")
        lines.append(f"Model closer to actual: {r['clv_wins']}/{r['clv_total']}"
                      f" ({r['clv_rate']:.1%})\n")

        lines.append(f"\n### Simulated Betting ROI (-110 juice)\n")
        lines.append(f"| Threshold | Bets | W | L | P | Win% | ROI% |\n")
        lines.append(f"|-----------|------|---|---|---|------|------|\n")
        for row in r["roi_table"]:
            lines.append(
                f"| {row['threshold']} | {row['bets']} | {row['wins']} |"
                f" {row['losses']} | {row['pushes']} |"
                f" {row['win_rate']:.1f}% | {row['roi']:.1f}% |\n"
            )

    # Comparison table
    if len(results) >= 2:
        lines.append("\n## Comparison Summary\n")
        lines.append("| Variant | MAE | Std | CLV% | ROI@3 | ROI@5 |\n")
        lines.append("|---------|-----|-----|------|-------|-------|\n")
        for r in results:
            if r["n_games"] == 0:
                continue
            roi3 = next((x["roi"] for x in r["roi_table"] if x["threshold"] == 3), 0)
            roi5 = next((x["roi"] for x in r["roi_table"] if x["threshold"] == 5), 0)
            lines.append(
                f"| {r['label']} | {r['model_mae']:.3f} |"
                f" {r['model_std']:.3f} | {r['clv_rate']:.1%} |"
                f" {roi3:.1f}% | {roi5:.1f}% |\n"
            )

    with open(path, "w") as f:
        f.writelines(lines)
    print(f"\n  Report saved to {path}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def run_single_season_backtest(
    s3: S3IO, cfg, season: int, d1_ids: Set[int], params: Dict,
) -> Dict:
    """Run backtest for one season using pre-built gold table. Returns result dict."""
    gold_all = read_gold_table(s3, cfg, "team_adjusted_efficiencies", season)
    if gold_all.num_rows == 0:
        return {"result": {"n_games": 0, "label": f"Season {season}", "roi_table": []}, "hca": 0}
    lookup = build_rating_lookup(gold_all)
    matchups, hca_game = load_matchups(s3, cfg, season, d1_ids)
    if not matchups:
        return {"result": {"n_games": 0, "label": f"Season {season}", "roi_table": []}, "hca": 0}
    league_tempo = compute_league_avg_tempo(lookup)
    league_eff = compute_league_avg_eff(lookup)
    result = run_backtest(matchups, lookup, league_tempo, hca_game, f"Season {season}",
                          league_avg_eff=league_eff)
    return {"result": result, "hca": hca_game}


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--season", type=int, default=2025)
    parser.add_argument("--multi-season", action="store_true",
                        help="Run backtest across all available seasons")
    parser.add_argument("--config", type=str, default="config.yaml")
    args = parser.parse_args()

    cfg = load_config(args.config)
    s3 = S3IO(cfg.bucket, cfg.region)
    params = _get_rating_params(cfg)

    if args.multi_season:
        d1_ids = _load_d1_team_ids(s3, cfg)
        prefix = f"{cfg.s3_layout['gold_prefix']}/team_adjusted_efficiencies/"
        all_keys = s3.list_keys(prefix)
        available = sorted({
            int(part.split("=")[1])
            for k in all_keys
            for part in k.split("/")
            if part.startswith("season=")
        })
        print(f"\nMulti-season backtest across {len(available)} seasons: {available}")
        season_results: Dict[int, Dict] = {}
        for ssn in available:
            print(f"\n{'='*60}")
            print(f"  SEASON {ssn}")
            print(f"{'='*60}")
            sr = run_single_season_backtest(s3, cfg, ssn, d1_ids, params)
            season_results[ssn] = sr
            r = sr["result"]
            if r["n_games"] > 0:
                roi3 = next((x["roi"] for x in r["roi_table"] if x["threshold"] == 3), 0)
                roi5 = next((x["roi"] for x in r["roi_table"] if x["threshold"] == 5), 0)
                print(f"  {r['n_games']} games | MAE={r['model_mae']:.3f} | Book={r['book_mae']:.3f} |"
                      f" CLV={r['clv_rate']:.1%} | ROI@3={roi3:.1f}% | ROI@5={roi5:.1f}%")
            else:
                print(f"  No games with spreads")

        report_path = str(Path(__file__).resolve().parent.parent / "reports" / "backtest_multi_season.md")
        save_multi_season_report(season_results, report_path)

        # Print aggregate summary
        total_games = sum(sr["result"]["n_games"] for sr in season_results.values())
        print(f"\n{'='*60}")
        print(f"  AGGREGATE: {total_games} games across {len(available)} seasons")
        print(f"{'='*60}")
        return

    season = args.season

    # ---------------------------------------------------------------
    # 1. Load gold tables
    # ---------------------------------------------------------------
    print("\n[1/7] Loading gold tables from S3...")
    gold_all = read_gold_table(s3, cfg, "team_adjusted_efficiencies", season)
    gold_ng = read_gold_table(s3, cfg, "team_adjusted_efficiencies_no_garbage", season)
    print(f"  All possessions: {gold_all.num_rows} rows")
    print(f"  No garbage:      {gold_ng.num_rows} rows")

    lookup_all = build_rating_lookup(gold_all)
    lookup_ng = build_rating_lookup(gold_ng)

    # ---------------------------------------------------------------
    # 2. Load games and lines
    # ---------------------------------------------------------------
    print("\n[2/7] Loading games and lines...")
    d1_ids = _load_d1_team_ids(s3, cfg)
    matchups, hca_game = load_matchups(s3, cfg, season, d1_ids)
    print(f"  Games with spreads: {len(matchups)}")
    print(f"  HCA (game points):  {hca_game:.2f}")

    league_tempo_all = compute_league_avg_tempo(lookup_all)
    league_tempo_ng = compute_league_avg_tempo(lookup_ng)
    league_eff_all = compute_league_avg_eff(lookup_all)
    league_eff_ng = compute_league_avg_eff(lookup_ng)
    print(f"  League avg tempo:   {league_tempo_all:.1f}")
    print(f"  League avg eff:     {league_eff_all:.1f}")

    # ---------------------------------------------------------------
    # 3. Backtest both variants
    # ---------------------------------------------------------------
    print("\n[3/7] Backtesting all-possessions variant...")
    r_all = run_backtest(matchups, lookup_all, league_tempo_all, hca_game,
                         "All Possessions", league_avg_eff=league_eff_all)
    print_backtest(r_all)

    print("\n[4/7] Backtesting no-garbage variant...")
    r_ng = run_backtest(matchups, lookup_ng, league_tempo_ng, hca_game,
                        "No Garbage Time", league_avg_eff=league_eff_ng)
    print_backtest(r_ng)

    # ---------------------------------------------------------------
    # 4. Garbage time comparison
    # ---------------------------------------------------------------
    print(f"\n{'='*60}")
    print("  GARBAGE TIME IMPACT")
    print(f"{'='*60}")
    print(f"  {'Metric':<20} {'All Poss':>12} {'No Garbage':>12} {'Winner':>10}")
    print(f"  {'-'*20} {'-'*12} {'-'*12} {'-'*10}")
    mae_w = "All" if r_all["model_mae"] < r_ng["model_mae"] else "NoGarb"
    print(f"  {'MAE':<20} {r_all['model_mae']:>12.3f} {r_ng['model_mae']:>12.3f} {mae_w:>10}")
    std_w = "All" if r_all["model_std"] < r_ng["model_std"] else "NoGarb"
    print(f"  {'Std':<20} {r_all['model_std']:>12.3f} {r_ng['model_std']:>12.3f} {std_w:>10}")
    clv_w = "All" if r_all["clv_rate"] > r_ng["clv_rate"] else "NoGarb"
    print(f"  {'CLV%':<20} {r_all['clv_rate']:>11.1%} {r_ng['clv_rate']:>11.1%} {clv_w:>10}")
    for thresh in [3, 5]:
        a = next((x for x in r_all["roi_table"] if x["threshold"] == thresh), None)
        b = next((x for x in r_ng["roi_table"] if x["threshold"] == thresh), None)
        if a and b:
            w = "All" if a["roi"] > b["roi"] else "NoGarb"
            print(f"  {'ROI@' + str(thresh):<20} {a['roi']:>11.1f}% {b['roi']:>11.1f}% {w:>10}")

    # ---------------------------------------------------------------
    # 5. Margin cap variants
    # ---------------------------------------------------------------
    print(f"\n[5/7] Loading raw game data for margin cap variants...")
    team_info = _load_team_info(s3, cfg)
    raw_games = _load_box_score_games(s3, cfg, season, d1_ids)
    print(f"  Raw games loaded: {sum(len(v) for v in raw_games.values())} obs")

    cap_results = []
    for cap in [15, 20, 25]:
        print(f"\n[6/7] Rebuilding with margin cap = {cap} pts...")
        capped = apply_margin_cap(raw_games, cap)
        cap_lookup = build_lookup_from_solver(capped, team_info, season, params)
        cap_tempo = compute_league_avg_tempo(cap_lookup)
        cap_eff = compute_league_avg_eff(cap_lookup)
        r_cap = run_backtest(matchups, cap_lookup, cap_tempo, hca_game,
                             f"Cap {cap} pts", league_avg_eff=cap_eff)
        cap_results.append(r_cap)
        print_backtest(r_cap)

    # ---------------------------------------------------------------
    # 6. Comparison of all variants
    # ---------------------------------------------------------------
    all_results = [r_all, r_ng] + cap_results
    print(f"\n{'='*80}")
    print("  FULL COMPARISON")
    print(f"{'='*80}")
    print(f"  {'Variant':<20} {'MAE':>7} {'Std':>7} {'CLV%':>6}"
          f" {'ROI@3':>7} {'ROI@5':>7} {'ROI@7':>7}")
    print(f"  {'-'*20} {'-'*7} {'-'*7} {'-'*6} {'-'*7} {'-'*7} {'-'*7}")
    best_roi5 = -999
    best_label = ""
    for r in all_results:
        if r["n_games"] == 0:
            continue
        roi3 = next((x["roi"] for x in r["roi_table"] if x["threshold"] == 3), 0)
        roi5 = next((x["roi"] for x in r["roi_table"] if x["threshold"] == 5), 0)
        roi7 = next((x["roi"] for x in r["roi_table"] if x["threshold"] == 7), 0)
        print(f"  {r['label']:<20} {r['model_mae']:>7.3f} {r['model_std']:>7.3f}"
              f" {r['clv_rate']:>5.1%} {roi3:>6.1f}% {roi5:>6.1f}% {roi7:>6.1f}%")
        if roi5 > best_roi5:
            best_roi5 = roi5
            best_label = r["label"]

    print(f"\n  Best variant by ROI@5: {best_label} ({best_roi5:.1f}%)")
    print(f"  Book MAE: {r_all['book_mae']:.3f}  Book Std: {r_all['book_std']:.3f}")

    # ---------------------------------------------------------------
    # 7. Save report
    # ---------------------------------------------------------------
    report_path = str(Path(__file__).resolve().parent.parent / "reports" / "backtest_2025.md")
    save_report(all_results, hca_game, report_path)

    # Recommend best config
    print(f"\n{'='*60}")
    print("  RECOMMENDED CONFIG UPDATE")
    print(f"{'='*60}")
    # Parse best variant
    best_cap = None
    best_garbage = "all"
    if "No Garbage" in best_label:
        best_garbage = "no_garbage"
    for cap in [15, 20, 25]:
        if str(cap) in best_label:
            best_cap = cap
    print(f"  variant:    {best_garbage}")
    if best_cap:
        print(f"  margin_cap: {best_cap}")
    else:
        print(f"  margin_cap: none")
    print(f"  ROI@5:      {best_roi5:.1f}%")


if __name__ == "__main__":
    main()
