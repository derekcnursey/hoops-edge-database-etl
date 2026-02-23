#!/usr/bin/env python3
"""Verify production solver against KenPom 2026 data after fix."""
from __future__ import annotations
import csv, math, sys
from pathlib import Path
from typing import Dict

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from cbbd_etl.config import load_config
from cbbd_etl.gold.adjusted_efficiencies import (
    _get_margin_cap, _get_rating_params, _load_box_score_games,
    _load_d1_team_ids, _load_team_info, _apply_margin_cap,
)
from cbbd_etl.gold.iterative_ratings import GameObs, solve_ratings
from cbbd_etl.s3_io import S3IO

KENPOM_PATH = "/Users/dereknursey/Desktop/ml_projects/summary26.csv"

# Quick name map for top teams
KP_MAP = {
    "N.C. State": "NC State", "Connecticut": "UConn", "St. John's": "St. John's",
    "Iowa St.": "Iowa State", "Michigan St.": "Michigan State", "Kansas St.": "Kansas State",
    "Ohio St.": "Ohio State", "Penn St.": "Penn State", "Boise St.": "Boise State",
    "Utah St.": "Utah State", "Oklahoma St.": "Oklahoma State", "Oregon St.": "Oregon State",
    "Arizona St.": "Arizona State", "Colorado St.": "Colorado State",
    "Washington St.": "Washington State", "Mississippi St.": "Mississippi State",
    "Fresno St.": "Fresno State", "San Diego St.": "San Diego State",
    "Wichita St.": "Wichita State", "UMass": "Massachusetts",
}

def _std(vals):
    n = len(vals)
    m = sum(vals) / n
    return (sum((v - m)**2 for v in vals) / n) ** 0.5

def main():
    cfg = load_config("config.yaml")
    s3 = S3IO(cfg.bucket, cfg.region)
    params = _get_rating_params(cfg)
    margin_cap = _get_margin_cap(cfg)

    print(f"Config: half_life={params['half_life']}, hca_oe={params['hca_oe']:.4f}, margin_cap={margin_cap}")

    # Load KenPom
    kp = {}
    with open(KENPOM_PATH) as f:
        for row in csv.DictReader(f):
            kp[row["TeamName"].strip()] = {
                "adj_oe": float(row["AdjOE"]), "adj_de": float(row["AdjDE"]),
                "adj_em": float(row["AdjEM"]),
            }

    # Load game data
    print("Loading game data...")
    d1_ids = _load_d1_team_ids(s3, cfg)
    team_info = _load_team_info(s3, cfg)
    games_by_date = _load_box_score_games(s3, cfg, 2026, d1_ids)

    if margin_cap:
        games_by_date = _apply_margin_cap(games_by_date, margin_cap)

    # Flatten with config half_life
    all_games = []
    for dt_str, day_games in games_by_date.items():
        for g in day_games:
            all_games.append(GameObs(
                game_id=g.game_id, team_id=g.team_id, opp_id=g.opp_id,
                team_pts=g.team_pts, team_poss=g.team_poss,
                opp_pts=g.opp_pts, opp_poss=g.opp_poss,
                is_home=g.is_home, is_neutral=g.is_neutral,
                game_date=g.game_date, weight=1.0,  # null half_life → equal weight
            ))

    print(f"Running production solver on {len(all_games)} obs...")
    result = solve_ratings(all_games, hca_oe=params["hca_oe"], hca_de=params["hca_de"])

    # Match team names
    our_name_to_tid = {}
    for tid, info in team_info.items():
        name = info.get("school")
        if name:
            our_name_to_tid[name.strip()] = tid

    matched = {}
    for kp_name in kp:
        mapped = KP_MAP.get(kp_name, kp_name)
        if mapped in our_name_to_tid:
            matched[our_name_to_tid[mapped]] = kp_name
        else:
            for our_name, tid in our_name_to_tid.items():
                if our_name.lower() == kp_name.lower():
                    matched[tid] = kp_name
                    break

    # Stats
    active = {k: v for k, v in result.items() if v["games_played"] > 0}
    ems = [v["adj_oe"] - v["adj_de"] for v in active.values()]
    kp_ems = [v["adj_em"] for v in kp.values()]

    our_matched_ems = []
    kp_matched_ems = []
    abs_err_em = []
    for tid, kp_name in matched.items():
        if tid not in result or result[tid]["games_played"] == 0:
            continue
        our_em = result[tid]["adj_oe"] - result[tid]["adj_de"]
        kp_em = kp[kp_name]["adj_em"]
        our_matched_ems.append(our_em)
        kp_matched_ems.append(kp_em)
        abs_err_em.append(abs(our_em - kp_em))

    n = len(our_matched_ems)
    m_our = sum(our_matched_ems) / n
    m_kp = sum(kp_matched_ems) / n
    cov = sum((a - m_our)*(b - m_kp) for a,b in zip(our_matched_ems, kp_matched_ems)) / n
    s_our = (sum((a - m_our)**2 for a in our_matched_ems) / n) ** 0.5
    s_kp = (sum((b - m_kp)**2 for b in kp_matched_ems) / n) ** 0.5
    r = cov / (s_our * s_kp)

    print(f"\n{'='*70}")
    print(f"  PRODUCTION SOLVER vs KENPOM — 2026")
    print(f"{'='*70}")
    print(f"  Our std(em):     {_std(ems):.2f}")
    print(f"  KenPom std(em):  {_std(kp_ems):.2f}")
    print(f"  Scale:           {_std(ems) / _std(kp_ems):.2f}x")
    print(f"  Correlation:     {r:.4f}")
    print(f"  MAE(em):         {sum(abs_err_em)/n:.2f}")
    print(f"  Matched teams:   {n}")
    print(f"  mean(adj_oe):    {sum(v['adj_oe'] for v in active.values())/len(active):.2f}")
    print(f"  mean(adj_de):    {sum(v['adj_de'] for v in active.values())/len(active):.2f}")

    # Top 25 comparison
    print(f"\n  {'Team':<22} {'our_oe':>7} {'kp_oe':>7} {'our_de':>7} {'kp_de':>7} {'our_em':>7} {'kp_em':>7} {'err':>6}")
    print(f"  {'-'*22} {'-'*7} {'-'*7} {'-'*7} {'-'*7} {'-'*7} {'-'*7} {'-'*6}")
    rows = []
    for tid, kp_name in matched.items():
        if tid not in result or result[tid]["gp" if "gp" in result[tid] else "games_played"] == 0:
            continue
        r_v = result[tid]
        k = kp[kp_name]
        our_em = r_v["adj_oe"] - r_v["adj_de"]
        rows.append((kp_name, r_v["adj_oe"], k["adj_oe"], r_v["adj_de"], k["adj_de"], our_em, k["adj_em"]))
    rows.sort(key=lambda x: -x[6])  # sort by KP margin
    for name, o_oe, k_oe, o_de, k_de, o_em, k_em in rows[:25]:
        print(f"  {name:<22} {o_oe:>7.1f} {k_oe:>7.1f} {o_de:>7.1f} {k_de:>7.1f} {o_em:>+7.1f} {k_em:>+7.1f} {o_em-k_em:>+6.1f}")


if __name__ == "__main__":
    main()
