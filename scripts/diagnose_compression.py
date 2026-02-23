#!/usr/bin/env python3
"""Diagnose compression in the iterative ratings solver.

Runs the current aggregate solver and a per-game (KenPom-style) solver
side-by-side, tracks per-iteration std dev of adj_oe, and compares to
KenPom's published 2026 data.

Usage:
    poetry run python scripts/diagnose_compression.py
"""
from __future__ import annotations

import csv
import math
import sys
from collections import defaultdict
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from cbbd_etl.config import load_config
from cbbd_etl.gold._io_helpers import dedup_by, pydict_get, pydict_get_first, read_silver_table
from cbbd_etl.gold.adjusted_efficiencies import (
    _get_margin_cap,
    _get_rating_params,
    _load_box_score_games,
    _load_d1_team_ids,
    _load_team_info,
    _apply_margin_cap,
)
from cbbd_etl.gold.iterative_ratings import GameObs, _EFF_FLOOR, _EFF_CEIL
from cbbd_etl.s3_io import S3IO

KENPOM_PATH = "/Users/dereknursey/Desktop/ml_projects/summary26.csv"


# ---------------------------------------------------------------------------
# KenPom loader
# ---------------------------------------------------------------------------

def load_kenpom(path: str) -> Dict[str, Dict]:
    kp = {}
    with open(path) as f:
        reader = csv.DictReader(f)
        for row in reader:
            kp[row["TeamName"].strip()] = {
                "adj_oe": float(row["AdjOE"]),
                "adj_de": float(row["AdjDE"]),
                "adj_em": float(row["AdjEM"]),
                "adj_tempo": float(row["AdjTempo"]),
            }
    return kp


# ---------------------------------------------------------------------------
# Flatten games with optional recency weighting
# ---------------------------------------------------------------------------

def flatten_games(
    games_by_date: Dict[str, List[GameObs]],
    half_life: Optional[float] = None,
    as_of: Optional[str] = None,
) -> List[GameObs]:
    """Flatten all games into a single list with optional recency weighting.

    If half_life is None, all games get weight=1.0.
    as_of defaults to the latest date in the data.
    """
    if not games_by_date:
        return []

    if as_of is None:
        as_of = max(games_by_date.keys())
    ref = datetime.strptime(as_of, "%Y-%m-%d").date()

    result = []
    for dt_str, day_games in games_by_date.items():
        try:
            gd = datetime.strptime(dt_str, "%Y-%m-%d").date()
        except ValueError:
            continue
        if gd > ref:
            continue
        days_ago = (ref - gd).days
        if half_life and half_life > 0:
            w = 0.5 ** (days_ago / half_life)
        else:
            w = 1.0
        for g in day_games:
            result.append(GameObs(
                game_id=g.game_id, team_id=g.team_id, opp_id=g.opp_id,
                team_pts=g.team_pts, team_poss=g.team_poss,
                opp_pts=g.opp_pts, opp_poss=g.opp_poss,
                is_home=g.is_home, is_neutral=g.is_neutral,
                game_date=g.game_date, weight=w,
            ))
    return result


# ---------------------------------------------------------------------------
# Solvers with per-iteration diagnostics
# ---------------------------------------------------------------------------

def _compute_league_avg(games: List[GameObs]) -> float:
    total_w_pts = 0.0
    total_w_poss = 0.0
    for g in games:
        if g.team_poss > 0:
            total_w_pts += g.weight * g.team_pts
            total_w_poss += g.weight * g.team_poss
    return (total_w_pts / total_w_poss * 100.0) if total_w_poss > 0 else 100.0


def _std(values: List[float]) -> float:
    n = len(values)
    if n == 0:
        return 0.0
    m = sum(values) / n
    return (sum((v - m) ** 2 for v in values) / n) ** 0.5


def solve_aggregate(
    games: List[GameObs],
    hca_oe: float = 1.4,
    hca_de: float = 1.4,
    max_iter: int = 200,
    tol: float = 0.01,
) -> Tuple[Dict[int, Dict], List[Dict]]:
    """Current aggregate solver with per-iteration diagnostics."""
    if not games:
        return {}, []

    team_ids_set = set()
    for g in games:
        team_ids_set.add(g.team_id)
        team_ids_set.add(g.opp_id)
    team_ids = sorted(team_ids_set)

    team_games: Dict[int, List[GameObs]] = {tid: [] for tid in team_ids}
    for g in games:
        team_games[g.team_id].append(g)

    league_avg = _compute_league_avg(games)

    # Compute raw OE/DE per team
    raw: Dict[int, Dict] = {}
    for tid in team_ids:
        tg = team_games[tid]
        w_pts_for = w_poss_for = w_pts_ag = w_poss_ag = 0.0
        gp = 0
        for g in tg:
            if g.team_poss <= 0:
                continue
            hca_off = hca_def = 0.0
            if not g.is_neutral:
                if g.is_home:
                    hca_off, hca_def = hca_oe, -hca_de
                else:
                    hca_off, hca_def = -hca_oe, hca_de
            w_pts_for += g.weight * (g.team_pts - hca_off * g.team_poss / 100.0)
            w_poss_for += g.weight * g.team_poss
            w_pts_ag += g.weight * (g.opp_pts - hca_def * g.opp_poss / 100.0)
            w_poss_ag += g.weight * g.opp_poss
            gp += 1
        raw_oe = max(_EFF_FLOOR, min(_EFF_CEIL, (w_pts_for / w_poss_for * 100) if w_poss_for > 0 else league_avg))
        raw_de = max(_EFF_FLOOR, min(_EFF_CEIL, (w_pts_ag / w_poss_ag * 100) if w_poss_ag > 0 else league_avg))
        raw[tid] = {"raw_oe": raw_oe, "raw_de": raw_de, "gp": gp}

    adj_oe = {tid: raw[tid]["raw_oe"] for tid in team_ids}
    adj_de = {tid: raw[tid]["raw_de"] for tid in team_ids}

    iter_stats = []
    for it in range(max_iter):
        new_oe: Dict[int, float] = {}
        new_de: Dict[int, float] = {}
        for tid in team_ids:
            if raw[tid]["gp"] == 0:
                new_oe[tid] = league_avg
                new_de[tid] = league_avg
                continue
            w_opp_de = w_opp_oe = w_total = 0.0
            for g in team_games[tid]:
                if g.team_poss <= 0:
                    continue
                w_opp_de += g.weight * adj_de.get(g.opp_id, league_avg)
                w_opp_oe += g.weight * adj_oe.get(g.opp_id, league_avg)
                w_total += g.weight
            avg_opp_de = (w_opp_de / w_total) if w_total > 0 else league_avg
            avg_opp_oe = (w_opp_oe / w_total) if w_total > 0 else league_avg
            c_oe = raw[tid]["raw_oe"] * (league_avg / avg_opp_de) if avg_opp_de > 0 else raw[tid]["raw_oe"]
            c_de = raw[tid]["raw_de"] * (league_avg / avg_opp_oe) if avg_opp_oe > 0 else raw[tid]["raw_de"]
            new_oe[tid] = c_oe if math.isfinite(c_oe) else league_avg
            new_de[tid] = c_de if math.isfinite(c_de) else league_avg

        max_delta = max(
            max(abs(new_oe[t] - adj_oe[t]) for t in team_ids),
            max(abs(new_de[t] - adj_de[t]) for t in team_ids),
        )
        adj_oe, adj_de = new_oe, new_de

        active = [t for t in team_ids if raw[t]["gp"] > 0]
        oe_vals = [adj_oe[t] for t in active]
        de_vals = [adj_de[t] for t in active]
        em_vals = [adj_oe[t] - adj_de[t] for t in active]
        iter_stats.append({
            "iter": it + 1,
            "std_oe": _std(oe_vals),
            "std_de": _std(de_vals),
            "std_em": _std(em_vals),
            "max_delta": max_delta,
        })
        if max_delta < tol:
            break

    result = {}
    for tid in team_ids:
        result[tid] = {
            "adj_oe": adj_oe[tid], "adj_de": adj_de[tid],
            "adj_em": adj_oe[tid] - adj_de[tid],
            "raw_oe": raw[tid]["raw_oe"], "raw_de": raw[tid]["raw_de"],
            "gp": raw[tid]["gp"],
        }
    return result, iter_stats


def solve_per_game(
    games: List[GameObs],
    hca_oe: float = 1.4,
    hca_de: float = 1.4,
    max_iter: int = 200,
    tol: float = 0.01,
) -> Tuple[Dict[int, Dict], List[Dict]]:
    """Per-game KenPom-style solver with per-iteration diagnostics."""
    if not games:
        return {}, []

    team_ids_set = set()
    for g in games:
        team_ids_set.add(g.team_id)
        team_ids_set.add(g.opp_id)
    team_ids = sorted(team_ids_set)

    team_games: Dict[int, List[GameObs]] = {tid: [] for tid in team_ids}
    for g in games:
        team_games[g.team_id].append(g)

    league_avg = _compute_league_avg(games)

    # Pre-compute per-game HCA-adjusted OE and DE
    game_oe: Dict[int, Dict[int, float]] = defaultdict(dict)  # {tid: {game_id: oe}}
    game_de: Dict[int, Dict[int, float]] = defaultdict(dict)
    team_gp: Dict[int, int] = defaultdict(int)

    for tid in team_ids:
        for g in team_games[tid]:
            if g.team_poss <= 0:
                continue
            hca_off = hca_def = 0.0
            if not g.is_neutral:
                if g.is_home:
                    hca_off, hca_def = hca_oe, -hca_de
                else:
                    hca_off, hca_def = -hca_oe, hca_de

            adj_team_pts = g.team_pts - hca_off * g.team_poss / 100.0
            adj_opp_pts = g.opp_pts - hca_def * g.opp_poss / 100.0

            oe = adj_team_pts / g.team_poss * 100.0
            de = adj_opp_pts / g.opp_poss * 100.0 if g.opp_poss > 0 else league_avg

            # Clamp extreme game-level values
            oe = max(_EFF_FLOOR, min(_EFF_CEIL, oe))
            de = max(_EFF_FLOOR, min(_EFF_CEIL, de))

            # Use a unique key: (game_id, is_home) to handle the two sides
            gkey = (g.game_id, g.is_home)
            game_oe[tid][gkey] = oe
            game_de[tid][gkey] = de
            team_gp[tid] += 1

    # Compute raw aggregates for initialization
    raw_oe_agg: Dict[int, float] = {}
    raw_de_agg: Dict[int, float] = {}
    for tid in team_ids:
        if game_oe[tid]:
            # Weighted average of per-game OE
            w_total = 0.0
            w_oe = 0.0
            w_de = 0.0
            for g in team_games[tid]:
                gkey = (g.game_id, g.is_home)
                if gkey not in game_oe[tid]:
                    continue
                w_oe += g.weight * game_oe[tid][gkey]
                w_de += g.weight * game_de[tid][gkey]
                w_total += g.weight
            raw_oe_agg[tid] = (w_oe / w_total) if w_total > 0 else league_avg
            raw_de_agg[tid] = (w_de / w_total) if w_total > 0 else league_avg
        else:
            raw_oe_agg[tid] = league_avg
            raw_de_agg[tid] = league_avg

    # Initialize
    adj_oe = dict(raw_oe_agg)
    adj_de = dict(raw_de_agg)

    iter_stats = []
    for it in range(max_iter):
        new_oe: Dict[int, float] = {}
        new_de: Dict[int, float] = {}

        for tid in team_ids:
            if team_gp[tid] == 0:
                new_oe[tid] = league_avg
                new_de[tid] = league_avg
                continue

            w_adj_oe = 0.0
            w_adj_de = 0.0
            w_total = 0.0
            for g in team_games[tid]:
                gkey = (g.game_id, g.is_home)
                if gkey not in game_oe[tid]:
                    continue
                opp_de = adj_de.get(g.opp_id, league_avg)
                opp_oe = adj_oe.get(g.opp_id, league_avg)

                # Per-game SOS adjustment
                if opp_de > 0:
                    w_adj_oe += g.weight * game_oe[tid][gkey] * (league_avg / opp_de)
                else:
                    w_adj_oe += g.weight * game_oe[tid][gkey]
                if opp_oe > 0:
                    w_adj_de += g.weight * game_de[tid][gkey] * (league_avg / opp_oe)
                else:
                    w_adj_de += g.weight * game_de[tid][gkey]
                w_total += g.weight

            c_oe = (w_adj_oe / w_total) if w_total > 0 else league_avg
            c_de = (w_adj_de / w_total) if w_total > 0 else league_avg

            # Clamp
            c_oe = max(_EFF_FLOOR, min(_EFF_CEIL, c_oe))
            c_de = max(_EFF_FLOOR, min(_EFF_CEIL, c_de))

            new_oe[tid] = c_oe if math.isfinite(c_oe) else league_avg
            new_de[tid] = c_de if math.isfinite(c_de) else league_avg

        max_delta = max(
            max(abs(new_oe[t] - adj_oe[t]) for t in team_ids),
            max(abs(new_de[t] - adj_de[t]) for t in team_ids),
        )
        adj_oe, adj_de = new_oe, new_de

        active = [t for t in team_ids if team_gp[t] > 0]
        oe_vals = [adj_oe[t] for t in active]
        de_vals = [adj_de[t] for t in active]
        em_vals = [adj_oe[t] - adj_de[t] for t in active]
        iter_stats.append({
            "iter": it + 1,
            "std_oe": _std(oe_vals),
            "std_de": _std(de_vals),
            "std_em": _std(em_vals),
            "max_delta": max_delta,
        })
        if max_delta < tol:
            break

    result = {}
    for tid in team_ids:
        result[tid] = {
            "adj_oe": adj_oe[tid], "adj_de": adj_de[tid],
            "adj_em": adj_oe[tid] - adj_de[tid],
            "raw_oe": raw_oe_agg[tid], "raw_de": raw_de_agg[tid],
            "gp": team_gp[tid],
        }
    return result, iter_stats


# ---------------------------------------------------------------------------
# Team name matching
# ---------------------------------------------------------------------------

# Common KenPom → our API name differences
KP_NAME_MAP = {
    "St. John's": "St. John's",
    "Miami FL": "Miami",
    "Miami OH": "Miami (OH)",
    "Connecticut": "UConn",
    "N.C. State": "NC State",
    "Penn St.": "Penn State",
    "Kansas St.": "Kansas State",
    "Michigan St.": "Michigan State",
    "Mississippi St.": "Mississippi State",
    "Ohio St.": "Ohio State",
    "Oregon St.": "Oregon State",
    "Boise St.": "Boise State",
    "Iowa St.": "Iowa State",
    "Oklahoma St.": "Oklahoma State",
    "Colorado St.": "Colorado State",
    "Utah St.": "Utah State",
    "Arizona St.": "Arizona State",
    "Washington St.": "Washington State",
    "Fresno St.": "Fresno State",
    "San Jose St.": "San José State",
    "San Diego St.": "San Diego State",
    "Wichita St.": "Wichita State",
    "Ball St.": "Ball State",
    "Kent St.": "Kent State",
    "Boise St.": "Boise State",
    "Appalachian St.": "Appalachian State",
    "Kennesaw St.": "Kennesaw State",
    "Sacramento St.": "Sacramento State",
    "Portland St.": "Portland State",
    "Weber St.": "Weber State",
    "Idaho St.": "Idaho State",
    "Murray St.": "Murray State",
    "Morehead St.": "Morehead State",
    "Jacksonville St.": "Jacksonville State",
    "Norfolk St.": "Norfolk State",
    "Delaware St.": "Delaware State",
    "Morgan St.": "Morgan State",
    "Coppin St.": "Coppin State",
    "South Carolina St.": "South Carolina State",
    "Alabama St.": "Alabama State",
    "Mississippi Valley St.": "Mississippi Valley State",
    "Alcorn St.": "Alcorn State",
    "Grambling St.": "Grambling",
    "Prairie View A&M": "Prairie View",
    "Arkansas Pine Bluff": "Arkansas-Pine Bluff",
    "LIU": "Long Island University",
    "Loyola Chicago": "Loyola (IL)",
    "Loyola MD": "Loyola (MD)",
    "Loyola Marymount": "Loyola Marymount",
    "Saint Mary's": "Saint Mary's",
    "Saint Joseph's": "Saint Joseph's",
    "Saint Louis": "Saint Louis",
    "Saint Peter's": "Saint Peter's",
    "Mount St. Mary's": "Mount St. Mary's",
    "Central Connecticut": "Central Connecticut State",
    "Fort Wayne": "Purdue Fort Wayne",
    "UNC Greensboro": "North Carolina-Greensboro",
    "UNC Asheville": "UNC Asheville",
    "UNC Wilmington": "UNC Wilmington",
    "UT Arlington": "UT Arlington",
    "UT Martin": "UT Martin",
    "UT Rio Grande Valley": "UT Rio Grande Valley",
    "UTEP": "UTEP",
    "UTSA": "UTSA",
    "Little Rock": "Little Rock",
    "USC Upstate": "USC Upstate",
    "College of Charleston": "Charleston",
    "Cal St. Bakersfield": "Cal State Bakersfield",
    "Cal St. Fullerton": "Cal State Fullerton",
    "Cal St. Northridge": "Cal State Northridge",
    "Sam Houston St.": "Sam Houston State",
    "Stephen F. Austin": "Stephen F. Austin",
    "Nicholls St.": "Nicholls State",
    "McNeese St.": "McNeese State",
    "Northwestern St.": "Northwestern State",
    "Southeastern Louisiana": "Southeastern Louisiana",
    "Texas A&M Corpus Christi": "Texas A&M-Corpus Christi",
    "Alabama A&M": "Alabama A&M",
    "Bethune Cookman": "Bethune-Cookman",
    "Florida A&M": "Florida A&M",
    "North Carolina A&T": "North Carolina A&T",
    "Texas Southern": "Texas Southern",
    "SIU Edwardsville": "SIU-Edwardsville",
    "Green Bay": "Green Bay",
    "Wisconsin": "Wisconsin",
    "Bowling Green": "Bowling Green",
    "Cleveland St.": "Cleveland State",
    "Youngstown St.": "Youngstown State",
    "Wright St.": "Wright State",
    "Georgia St.": "Georgia State",
    "Tennessee St.": "Tennessee State",
    "North Carolina Central": "North Carolina Central",
    "Charleston Southern": "Charleston Southern",
    "Winthrop": "Winthrop",
    "Gardner Webb": "Gardner-Webb",
    "Cal Poly": "Cal Poly",
    "UC Davis": "UC Davis",
    "UC Irvine": "UC Irvine",
    "UC Riverside": "UC Riverside",
    "UC San Diego": "UC San Diego",
    "UC Santa Barbara": "UC Santa Barbara",
    "Long Beach St.": "Long Beach State",
    "Cal Baptist": "California Baptist",
    "Tarleton St.": "Tarleton State",
    "Dixie St.": "Utah Tech",
    "Southern Utah": "Southern Utah",
    "Southern": "Southern",
    "Southern Miss": "Southern Mississippi",
    "Southeast Missouri St.": "Southeast Missouri State",
    "Loyola Maryland": "Loyola (MD)",
    "Albany": "Albany",
    "NJIT": "NJIT",
    "VMI": "VMI",
    "VCU": "VCU",
    "BYU": "BYU",
    "SMU": "SMU",
    "TCU": "TCU",
    "UCF": "UCF",
    "LSU": "LSU",
    "USC": "USC",
    "UCLA": "UCLA",
    "UNLV": "UNLV",
    "UAB": "UAB",
    "UIC": "UIC",
    "IUPUI": "IUPUI",
    "Binghamton": "Binghamton",
    "Stony Brook": "Stony Brook",
    "UMass Lowell": "UMass Lowell",
    "UMass": "Massachusetts",
    "UConn": "UConn",
    "FIU": "FIU",
    "FAU": "FAU",
    "SIUE": "SIU-Edwardsville",
    "North Florida": "North Florida",
    "Jacksonville": "Jacksonville",
    "Stetson": "Stetson",
    "Monmouth": "Monmouth",
    "Quinnipiac": "Quinnipiac",
    "Sacred Heart": "Sacred Heart",
    "Fairfield": "Fairfield",
    "Rider": "Rider",
    "Iona": "Iona",
    "Marist": "Marist",
    "Manhattan": "Manhattan",
    "Canisius": "Canisius",
    "Niagara": "Niagara",
    "Siena": "Siena",
    "Le Moyne": "Le Moyne",
    "Mercyhurst": "Mercyhurst",
    "Lindenwood": "Lindenwood",
    "Queens": "Queens (NC)",
    "Stonehill": "Stonehill",
    "Bellarmine": "Bellarmine",
    "North Alabama": "North Alabama",
    "Indiana St.": "Indiana State",
    "Illinois St.": "Illinois State",
    "Missouri St.": "Missouri State",
    "Wichita St.": "Wichita State",
    "Southern Illinois": "Southern Illinois",
    "Evansville": "Evansville",
    "Valparaiso": "Valparaiso",
    "Drake": "Drake",
    "Bradley": "Bradley",
    "UNI": "Northern Iowa",
}


def match_teams(kp: Dict, team_info: Dict[int, Dict], result: Dict[int, Dict]):
    """Match KenPom names to our team IDs. Returns {tid: kp_name}."""
    # Build our name → tid map
    our_name_to_tid: Dict[str, int] = {}
    for tid, info in team_info.items():
        name = info.get("school")
        if name:
            our_name_to_tid[name.strip()] = tid

    # Try matching
    matched: Dict[int, str] = {}
    unmatched_kp = []
    for kp_name in kp:
        # Direct match
        if kp_name in our_name_to_tid:
            matched[our_name_to_tid[kp_name]] = kp_name
            continue
        # Map match
        mapped = KP_NAME_MAP.get(kp_name)
        if mapped and mapped in our_name_to_tid:
            matched[our_name_to_tid[mapped]] = kp_name
            continue
        # Case-insensitive
        found = False
        for our_name, tid in our_name_to_tid.items():
            if our_name.lower() == kp_name.lower():
                matched[tid] = kp_name
                found = True
                break
        if not found:
            unmatched_kp.append(kp_name)

    return matched, unmatched_kp


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    cfg = load_config("config.yaml")
    s3 = S3IO(cfg.bucket, cfg.region)
    params = _get_rating_params(cfg)
    margin_cap = _get_margin_cap(cfg)

    print(f"\n{'='*70}")
    print("  COMPRESSION DIAGNOSTIC — Season 2026")
    print(f"{'='*70}")
    print(f"\n  Config: half_life={params['half_life']}, hca_oe={params['hca_oe']:.4f}, "
          f"hca_de={params['hca_de']:.4f}, margin_cap={margin_cap}")

    # Load KenPom
    print("\n  Loading KenPom data...")
    kp = load_kenpom(KENPOM_PATH)
    kp_ems = [v["adj_em"] for v in kp.values()]
    print(f"  KenPom: {len(kp)} teams, std(adj_em)={_std(kp_ems):.2f}, "
          f"mean(adj_oe)={sum(v['adj_oe'] for v in kp.values())/len(kp):.2f}, "
          f"mean(adj_de)={sum(v['adj_de'] for v in kp.values())/len(kp):.2f}")

    # Load game data
    print("\n  Loading game data from S3...")
    d1_ids = _load_d1_team_ids(s3, cfg)
    team_info = _load_team_info(s3, cfg)
    games_by_date = _load_box_score_games(s3, cfg, 2026, d1_ids)
    total_obs = sum(len(v) for v in games_by_date.values())
    print(f"  {total_obs} game observations, {len(games_by_date)} dates")

    # -----------------------------------------------------------------------
    # Variant 1: Current aggregate solver with recency + cap (production)
    # -----------------------------------------------------------------------
    print(f"\n{'='*70}")
    print("  VARIANT 1: Aggregate solver (current production)")
    print(f"{'='*70}")
    gbd_capped = _apply_margin_cap(games_by_date, margin_cap) if margin_cap else games_by_date
    all_games_recency = flatten_games(gbd_capped, half_life=params["half_life"])
    print(f"  {len(all_games_recency)} obs, half_life={params['half_life']}, cap={margin_cap}")
    r1, stats1 = solve_aggregate(all_games_recency, hca_oe=params["hca_oe"], hca_de=params["hca_de"])
    _print_iter_stats("Aggregate+recency+cap", stats1)
    _print_summary("Aggregate+recency+cap", r1)

    # -----------------------------------------------------------------------
    # Variant 2: Per-game solver with recency + cap
    # -----------------------------------------------------------------------
    print(f"\n{'='*70}")
    print("  VARIANT 2: Per-game solver with recency + cap")
    print(f"{'='*70}")
    r2, stats2 = solve_per_game(all_games_recency, hca_oe=params["hca_oe"], hca_de=params["hca_de"])
    _print_iter_stats("Per-game+recency+cap", stats2)
    _print_summary("Per-game+recency+cap", r2)

    # -----------------------------------------------------------------------
    # Variant 3: Per-game solver, NO recency (weight=1.0), WITH cap
    # -----------------------------------------------------------------------
    print(f"\n{'='*70}")
    print("  VARIANT 3: Per-game solver, no recency, with cap")
    print(f"{'='*70}")
    all_games_no_recency = flatten_games(gbd_capped, half_life=None)
    r3, stats3 = solve_per_game(all_games_no_recency, hca_oe=params["hca_oe"], hca_de=params["hca_de"])
    _print_iter_stats("Per-game+no_recency+cap", stats3)
    _print_summary("Per-game+no_recency+cap", r3)

    # -----------------------------------------------------------------------
    # Variant 4: Per-game solver, NO recency, NO cap
    # -----------------------------------------------------------------------
    print(f"\n{'='*70}")
    print("  VARIANT 4: Per-game solver, no recency, no cap")
    print(f"{'='*70}")
    all_games_raw = flatten_games(games_by_date, half_life=None)
    r4, stats4 = solve_per_game(all_games_raw, hca_oe=params["hca_oe"], hca_de=params["hca_de"])
    _print_iter_stats("Per-game+no_recency+no_cap", stats4)
    _print_summary("Per-game+no_recency+no_cap", r4)

    # -----------------------------------------------------------------------
    # Variant 5: Per-game solver, NO recency, NO cap, KenPom-like HCA (~1.4)
    # -----------------------------------------------------------------------
    print(f"\n{'='*70}")
    print("  VARIANT 5: Per-game, no recency, no cap, HCA=1.4")
    print(f"{'='*70}")
    r5, stats5 = solve_per_game(all_games_raw, hca_oe=1.4, hca_de=1.4)
    _print_iter_stats("Per-game+no_recency+no_cap+hca1.4", stats5)
    _print_summary("Per-game+no_recency+no_cap+hca1.4", r5)

    # -----------------------------------------------------------------------
    # Compare all variants to KenPom
    # -----------------------------------------------------------------------
    print(f"\n{'='*70}")
    print("  COMPARISON TO KENPOM")
    print(f"{'='*70}")

    matched, unmatched = match_teams(kp, team_info, r1)
    print(f"\n  Matched {len(matched)} teams, {len(unmatched)} unmatched")
    if unmatched:
        print(f"  Unmatched KenPom teams: {unmatched[:15]}...")

    variants = [
        ("V1:Agg+rec+cap", r1),
        ("V2:PG+rec+cap", r2),
        ("V3:PG+cap", r3),
        ("V4:PG raw", r4),
        ("V5:PG+hca1.4", r5),
    ]

    print(f"\n  {'Variant':<18} {'std(em)':>8} {'r(em)':>7} {'MAE_oe':>8} {'MAE_de':>8} {'MAE_em':>8} {'scale':>6}")
    print(f"  {'-'*18} {'-'*8} {'-'*7} {'-'*8} {'-'*8} {'-'*8} {'-'*6}")
    print(f"  {'KenPom':<18} {_std(kp_ems):>8.2f}")

    for label, result in variants:
        _print_variant_comparison(label, result, kp, matched)

    # -----------------------------------------------------------------------
    # Top 20 comparison for best variant vs KenPom
    # -----------------------------------------------------------------------
    best_label, best_result = min(variants, key=lambda v: _compute_mae_em(v[1], kp, matched))
    print(f"\n{'='*70}")
    print(f"  TOP 20 COMPARISON: {best_label} vs KenPom")
    print(f"{'='*70}")
    _print_top20(best_result, kp, matched, team_info)


def _print_iter_stats(label: str, stats: List[Dict]):
    """Print per-iteration std dev (first 10 + last 5)."""
    print(f"\n  Per-iteration std dev for [{label}]:")
    print(f"  {'Iter':>5} {'std(oe)':>9} {'std(de)':>9} {'std(em)':>9} {'max_delta':>10}")
    n = len(stats)
    show = list(range(min(10, n)))
    if n > 15:
        show += list(range(n - 5, n))
    for i in sorted(set(show)):
        s = stats[i]
        print(f"  {s['iter']:>5} {s['std_oe']:>9.3f} {s['std_de']:>9.3f} {s['std_em']:>9.3f} {s['max_delta']:>10.4f}")
    if n > 15:
        print(f"  ... ({n} total iterations)")


def _print_summary(label: str, result: Dict[int, Dict]):
    """Print summary stats for a solver run."""
    active = {k: v for k, v in result.items() if v["gp"] > 0}
    ems = [v["adj_em"] for v in active.values()]
    oes = [v["adj_oe"] for v in active.values()]
    des = [v["adj_de"] for v in active.values()]
    print(f"\n  Final [{label}]: {len(active)} teams")
    print(f"  mean(adj_oe)={sum(oes)/len(oes):.2f}, std(adj_oe)={_std(oes):.2f}")
    print(f"  mean(adj_de)={sum(des)/len(des):.2f}, std(adj_de)={_std(des):.2f}")
    print(f"  mean(adj_em)={sum(ems)/len(ems):.2f}, std(adj_em)={_std(ems):.2f}")
    top5 = sorted(active.items(), key=lambda x: -x[1]["adj_em"])[:5]
    print(f"  Top 5 by margin: ", end="")
    for tid, v in top5:
        print(f"{tid}({v['adj_em']:.1f}) ", end="")
    print()


def _compute_mae_em(result, kp, matched):
    errs = []
    for tid, kp_name in matched.items():
        if tid not in result or result[tid]["gp"] == 0:
            continue
        errs.append(abs(result[tid]["adj_em"] - kp[kp_name]["adj_em"]))
    return sum(errs) / len(errs) if errs else 999.0


def _print_variant_comparison(label, result, kp, matched):
    """Print one line of comparison stats."""
    our_ems = []
    kp_ems = []
    abs_err_oe = []
    abs_err_de = []
    abs_err_em = []
    for tid, kp_name in matched.items():
        if tid not in result or result[tid]["gp"] == 0:
            continue
        our_ems.append(result[tid]["adj_em"])
        kp_ems.append(kp[kp_name]["adj_em"])
        abs_err_oe.append(abs(result[tid]["adj_oe"] - kp[kp_name]["adj_oe"]))
        abs_err_de.append(abs(result[tid]["adj_de"] - kp[kp_name]["adj_de"]))
        abs_err_em.append(abs(result[tid]["adj_em"] - kp[kp_name]["adj_em"]))

    n = len(our_ems)
    if n == 0:
        print(f"  {label:<18} no matches")
        return

    std_em = _std(our_ems)
    std_kp = _std(kp_ems)
    mae_oe = sum(abs_err_oe) / n
    mae_de = sum(abs_err_de) / n
    mae_em = sum(abs_err_em) / n

    # Correlation
    m_our = sum(our_ems) / n
    m_kp = sum(kp_ems) / n
    cov = sum((a - m_our) * (b - m_kp) for a, b in zip(our_ems, kp_ems)) / n
    s_our = (sum((a - m_our) ** 2 for a in our_ems) / n) ** 0.5
    s_kp = (sum((b - m_kp) ** 2 for b in kp_ems) / n) ** 0.5
    r = cov / (s_our * s_kp) if s_our > 0 and s_kp > 0 else 0

    scale = std_em / std_kp if std_kp > 0 else 0

    print(f"  {label:<18} {std_em:>8.2f} {r:>7.3f} {mae_oe:>8.2f} {mae_de:>8.2f} {mae_em:>8.2f} {scale:>6.2f}")


def _print_top20(result, kp, matched, team_info):
    """Print top 20 teams side-by-side with KenPom."""
    # Sort by KenPom adj_em
    rows = []
    for tid, kp_name in matched.items():
        if tid not in result or result[tid]["gp"] == 0:
            continue
        r = result[tid]
        k = kp[kp_name]
        rows.append((kp_name, r, k))
    rows.sort(key=lambda x: -x[2]["adj_em"])

    print(f"\n  {'Team':<22} {'our_oe':>7} {'kp_oe':>7} {'our_de':>7} {'kp_de':>7} {'our_em':>7} {'kp_em':>7} {'err':>6}")
    print(f"  {'-'*22} {'-'*7} {'-'*7} {'-'*7} {'-'*7} {'-'*7} {'-'*7} {'-'*6}")
    for name, r, k in rows[:25]:
        err = r["adj_em"] - k["adj_em"]
        print(f"  {name:<22} {r['adj_oe']:>7.1f} {k['adj_oe']:>7.1f} "
              f"{r['adj_de']:>7.1f} {k['adj_de']:>7.1f} "
              f"{r['adj_em']:>7.1f} {k['adj_em']:>7.1f} {err:>+6.1f}")


if __name__ == "__main__":
    main()
