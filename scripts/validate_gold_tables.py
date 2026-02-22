#!/usr/bin/env python3
"""Comprehensive gold table validation across seasons 2025 and 2026.

Builds all 5 gold tables, runs data quality checks, and prints a report.
"""
from __future__ import annotations

import sys
import os
import statistics
from collections import Counter
from datetime import date

# Add project root to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.cbbd_etl.config import load_config
from src.cbbd_etl.gold import GOLD_TRANSFORMS

cfg = load_config("config.yaml")

PASS = "PASS"
FAIL = "FAIL"
WARN = "WARN"
SKIP = "SKIP"

findings = []

def report(status, table, check, detail=""):
    tag = {"PASS": "\033[92mPASS\033[0m", "FAIL": "\033[91mFAIL\033[0m",
           "WARN": "\033[93mWARN\033[0m", "SKIP": "\033[94mSKIP\033[0m"}[status]
    msg = f"  [{tag}] {check}"
    if detail:
        msg += f" — {detail}"
    print(msg)
    findings.append((status, table, check, detail))


def col(tbl, name):
    """Extract column as python list, None-safe."""
    if name in tbl.column_names:
        return tbl.column(name).to_pylist()
    return [None] * tbl.num_rows


def non_null(vals):
    return [v for v in vals if v is not None]


# ======================================================================
# 1. TEAM POWER RANKINGS
# ======================================================================
print("=" * 72)
print("1. TEAM POWER RANKINGS")
print("=" * 72)

for season in (2025, 2026):
    print(f"\n--- Season {season} ---")
    try:
        tpr = GOLD_TRANSFORMS["team_power_rankings"](cfg, season)
    except Exception as e:
        report(FAIL, "team_power_rankings", f"Build failed for {season}", str(e))
        continue

    nrows = tpr.num_rows
    report(PASS if nrows > 0 else FAIL, "team_power_rankings",
           f"Row count ({season})", f"{nrows} teams")

    # Check for duplicates
    team_ids = col(tpr, "teamId")
    dupes = nrows - len(set(tid for tid in team_ids if tid is not None))
    report(PASS if dupes == 0 else FAIL, "team_power_rankings",
           f"No duplicate teamIds ({season})", f"{dupes} duplicates")

    # Check composite_rank populated
    composites = non_null(col(tpr, "composite_rank"))
    pct_comp = len(composites) / nrows * 100 if nrows else 0
    report(PASS if pct_comp > 80 else WARN, "team_power_rankings",
           f"composite_rank populated ({season})", f"{pct_comp:.1f}% non-null ({len(composites)}/{nrows})")

    # Check adj ratings populated
    adj_nets = non_null(col(tpr, "adj_net_rating"))
    report(PASS if len(adj_nets) > 300 else WARN, "team_power_rankings",
           f"adj_net_rating populated ({season})", f"{len(adj_nets)} teams have ratings")

    # Check SRS populated
    srs_vals = non_null(col(tpr, "srs_rating"))
    report(PASS if len(srs_vals) > 0 else WARN, "team_power_rankings",
           f"SRS populated ({season})", f"{len(srs_vals)} teams have SRS")

    # Print top 25
    teams = col(tpr, "team")
    confs = col(tpr, "conference")
    adj_off = col(tpr, "adj_off_rating")
    adj_def = col(tpr, "adj_def_rating")
    adj_net = col(tpr, "adj_net_rating")
    ap = col(tpr, "ap_rank")
    comp = col(tpr, "composite_rank")
    srs = col(tpr, "srs_rating")
    games = col(tpr, "games_played")

    # Sort by composite_rank descending
    indices = list(range(nrows))
    indices.sort(key=lambda i: comp[i] if comp[i] is not None else -1, reverse=True)

    print(f"\n  Top 25 by blended_score ({season}):")
    print(f"  {'Rk':>3} {'Team':<25} {'Conf':<12} {'AdjNet':>7} {'SRS':>7} {'Comp':>6} {'AP':>4} {'GP':>3}")
    print(f"  {'---':>3} {'---':<25} {'---':<12} {'---':>7} {'---':>7} {'---':>6} {'---':>4} {'---':>3}")
    for rank, idx in enumerate(indices[:25], 1):
        t = teams[idx] or "?"
        c = confs[idx] or "?"
        an = f"{adj_net[idx]:.1f}" if adj_net[idx] is not None else "—"
        sr = f"{srs[idx]:.1f}" if srs[idx] is not None else "—"
        cp = f"{comp[idx]:.1f}" if comp[idx] is not None else "—"
        ap_r = str(ap[idx]) if ap[idx] is not None else "—"
        gp = str(games[idx]) if games[idx] is not None else "—"
        print(f"  {rank:>3} {t:<25} {c:<12} {an:>7} {sr:>7} {cp:>6} {ap_r:>4} {gp:>3}")

    # Season-specific checks
    if season == 2025:
        # UConn repeated as champion - verify they're near top
        uconn_idx = None
        for i, t in enumerate(teams):
            if t and t.lower().strip() in ("uconn", "connecticut"):
                uconn_idx = i
                break
        if uconn_idx is not None:
            uconn_rank = None
            for rank, idx in enumerate(indices, 1):
                if idx == uconn_idx:
                    uconn_rank = rank
                    break
            report(PASS if uconn_rank and uconn_rank <= 15 else WARN, "team_power_rankings",
                   f"UConn near top ({season})", f"Ranked #{uconn_rank}")
        else:
            report(WARN, "team_power_rankings", f"UConn not found ({season})")


# ======================================================================
# 2. TEAM SEASON SUMMARY
# ======================================================================
print("\n" + "=" * 72)
print("2. TEAM SEASON SUMMARY")
print("=" * 72)

BLUE_BLOODS = {
    "UConn": "UConn",
    "Duke": "Duke",
    "Kansas": "Kansas",
    "Kentucky": "Kentucky",
    "Houston": "Houston",
}

def match_team(team_name, exact_name):
    """Match a team name exactly."""
    if team_name is None:
        return False
    return team_name.strip() == exact_name

for season in (2025, 2026):
    print(f"\n--- Season {season} ---")
    try:
        tss = GOLD_TRANSFORMS["team_season_summary"](cfg, season)
    except Exception as e:
        report(FAIL, "team_season_summary", f"Build failed for {season}", str(e))
        continue

    nrows = tss.num_rows
    report(PASS if 300 <= nrows <= 400 else WARN, "team_season_summary",
           f"Row count ({season})", f"{nrows} teams (expect ~360)")

    # Check for duplicates
    team_ids = col(tss, "teamId")
    dupes = nrows - len(set(tid for tid in team_ids if tid is not None))
    report(PASS if dupes == 0 else FAIL, "team_season_summary",
           f"No duplicate teamIds ({season})", f"{dupes} duplicates")

    # Check W-L populated
    wins = non_null(col(tss, "wins"))
    losses = non_null(col(tss, "losses"))
    report(PASS if len(wins) > 300 else WARN, "team_season_summary",
           f"Wins populated ({season})", f"{len(wins)} teams have W-L")

    # Check PPG
    ppg_vals = non_null(col(tss, "ppg"))
    report(PASS if len(ppg_vals) > 200 else WARN, "team_season_summary",
           f"PPG populated ({season})", f"{len(ppg_vals)} teams have PPG")

    # Verify margins make sense
    margins = non_null(col(tss, "margin"))
    if margins:
        avg_margin = statistics.mean(margins)
        report(PASS if -5 < avg_margin < 5 else WARN, "team_season_summary",
               f"Average margin sanity ({season})", f"avg={avg_margin:.2f}")

    # Blue bloods check
    teams = col(tss, "team")
    w_list = col(tss, "wins")
    l_list = col(tss, "losses")
    ppg_list = col(tss, "ppg")
    opp_ppg_list = col(tss, "opp_ppg")
    margin_list = col(tss, "margin")
    confs = col(tss, "conference")
    win_pct_list = col(tss, "win_pct")

    print(f"\n  Blue Bloods ({season}):")
    print(f"  {'Team':<20} {'W-L':>8} {'WPct':>6} {'PPG':>6} {'OppPPG':>7} {'Margin':>7} {'Conf':<12}")
    print(f"  {'---':<20} {'---':>8} {'---':>6} {'---':>6} {'---':>7} {'---':>7} {'---':<12}")
    for bb_name, bb_exact in BLUE_BLOODS.items():
        found = False
        for i, t in enumerate(teams):
            if match_team(t, bb_exact):
                w = w_list[i] if w_list[i] is not None else 0
                l = l_list[i] if l_list[i] is not None else 0
                p = f"{ppg_list[i]:.1f}" if ppg_list[i] is not None else "—"
                op = f"{opp_ppg_list[i]:.1f}" if opp_ppg_list[i] is not None else "—"
                m = f"{margin_list[i]:+.1f}" if margin_list[i] is not None else "—"
                c = confs[i] or "?"
                wpc = f"{win_pct_list[i]:>5.3f}" if win_pct_list[i] is not None else "    —"
                print(f"  {t:<20} {w:>3}-{l:<4} {wpc} {p:>6} {op:>7} {m:>7} {c:<12}")
                found = True
                break
        if not found:
            print(f"  {bb_name:<20} NOT FOUND")

    if season == 2026:
        # Verify records are partial (season in progress)
        total_games = [
            (w_list[i] or 0) + (l_list[i] or 0)
            for i in range(nrows) if w_list[i] is not None
        ]
        if total_games:
            max_games = max(total_games)
            avg_games = statistics.mean(total_games)
            report(PASS if max_games < 40 else WARN, "team_season_summary",
                   f"Season partial ({season})", f"max games={max_games}, avg={avg_games:.1f}")


# ======================================================================
# 3. GAME PREDICTIONS FEATURES
# ======================================================================
print("\n" + "=" * 72)
print("3. GAME PREDICTIONS FEATURES")
print("=" * 72)

for season in (2026,):
    print(f"\n--- Season {season} ---")
    try:
        gpf = GOLD_TRANSFORMS["game_predictions_features"](cfg, season)
    except Exception as e:
        report(FAIL, "game_predictions_features", f"Build failed for {season}", str(e))
        continue

    nrows = gpf.num_rows
    report(PASS if nrows > 0 else FAIL, "game_predictions_features",
           f"Row count ({season})", f"{nrows} rows")

    # Check 2 rows per game
    game_ids = col(gpf, "gameId")
    game_counts = Counter(gid for gid in game_ids if gid is not None)
    non_two = sum(1 for c in game_counts.values() if c != 2)
    report(PASS if non_two == 0 else WARN, "game_predictions_features",
           f"2 rows per game ({season})", f"{non_two} games don't have exactly 2 rows (total {len(game_counts)} games)")

    # Check feature columns populated
    feature_cols = [
        "team_adj_off", "team_adj_def", "team_adj_net",
        "opp_adj_off", "opp_adj_def", "opp_adj_net",
        "team_srs", "opp_srs",
        "team_ppg", "team_opp_ppg", "team_pace",
        "spread", "over_under",
        "team_name", "opp_name",
        "team_conference", "opp_conference",
    ]
    print(f"\n  Feature column coverage ({season}):")
    print(f"  {'Column':<25} {'NonNull':>8} {'Pct':>6}")
    print(f"  {'---':<25} {'---':>8} {'---':>6}")
    for fc in feature_cols:
        vals = non_null(col(gpf, fc))
        pct = len(vals) / nrows * 100 if nrows else 0
        status = PASS if pct > 80 else (WARN if pct > 50 else FAIL)
        report(status, "game_predictions_features",
               f"{fc} populated ({season})", f"{len(vals)}/{nrows} ({pct:.1f}%)")
        print(f"  {fc:<25} {len(vals):>8} {pct:>5.1f}%")

    # Check is_home balance
    is_home = col(gpf, "is_home")
    home_count = sum(1 for h in is_home if h is True)
    away_count = sum(1 for h in is_home if h is False)
    report(PASS if home_count == away_count else WARN, "game_predictions_features",
           f"Home/away balance ({season})", f"home={home_count}, away={away_count}")

    # Check today's games
    today = date.today().isoformat()
    dates = col(gpf, "game_date")
    today_rows = [(i, game_ids[i]) for i in range(nrows) if dates[i] and dates[i][:10] == today]
    today_games = set(game_ids[i] for i, _ in today_rows)
    print(f"\n  Today's games ({today}): {len(today_games)} games, {len(today_rows)} rows")
    if today_rows:
        tnames = col(gpf, "team_name")
        onames = col(gpf, "opp_name")
        homes = col(gpf, "is_home")
        spreads = col(gpf, "spread")
        for i, gid in today_rows:
            if homes[i]:
                sp = f"spread={spreads[i]:.1f}" if spreads[i] is not None else "no line"
                print(f"    Game {gid}: {tnames[i]} vs {onames[i]} ({sp})")


# ======================================================================
# 4. MARKET LINES ANALYSIS
# ======================================================================
print("\n" + "=" * 72)
print("4. MARKET LINES ANALYSIS")
print("=" * 72)

for season in (2025,):
    print(f"\n--- Season {season} ---")
    try:
        mla = GOLD_TRANSFORMS["market_lines_analysis"](cfg, season)
    except Exception as e:
        report(FAIL, "market_lines_analysis", f"Build failed for {season}", str(e))
        continue

    nrows = mla.num_rows
    report(PASS if nrows > 0 else FAIL, "market_lines_analysis",
           f"Row count ({season})", f"{nrows} rows")

    # Spread error distribution
    spread_errors = non_null(col(mla, "spread_error"))
    if spread_errors:
        mean_err = statistics.mean(spread_errors)
        std_err = statistics.stdev(spread_errors) if len(spread_errors) > 1 else 0
        report(PASS if 6 <= std_err <= 14 else WARN, "market_lines_analysis",
               f"Spread error std ({season})", f"mean={mean_err:.2f}, std={std_err:.2f}")

    # ATS margin distribution (should be centered near 0)
    ats_margins = non_null(col(mla, "ats_margin"))
    if ats_margins:
        mean_ats = statistics.mean(ats_margins)
        report(PASS if -3 < mean_ats < 3 else WARN, "market_lines_analysis",
               f"ATS margin centered ({season})", f"mean={mean_ats:.2f}")

    # home_covered is binary
    hc = col(mla, "home_covered")
    hc_non_null = [v for v in hc if v is not None]
    hc_types = set(type(v) for v in hc_non_null)
    hc_values = set(hc_non_null)
    report(PASS if hc_values <= {True, False} else FAIL, "market_lines_analysis",
           f"home_covered is binary ({season})", f"values={hc_values}, types={hc_types}")

    # over_hit coverage
    oh = non_null(col(mla, "over_hit"))
    report(PASS if len(oh) > nrows * 0.5 else WARN, "market_lines_analysis",
           f"over_hit populated ({season})", f"{len(oh)}/{nrows}")

    # home_win coverage
    hw = non_null(col(mla, "home_win"))
    if hw:
        home_win_rate = sum(1 for v in hw if v) / len(hw)
        report(PASS if 0.45 <= home_win_rate <= 0.70 else WARN, "market_lines_analysis",
               f"Home win rate sanity ({season})", f"{home_win_rate:.3f} ({sum(1 for v in hw if v)}/{len(hw)})")

    # Spread distribution
    spreads = non_null(col(mla, "spread"))
    if spreads:
        spread_mean = statistics.mean(spreads)
        spread_std = statistics.stdev(spreads) if len(spreads) > 1 else 0
        report(PASS if -5 < spread_mean < 0 else WARN, "market_lines_analysis",
               f"Spread mean sanity ({season})", f"mean={spread_mean:.2f}, std={spread_std:.2f}")

    # Print summary stats
    print(f"\n  Summary ({season}):")
    print(f"    Total rows: {nrows}")
    print(f"    Spreads: {len(spreads)} non-null, mean={statistics.mean(spreads):.2f}" if spreads else "    No spreads")
    if ats_margins:
        print(f"    ATS margin: mean={statistics.mean(ats_margins):.2f}, std={statistics.stdev(ats_margins):.2f}")
    cover_rate = sum(1 for v in hc_non_null if v) / len(hc_non_null) if hc_non_null else 0
    print(f"    Home covers: {cover_rate:.3f}")
    oh_rate = sum(1 for v in oh if v) / len(oh) if oh else 0
    print(f"    Over hits: {oh_rate:.3f}")


# ======================================================================
# 5. PLAYER SEASON IMPACT
# ======================================================================
print("\n" + "=" * 72)
print("5. PLAYER SEASON IMPACT")
print("=" * 72)

# Note: fct_player_season_stats only has season 2024 data
for season in (2024,):
    print(f"\n--- Season {season} (only season with player stats) ---")
    try:
        psi = GOLD_TRANSFORMS["player_season_impact"](cfg, season)
    except Exception as e:
        report(FAIL, "player_season_impact", f"Build failed for {season}", str(e))
        continue

    nrows = psi.num_rows
    report(PASS if nrows > 0 else FAIL, "player_season_impact",
           f"Row count ({season})", f"{nrows} players")

    if nrows == 0:
        report(SKIP, "player_season_impact", f"No data to validate ({season})")
        continue

    # No player > 50 PPG
    ppg = non_null(col(psi, "ppg"))
    over_50 = [v for v in ppg if v > 50]
    report(PASS if not over_50 else FAIL, "player_season_impact",
           f"No PPG > 50 ({season})", f"{len(over_50)} players over 50 PPG" if over_50 else "")

    # No negative minutes
    mins = non_null(col(psi, "minutes"))
    neg_mins = [v for v in mins if v < 0]
    report(PASS if not neg_mins else FAIL, "player_season_impact",
           f"No negative minutes ({season})", f"{len(neg_mins)} players with negative minutes" if neg_mins else "")

    # Shooting percentages: fg_pct, fg3_pct, ft_pct must be in [0,1]
    # EFG% can exceed 1.0 (max 1.5 for all-3pt shooters); TS% can exceed 1.0 in edge cases
    pct_ranges = {
        "fg_pct": (0, 1.0), "fg3_pct": (0, 1.0), "ft_pct": (0, 1.0),
        "efg_pct": (0, 1.5), "true_shooting": (0, 1.5),
    }
    for pct_col, (lo, hi) in pct_ranges.items():
        vals = non_null(col(psi, pct_col))
        if vals:
            out_of_range = [v for v in vals if v < lo or v > hi]
            report(PASS if not out_of_range else FAIL, "player_season_impact",
                   f"{pct_col} in [{lo},{hi}] ({season})",
                   f"{len(out_of_range)}/{len(vals)} out of range" if out_of_range else f"{len(vals)} valid")
        else:
            report(WARN, "player_season_impact", f"{pct_col} empty ({season})")

    # Check duplicates
    player_ids = col(psi, "playerId")
    dupes = nrows - len(set(pid for pid in player_ids if pid is not None))
    report(PASS if dupes == 0 else FAIL, "player_season_impact",
           f"No duplicate playerIds ({season})", f"{dupes} duplicates")

    # Print top scorers
    teams = col(psi, "team")
    pts = col(psi, "ppg")
    rpg = col(psi, "rpg")
    apg = col(psi, "apg")
    fg_pct = col(psi, "fg_pct")
    ts = col(psi, "true_shooting")
    gp = col(psi, "games")

    # Sort by PPG
    indices = list(range(nrows))
    indices.sort(key=lambda i: pts[i] if pts[i] is not None else -1, reverse=True)

    print(f"\n  Top 20 scorers ({season}):")
    print(f"  {'Rk':>3} {'PlayerID':>10} {'Team':<20} {'GP':>3} {'PPG':>6} {'RPG':>5} {'APG':>5} {'FG%':>5} {'TS%':>5}")
    print(f"  {'---':>3} {'---':>10} {'---':<20} {'---':>3} {'---':>6} {'---':>5} {'---':>5} {'---':>5} {'---':>5}")
    for rank, idx in enumerate(indices[:20], 1):
        pid = player_ids[idx] or "?"
        t = (teams[idx] or "?")[:20]
        g = str(gp[idx]) if gp[idx] is not None else "—"
        p = f"{pts[idx]:.1f}" if pts[idx] is not None else "—"
        r = f"{rpg[idx]:.1f}" if rpg[idx] is not None else "—"
        a = f"{apg[idx]:.1f}" if apg[idx] is not None else "—"
        fg = f"{fg_pct[idx]:.3f}" if fg_pct[idx] is not None else "—"
        t_s = f"{ts[idx]:.3f}" if ts[idx] is not None else "—"
        print(f"  {rank:>3} {pid:>10} {t:<20} {g:>3} {p:>6} {r:>5} {a:>5} {fg:>5} {t_s:>5}")

# Also check that season 2025 returns empty (expected)
print(f"\n--- Season 2025 (expected empty - no silver data) ---")
try:
    psi_2025 = GOLD_TRANSFORMS["player_season_impact"](cfg, 2025)
    nrows_2025 = psi_2025.num_rows
    report(PASS if nrows_2025 == 0 else WARN, "player_season_impact",
           "Season 2025 empty (expected)", f"{nrows_2025} rows")
except Exception as e:
    report(WARN, "player_season_impact", f"Season 2025 build", str(e))


# ======================================================================
# FINAL SUMMARY
# ======================================================================
print("\n" + "=" * 72)
print("VALIDATION SUMMARY")
print("=" * 72)

counts = Counter(f[0] for f in findings)
print(f"\n  Total checks: {len(findings)}")
print(f"  PASS: {counts.get(PASS, 0)}")
print(f"  WARN: {counts.get(WARN, 0)}")
print(f"  FAIL: {counts.get(FAIL, 0)}")
print(f"  SKIP: {counts.get(SKIP, 0)}")

if counts.get(FAIL, 0) > 0:
    print("\n  FAILURES:")
    for status, table, check, detail in findings:
        if status == FAIL:
            print(f"    [{table}] {check}: {detail}")

if counts.get(WARN, 0) > 0:
    print("\n  WARNINGS:")
    for status, table, check, detail in findings:
        if status == WARN:
            print(f"    [{table}] {check}: {detail}")
