"""Pure math engine for iterative SOS-adjusted efficiency ratings.

Implements a Pomeroy-style iterative solver that computes adjusted offensive
and defensive efficiency ratings with home-court correction and recency
weighting. No I/O or AWS dependencies — pure functions for easy testing.

Key design:
- **Per-game SOS adjustment**: each game observation is individually adjusted
  by the specific opponent's current rating (league_avg / opp_adj_de), then
  the team's adj_oe is the weighted average of all adjusted game values.
  This preserves game-level signal and avoids the compression that occurs
  with aggregate-then-adjust approaches.
- **Data-driven league average** computed from actual game data (~109 pts/100
  possessions for real D1 basketball) anchors the SOS adjustments.
- **Damping** (default 1.0 = no damping) blends old and new ratings each
  iteration to prevent overshooting if needed.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

# Reasonable bounds for per-100-possession efficiency. Values outside this
# range indicate bad possession data (e.g. 7 possessions → 685 OE).
_EFF_FLOOR = 40.0
_EFF_CEIL = 200.0


@dataclass
class GameObs:
    """A single team-game observation for the ratings solver."""

    game_id: int
    team_id: int
    opp_id: int
    team_pts: float
    team_poss: float
    opp_pts: float
    opp_poss: float
    is_home: bool
    is_neutral: bool
    game_date: str  # YYYY-MM-DD
    weight: float  # recency decay weight


def exponential_decay_weight(days_ago: int, half_life: float = 30.0) -> float:
    """Compute recency weight using exponential decay.

    w = 0.5^(days_ago / half_life). Returns 1.0 for days_ago <= 0.
    """
    if days_ago <= 0:
        return 1.0
    return 0.5 ** (days_ago / half_life)


def compute_barthag(adj_oe: float, adj_de: float, exp: float = 11.5) -> float:
    """Compute BARTHAG power rating.

    BARTHAG = adj_oe^exp / (adj_oe^exp + adj_de^exp).
    Returns 0.5 if both are zero, equal, or non-finite.
    """
    if not math.isfinite(adj_oe) or not math.isfinite(adj_de):
        return 0.5
    if adj_oe <= 0 and adj_de <= 0:
        return 0.5
    try:
        oe_pow = adj_oe ** exp
        de_pow = adj_de ** exp
    except (OverflowError, ValueError):
        return 0.5
    denom = oe_pow + de_pow
    if not math.isfinite(denom) or denom == 0:
        return 0.5
    result = oe_pow / denom
    return result if math.isfinite(result) else 0.5


def solve_ratings(
    games: List[GameObs],
    prior: Optional[Dict[int, Tuple[float, float]]] = None,
    hca_oe: float = 1.4,
    hca_de: float = 1.4,
    max_iter: int = 200,
    tol: float = 0.01,
    damping: float = 1.0,
    sos_exponent: float = 1.0,
    shrinkage: float = 0.0,
) -> Dict[int, Dict]:
    """Run iterative convergence to compute adjusted efficiency ratings.

    Uses per-game SOS adjustments: each game observation is individually
    adjusted by the specific opponent's current rating, then averaged.
    This matches the Pomeroy methodology and avoids the compression that
    occurs with aggregate-then-adjust approaches.

    Args:
        games: List of game observations (already filtered to desired window).
        prior: Optional warm-start from previous date: {team_id: (adj_oe, adj_de)}.
        hca_oe: Home court advantage added to home team OE (pts/100 poss).
        hca_de: Home court advantage subtracted from home team DE (pts/100 poss).
        max_iter: Maximum solver iterations.
        tol: Convergence tolerance (max absolute change in any rating).
        damping: Blend factor (0-1). Each iteration: new = damping*computed + (1-damping)*old.
            Higher values update more aggressively; 1.0 means full replacement.
        sos_exponent: Exponent on the SOS multiplier (league_avg / opp_rating)^alpha.
            Values < 1 dampen the SOS adjustment; 1.0 = standard Pomeroy.
        shrinkage: Post-convergence shrinkage toward league average (0-1).
            Final adj_oe = (1-shrinkage)*adj_oe + shrinkage*league_avg.

    Returns:
        Dict keyed by team_id with keys: adj_oe, adj_de, raw_oe, raw_de,
        sos_oe, sos_de, games_played, adj_tempo, iterations.
    """
    if not games:
        return {}

    # Collect all team IDs
    team_ids_set = set()
    for g in games:
        team_ids_set.add(g.team_id)
        team_ids_set.add(g.opp_id)
    team_ids = sorted(team_ids_set)

    # Index games by team
    team_games: Dict[int, List[GameObs]] = {tid: [] for tid in team_ids}
    for g in games:
        team_games[g.team_id].append(g)

    # Compute weighted league averages
    total_w_pts = 0.0
    total_w_poss = 0.0
    for g in games:
        if g.team_poss > 0:
            total_w_pts += g.weight * g.team_pts
            total_w_poss += g.weight * g.team_poss

    league_avg = (total_w_pts / total_w_poss * 100.0) if total_w_poss > 0 else 100.0

    # Pre-compute per-game HCA-adjusted OE and DE.
    # game_oe[tid][i] corresponds to team_games[tid][i].
    game_oe: Dict[int, List[float]] = {tid: [] for tid in team_ids}
    game_de: Dict[int, List[float]] = {tid: [] for tid in team_ids}
    game_valid: Dict[int, List[bool]] = {tid: [] for tid in team_ids}

    for tid in team_ids:
        for g in team_games[tid]:
            if g.team_poss <= 0:
                game_oe[tid].append(0.0)
                game_de[tid].append(0.0)
                game_valid[tid].append(False)
                continue

            hca_off = 0.0
            hca_def = 0.0
            if not g.is_neutral:
                if g.is_home:
                    hca_off = hca_oe
                    hca_def = -hca_de
                else:
                    hca_off = -hca_oe
                    hca_def = hca_de

            adj_team_pts = g.team_pts - hca_off * g.team_poss / 100.0
            adj_opp_pts = g.opp_pts - hca_def * g.opp_poss / 100.0

            oe = adj_team_pts / g.team_poss * 100.0
            de = (adj_opp_pts / g.opp_poss * 100.0) if g.opp_poss > 0 else league_avg

            # Clamp extreme values from bad possession data
            oe = max(_EFF_FLOOR, min(_EFF_CEIL, oe))
            de = max(_EFF_FLOOR, min(_EFF_CEIL, de))

            game_oe[tid].append(oe)
            game_de[tid].append(de)
            game_valid[tid].append(True)

    # Compute raw aggregates (weighted average of per-game values)
    raw: Dict[int, Dict] = {}
    team_total_weight: Dict[int, float] = {}

    for tid in team_ids:
        w_oe = 0.0
        w_de = 0.0
        w_total = 0.0
        w_tempo_poss = 0.0
        w_tempo_weight = 0.0
        gp = 0
        for i, g in enumerate(team_games[tid]):
            if not game_valid[tid][i]:
                continue
            w_oe += g.weight * game_oe[tid][i]
            w_de += g.weight * game_de[tid][i]
            w_total += g.weight
            w_tempo_poss += g.weight * g.team_poss
            w_tempo_weight += g.weight
            gp += 1

        raw_oe = (w_oe / w_total) if w_total > 0 else league_avg
        raw_de = (w_de / w_total) if w_total > 0 else league_avg
        raw_tempo = (w_tempo_poss / w_tempo_weight) if w_tempo_weight > 0 else 0.0

        raw[tid] = {
            "raw_oe": raw_oe,
            "raw_de": raw_de,
            "raw_tempo": raw_tempo,
            "games_played": gp,
        }
        team_total_weight[tid] = w_tempo_weight

    # Initialize adjusted ratings
    adj_oe_map: Dict[int, float] = {}
    adj_de_map: Dict[int, float] = {}
    for tid in team_ids:
        if prior and tid in prior:
            p_oe, p_de = prior[tid]
            # Guard against NaN/inf warm-start values
            adj_oe_map[tid] = p_oe if math.isfinite(p_oe) else league_avg
            adj_de_map[tid] = p_de if math.isfinite(p_de) else league_avg
        else:
            adj_oe_map[tid] = raw[tid]["raw_oe"]
            adj_de_map[tid] = raw[tid]["raw_de"]

    # Iterative solver with per-game SOS adjustments
    iterations_used = 0
    for iteration in range(max_iter):
        iterations_used = iteration + 1
        new_oe: Dict[int, float] = {}
        new_de: Dict[int, float] = {}

        for tid in team_ids:
            tg = team_games[tid]
            if not tg or raw[tid]["games_played"] == 0:
                new_oe[tid] = league_avg
                new_de[tid] = league_avg
                continue

            w_adj_oe = 0.0
            w_adj_de = 0.0
            w_total = 0.0
            for i, g in enumerate(tg):
                if not game_valid[tid][i]:
                    continue

                opp_de = adj_de_map.get(g.opp_id, league_avg)
                opp_oe = adj_oe_map.get(g.opp_id, league_avg)

                # Per-game SOS adjustment:
                # adj_game_oe = game_oe * (league_avg / opp_adj_de)^alpha
                # If opponent has strong defense (low adj_de), this boosts.
                # If opponent has weak defense (high adj_de), this reduces.
                # Alpha < 1 dampens the SOS effect.
                if opp_de > 0:
                    sos_mult_oe = (league_avg / opp_de) ** sos_exponent
                    w_adj_oe += g.weight * game_oe[tid][i] * sos_mult_oe
                else:
                    w_adj_oe += g.weight * game_oe[tid][i]

                if opp_oe > 0:
                    sos_mult_de = (league_avg / opp_oe) ** sos_exponent
                    w_adj_de += g.weight * game_de[tid][i] * sos_mult_de
                else:
                    w_adj_de += g.weight * game_de[tid][i]

                w_total += g.weight

            computed_oe = (w_adj_oe / w_total) if w_total > 0 else league_avg
            computed_de = (w_adj_de / w_total) if w_total > 0 else league_avg

            # Clamp to reasonable range
            computed_oe = max(_EFF_FLOOR, min(_EFF_CEIL, computed_oe))
            computed_de = max(_EFF_FLOOR, min(_EFF_CEIL, computed_de))

            # Damped update: blend computed with previous (damping=1.0 means no blending)
            val_oe = damping * computed_oe + (1.0 - damping) * adj_oe_map[tid]
            val_de = damping * computed_de + (1.0 - damping) * adj_de_map[tid]

            # Guard against NaN/inf from numerical instability
            new_oe[tid] = val_oe if math.isfinite(val_oe) else league_avg
            new_de[tid] = val_de if math.isfinite(val_de) else league_avg

        # Check convergence.
        max_delta = 0.0
        for tid in team_ids:
            d_oe = abs(new_oe[tid] - adj_oe_map[tid])
            d_de = abs(new_de[tid] - adj_de_map[tid])
            if math.isfinite(d_oe):
                max_delta = max(max_delta, d_oe)
            if math.isfinite(d_de):
                max_delta = max(max_delta, d_de)

        adj_oe_map = new_oe
        adj_de_map = new_de

        if max_delta < tol:
            break

    # Apply post-convergence shrinkage toward league average
    if shrinkage > 0:
        for tid in team_ids:
            adj_oe_map[tid] = (1.0 - shrinkage) * adj_oe_map[tid] + shrinkage * league_avg
            adj_de_map[tid] = (1.0 - shrinkage) * adj_de_map[tid] + shrinkage * league_avg

    # Compute adjusted tempo
    league_avg_tempo = 0.0
    tempo_count = 0
    for tid in team_ids:
        if raw[tid]["games_played"] > 0 and raw[tid]["raw_tempo"] > 0:
            league_avg_tempo += raw[tid]["raw_tempo"]
            tempo_count += 1
    if tempo_count > 0:
        league_avg_tempo /= tempo_count

    # Build results
    result: Dict[int, Dict] = {}
    for tid in team_ids:
        tg = team_games[tid]
        w_opp_de = 0.0
        w_opp_oe = 0.0
        w_total = 0.0
        w_opp_tempo = 0.0
        for g in tg:
            if g.team_poss <= 0:
                continue
            w_opp_de += g.weight * adj_de_map.get(g.opp_id, league_avg)
            w_opp_oe += g.weight * adj_oe_map.get(g.opp_id, league_avg)
            w_opp_tempo += g.weight * raw.get(g.opp_id, {}).get("raw_tempo", league_avg_tempo)
            w_total += g.weight

        sos_oe = (w_opp_de / w_total) if w_total > 0 else league_avg
        sos_de = (w_opp_oe / w_total) if w_total > 0 else league_avg
        avg_opp_tempo = (w_opp_tempo / w_total) if w_total > 0 else league_avg_tempo

        raw_tempo = raw[tid]["raw_tempo"]
        if league_avg_tempo > 0 and avg_opp_tempo > 0:
            adj_tempo = raw_tempo * (league_avg_tempo / avg_opp_tempo)
        else:
            adj_tempo = raw_tempo

        result[tid] = {
            "adj_oe": adj_oe_map[tid],
            "adj_de": adj_de_map[tid],
            "adj_tempo": adj_tempo,
            "raw_oe": raw[tid]["raw_oe"],
            "raw_de": raw[tid]["raw_de"],
            "sos_oe": sos_oe,
            "sos_de": sos_de,
            "games_played": raw[tid]["games_played"],
            "iterations": iterations_used,
        }

    return result
