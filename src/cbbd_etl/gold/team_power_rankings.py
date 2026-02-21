"""Gold table: team_power_rankings.

Composite team ranking combining adjusted ratings, PBP-derived efficiencies,
SRS ratings, and poll rankings. One row per team per season.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

import pyarrow as pa

from ..config import Config
from ..normalize import normalize_records
from ..s3_io import S3IO
from ._io_helpers import pydict_get, read_silver_table, safe_divide


def build(cfg: Config, season: int) -> pa.Table:
    """Build the team_power_rankings gold table for a given season.

    Args:
        cfg: Pipeline configuration.
        season: Season year (e.g. 2024).

    Returns:
        A ``pyarrow.Table`` with one row per team containing composite rankings.
    """
    s3 = S3IO(cfg.bucket, cfg.region)

    # ------------------------------------------------------------------
    # 1. Read API adjusted ratings (spine table)
    # ------------------------------------------------------------------
    adj = read_silver_table(s3, cfg, "fct_ratings_adjusted", season=season)
    if adj.num_rows == 0:
        return _empty_table()

    adj_team_ids = pydict_get(adj, "teamid")
    adj_teams = pydict_get(adj, "team")
    adj_conferences = pydict_get(adj, "conference")
    adj_off = pydict_get(adj, "offenserating")
    adj_def = pydict_get(adj, "defenserating")
    adj_net = pydict_get(adj, "netrating")
    adj_rank_off = pydict_get(adj, "ranking_offense")
    adj_rank_def = pydict_get(adj, "ranking_defense")
    adj_rank_net = pydict_get(adj, "ranking_net")

    # Build team index
    team_ids_list: List[int] = []
    team_idx: Dict[int, int] = {}
    for i, tid in enumerate(adj_team_ids):
        if tid is None:
            continue
        tid_int = int(tid)
        if tid_int not in team_idx:
            team_idx[tid_int] = len(team_ids_list)
            team_ids_list.append(tid_int)

    n = len(team_ids_list)

    # Initialize output arrays
    out_team: List[Optional[str]] = [None] * n
    out_conf: List[Optional[str]] = [None] * n
    out_adj_off: List[Optional[float]] = [None] * n
    out_adj_def: List[Optional[float]] = [None] * n
    out_adj_net: List[Optional[float]] = [None] * n
    out_rank_off: List[Optional[int]] = [None] * n
    out_rank_def: List[Optional[int]] = [None] * n
    out_rank_net: List[Optional[int]] = [None] * n

    for i, tid in enumerate(adj_team_ids):
        if tid is None:
            continue
        idx = team_idx.get(int(tid))
        if idx is None:
            continue
        out_team[idx] = adj_teams[i]
        out_conf[idx] = adj_conferences[i]
        out_adj_off[idx] = adj_off[i]
        out_adj_def[idx] = adj_def[i]
        out_adj_net[idx] = adj_net[i]
        out_rank_off[idx] = adj_rank_off[i]
        out_rank_def[idx] = adj_rank_def[i]
        out_rank_net[idx] = adj_rank_net[i]

    # ------------------------------------------------------------------
    # 2. SRS ratings
    # ------------------------------------------------------------------
    out_srs: List[Optional[float]] = [None] * n
    srs = read_silver_table(s3, cfg, "fct_ratings_srs", season=season)
    if srs.num_rows > 0:
        srs_tids = pydict_get(srs, "teamId")
        srs_ratings = pydict_get(srs, "rating")
        for i, tid in enumerate(srs_tids):
            if tid is None:
                continue
            idx = team_idx.get(int(tid))
            if idx is not None:
                out_srs[idx] = srs_ratings[i]

    # ------------------------------------------------------------------
    # 3. Poll rankings (latest poll date per type)
    # ------------------------------------------------------------------
    out_ap: List[Optional[int]] = [None] * n
    out_coaches: List[Optional[int]] = [None] * n
    rankings = read_silver_table(s3, cfg, "fct_rankings", season=season)
    if rankings.num_rows > 0:
        r_tids = pydict_get(rankings, "teamId")
        r_polls = pydict_get(rankings, "pollType")
        r_dates = pydict_get(rankings, "pollDate")
        r_ranks = pydict_get(rankings, "ranking")

        # Find latest date per poll type
        latest_dates: Dict[str, str] = {}
        for poll, d in zip(r_polls, r_dates):
            if poll is None or d is None:
                continue
            poll_s = str(poll)
            d_s = str(d)
            if poll_s not in latest_dates or d_s > latest_dates[poll_s]:
                latest_dates[poll_s] = d_s

        for i, (tid, poll, d, rank) in enumerate(zip(r_tids, r_polls, r_dates, r_ranks)):
            if tid is None or poll is None or d is None:
                continue
            poll_s = str(poll)
            if str(d) != latest_dates.get(poll_s):
                continue
            idx = team_idx.get(int(tid))
            if idx is None:
                continue
            if poll_s.lower() in ("ap top 25", "ap"):
                out_ap[idx] = rank
            elif poll_s.lower() in ("coaches poll", "coaches"):
                out_coaches[idx] = rank

    # ------------------------------------------------------------------
    # 4. PBP rollup stats
    # ------------------------------------------------------------------
    out_pbp_off: List[Optional[float]] = [None] * n
    out_pbp_def: List[Optional[float]] = [None] * n
    out_pbp_net: List[Optional[float]] = [None] * n
    out_pbp_pace: List[Optional[float]] = [None] * n
    out_games: List[Optional[int]] = [None] * n

    rollup = read_silver_table(s3, cfg, "fct_pbp_team_daily_rollup", season=season)
    if rollup.num_rows > 0:
        ru_tids = pydict_get(rollup, "teamid")
        ru_team_pts = pydict_get(rollup, "team_points_total")
        ru_opp_pts = pydict_get(rollup, "opp_points_total")
        ru_team_poss = pydict_get(rollup, "team_possessions")
        ru_opp_poss = pydict_get(rollup, "opp_possessions")
        ru_games = pydict_get(rollup, "games_played")
        ru_minutes = pydict_get(rollup, "game_minutes_total")

        for i, tid in enumerate(ru_tids):
            if tid is None:
                continue
            idx = team_idx.get(int(tid))
            if idx is None:
                continue

            tp = ru_team_pts[i]
            tposs = ru_team_poss[i]
            op = ru_opp_pts[i]
            oposs = ru_opp_poss[i]
            gp = ru_games[i]
            mins = ru_minutes[i]

            if tposs and tposs > 0:
                out_pbp_off[idx] = (tp / tposs) * 100 if tp is not None else None
            if oposs and oposs > 0:
                out_pbp_def[idx] = (op / oposs) * 100 if op is not None else None
            if out_pbp_off[idx] is not None and out_pbp_def[idx] is not None:
                out_pbp_net[idx] = out_pbp_off[idx] - out_pbp_def[idx]
            if gp and gp > 0 and tposs is not None:
                avg_poss = tposs / gp
                avg_mins = (mins / gp) if mins and mins > 0 else 40.0
                out_pbp_pace[idx] = avg_poss * (40.0 / avg_mins) if avg_mins > 0 else None
            out_games[idx] = int(gp) if gp is not None else None

    # ------------------------------------------------------------------
    # 5. PBP adjusted efficiency
    # ------------------------------------------------------------------
    out_pbp_adj_off: List[Optional[float]] = [None] * n
    out_pbp_adj_def: List[Optional[float]] = [None] * n
    out_pbp_adj_net: List[Optional[float]] = [None] * n

    adj_pbp = read_silver_table(s3, cfg, "fct_pbp_team_daily_rollup_adj", season=season)
    if adj_pbp.num_rows > 0:
        ap_tids = pydict_get(adj_pbp, "teamid")
        ap_off = pydict_get(adj_pbp, "adj_off_eff")
        ap_def = pydict_get(adj_pbp, "adj_def_eff")
        ap_net = pydict_get(adj_pbp, "adj_net_eff")

        for i, tid in enumerate(ap_tids):
            if tid is None:
                continue
            idx = team_idx.get(int(tid))
            if idx is None:
                continue
            out_pbp_adj_off[idx] = ap_off[i]
            out_pbp_adj_def[idx] = ap_def[i]
            out_pbp_adj_net[idx] = ap_net[i]

    # ------------------------------------------------------------------
    # 6. Composite rank (percentile-normalized average of net ratings)
    # ------------------------------------------------------------------
    out_composite = _compute_composite(out_adj_net, out_pbp_adj_net, out_srs)

    # ------------------------------------------------------------------
    # 7. Enrich with dim_teams if names are missing
    # ------------------------------------------------------------------
    dim = read_silver_table(s3, cfg, "dim_teams")
    if dim.num_rows > 0:
        d_tids = pydict_get(dim, "teamId")
        d_schools = pydict_get(dim, "school")
        d_confs = pydict_get(dim, "conference")
        for i, tid in enumerate(d_tids):
            if tid is None:
                continue
            idx = team_idx.get(int(tid))
            if idx is None:
                continue
            if out_team[idx] is None:
                out_team[idx] = d_schools[i]
            if out_conf[idx] is None:
                out_conf[idx] = d_confs[i]

    # ------------------------------------------------------------------
    # Assemble output records
    # ------------------------------------------------------------------
    records: List[Dict[str, Any]] = []
    for i in range(n):
        records.append({
            "teamId": team_ids_list[i],
            "season": season,
            "team": out_team[i],
            "conference": out_conf[i],
            "adj_off_rating": out_adj_off[i],
            "adj_def_rating": out_adj_def[i],
            "adj_net_rating": out_adj_net[i],
            "ranking_offense": out_rank_off[i],
            "ranking_defense": out_rank_def[i],
            "ranking_net": out_rank_net[i],
            "srs_rating": out_srs[i],
            "ap_rank": out_ap[i],
            "coaches_rank": out_coaches[i],
            "pbp_off_eff": out_pbp_off[i],
            "pbp_def_eff": out_pbp_def[i],
            "pbp_net_eff": out_pbp_net[i],
            "pbp_adj_off_eff": out_pbp_adj_off[i],
            "pbp_adj_def_eff": out_pbp_adj_def[i],
            "pbp_adj_net_eff": out_pbp_adj_net[i],
            "pbp_pace": out_pbp_pace[i],
            "games_played": out_games[i],
            "composite_rank": out_composite[i],
        })

    return normalize_records("team_power_rankings", records)


def _compute_composite(
    adj_net: List[Optional[float]],
    pbp_adj_net: List[Optional[float]],
    srs: List[Optional[float]],
) -> List[Optional[float]]:
    """Compute a composite rank by averaging percentile-normalized net ratings.

    Each input is independently percentile-ranked (0-100 scale), then averaged.
    Teams missing all three inputs receive ``None``.
    """
    def _percentile_rank(values: List[Optional[float]]) -> List[Optional[float]]:
        valid = [(i, v) for i, v in enumerate(values) if v is not None]
        if not valid:
            return [None] * len(values)
        sorted_vals = sorted(set(v for _, v in valid))
        rank_map = {v: (r / (len(sorted_vals) - 1)) * 100 if len(sorted_vals) > 1 else 50.0
                    for r, v in enumerate(sorted_vals)}
        result: List[Optional[float]] = [None] * len(values)
        for i, v in valid:
            result[i] = rank_map[v]
        return result

    p_adj = _percentile_rank(adj_net)
    p_pbp = _percentile_rank(pbp_adj_net)
    p_srs = _percentile_rank(srs)

    composite: List[Optional[float]] = []
    for a, b, c in zip(p_adj, p_pbp, p_srs):
        vals = [v for v in (a, b, c) if v is not None]
        if vals:
            composite.append(sum(vals) / len(vals))
        else:
            composite.append(None)
    return composite


def _empty_table() -> pa.Table:
    """Return an empty table with the team_power_rankings schema."""
    return normalize_records("team_power_rankings", [])
