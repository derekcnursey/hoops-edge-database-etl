"""Gold table: team_season_summary.

Comprehensive team season profile: win/loss record, conference record,
offensive/defensive rankings, key statistical summaries, and recruiting
class quality. One row per team per season.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Set, Tuple

import pyarrow as pa

from ..config import Config
from ..normalize import normalize_records
from ..s3_io import S3IO
from ._io_helpers import dedup_by, filter_by_season, pydict_get, pydict_get_first, read_silver_table


def build(cfg: Config, season: int) -> pa.Table:
    """Build the team_season_summary gold table for a given season.

    Args:
        cfg: Pipeline configuration.
        season: Season year (e.g. 2024).

    Returns:
        A ``pyarrow.Table`` with one row per team containing season record,
        ratings, key stats, and recruiting class information.
    """
    s3 = S3IO(cfg.bucket, cfg.region)

    # ------------------------------------------------------------------
    # 1. Read dim_teams to build team spine and conference membership
    # ------------------------------------------------------------------
    dim = read_silver_table(s3, cfg, "dim_teams")
    team_lookup: Dict[int, Dict[str, Optional[str]]] = {}
    if dim.num_rows > 0:
        d_tids = pydict_get(dim, "teamId")
        d_schools = pydict_get(dim, "school")
        d_confs = pydict_get(dim, "conference")
        for i, tid in enumerate(d_tids):
            if tid is None:
                continue
            team_lookup[int(tid)] = {"school": d_schools[i], "conference": d_confs[i]}

    # ------------------------------------------------------------------
    # 2. Compute W/L record from fct_games
    # ------------------------------------------------------------------
    games = dedup_by(read_silver_table(s3, cfg, "fct_games", season=season), ["gameId"])
    record = _compute_records(games, team_lookup)

    # Build D1 team set from fct_ratings_adjusted (only D1 teams have ratings)
    adj = read_silver_table(s3, cfg, "fct_ratings_adjusted", season=season)
    d1_team_ids: Set[int] = set()
    if adj.num_rows > 0:
        adj_tids = pydict_get(adj, "teamid")
        for tid in adj_tids:
            if tid is not None:
                d1_team_ids.add(int(tid))

    # If no games found, use ratings as spine
    if not record:
        if not d1_team_ids:
            return _empty_table()
        for tid in d1_team_ids:
            if tid not in record:
                record[tid] = _default_record()

    # Restrict to D1 teams (those with adjusted ratings) to avoid inflating
    # the table with D2/D3/NAIA teams that appear in exhibition games
    if d1_team_ids:
        record = {tid: rec for tid, rec in record.items() if tid in d1_team_ids}
        # Ensure all D1 teams have a record entry even if they had no games
        for tid in d1_team_ids:
            if tid not in record:
                record[tid] = _default_record()

    team_ids = sorted(record.keys())
    n = len(team_ids)
    tid_idx = {tid: i for i, tid in enumerate(team_ids)}

    # Initialize output arrays from records
    out_wins = [record[tid]["wins"] for tid in team_ids]
    out_losses = [record[tid]["losses"] for tid in team_ids]
    out_conf_wins = [record[tid]["conf_wins"] for tid in team_ids]
    out_conf_losses = [record[tid]["conf_losses"] for tid in team_ids]

    # ------------------------------------------------------------------
    # 3. Adjusted ratings (reuse adj already loaded above for D1 filtering)
    # ------------------------------------------------------------------
    out_adj_off: List[Optional[float]] = [None] * n
    out_adj_def: List[Optional[float]] = [None] * n
    out_adj_net: List[Optional[float]] = [None] * n

    if adj.num_rows > 0:
        a_tids = pydict_get(adj, "teamid")
        a_off = pydict_get_first(adj, ["offenserating", "offensiveRating"])
        a_def = pydict_get_first(adj, ["defenserating", "defensiveRating"])
        a_net = pydict_get(adj, "netrating")
        for i, tid in enumerate(a_tids):
            if tid is None:
                continue
            idx = tid_idx.get(int(tid))
            if idx is not None:
                out_adj_off[idx] = a_off[i]
                out_adj_def[idx] = a_def[i]
                out_adj_net[idx] = a_net[i]

    # ------------------------------------------------------------------
    # 4. SRS
    # ------------------------------------------------------------------
    out_srs: List[Optional[float]] = [None] * n
    srs = filter_by_season(read_silver_table(s3, cfg, "fct_ratings_srs"), season)
    if srs.num_rows > 0:
        s_tids = pydict_get(srs, "teamId")
        s_ratings = pydict_get(srs, "rating")
        for i, tid in enumerate(s_tids):
            if tid is None:
                continue
            idx = tid_idx.get(int(tid))
            if idx is not None:
                out_srs[idx] = s_ratings[i]

    # ------------------------------------------------------------------
    # 5. PBP rollup for Four Factors and pace
    # ------------------------------------------------------------------
    out_ppg: List[Optional[float]] = [None] * n
    out_opp_ppg: List[Optional[float]] = [None] * n
    out_margin: List[Optional[float]] = [None] * n
    out_efg: List[Optional[float]] = [None] * n
    out_opp_efg: List[Optional[float]] = [None] * n
    out_tov: List[Optional[float]] = [None] * n
    out_opp_tov: List[Optional[float]] = [None] * n
    out_oreb: List[Optional[float]] = [None] * n
    out_opp_oreb: List[Optional[float]] = [None] * n
    out_ftr: List[Optional[float]] = [None] * n
    out_opp_ftr: List[Optional[float]] = [None] * n
    out_pace: List[Optional[float]] = [None] * n

    rollup = read_silver_table(s3, cfg, "fct_pbp_team_daily_rollup", season=season)
    if rollup.num_rows > 0:
        ru_tids = pydict_get(rollup, "teamid")
        ru_ppg = pydict_get(rollup, "team_points_per_game")
        ru_opp_ppg = pydict_get(rollup, "opp_points_per_game")
        ru_efg = pydict_get(rollup, "team_efg_pct")
        ru_opp_efg = pydict_get(rollup, "opp_efg_pct")
        ru_tov = pydict_get(rollup, "team_tov_ratio")
        ru_opp_tov = pydict_get(rollup, "opp_tov_ratio")
        ru_oreb = pydict_get(rollup, "team_oreb_pct")
        ru_opp_oreb = pydict_get(rollup, "opp_oreb_pct")
        ru_ftr = pydict_get(rollup, "team_ft_rate")
        ru_opp_ftr = pydict_get(rollup, "opp_ft_rate")
        ru_pace = pydict_get(rollup, "pace")

        for i, tid in enumerate(ru_tids):
            if tid is None:
                continue
            idx = tid_idx.get(int(tid))
            if idx is None:
                continue
            out_ppg[idx] = ru_ppg[i]
            out_opp_ppg[idx] = ru_opp_ppg[i]
            if ru_ppg[i] is not None and ru_opp_ppg[i] is not None:
                out_margin[idx] = ru_ppg[i] - ru_opp_ppg[i]
            out_efg[idx] = ru_efg[i]
            out_opp_efg[idx] = ru_opp_efg[i]
            out_tov[idx] = ru_tov[i]
            out_opp_tov[idx] = ru_opp_tov[i]
            out_oreb[idx] = ru_oreb[i]
            out_opp_oreb[idx] = ru_opp_oreb[i]
            out_ftr[idx] = ru_ftr[i]
            out_opp_ftr[idx] = ru_opp_ftr[i]
            out_pace[idx] = ru_pace[i]

    # ------------------------------------------------------------------
    # 6. Recruiting class aggregation
    # ------------------------------------------------------------------
    out_rec_avg: List[Optional[float]] = [None] * n
    out_rec_top: List[Optional[int]] = [None] * n
    out_rec_size: List[Optional[int]] = [None] * n

    recruiting = read_silver_table(s3, cfg, "fct_recruiting_players", season=season)
    if recruiting.num_rows > 0:
        _aggregate_recruiting(recruiting, team_lookup, tid_idx, out_rec_avg, out_rec_top, out_rec_size)

    # ------------------------------------------------------------------
    # 7. Assemble records
    # ------------------------------------------------------------------
    records: List[Dict[str, Any]] = []
    for i, tid in enumerate(team_ids):
        t_info = team_lookup.get(tid, {})
        w = out_wins[i] or 0
        l = out_losses[i] or 0
        cw = out_conf_wins[i] or 0
        cl = out_conf_losses[i] or 0
        records.append({
            "teamId": tid,
            "season": season,
            "team": t_info.get("school"),
            "conference": t_info.get("conference"),
            "wins": w,
            "losses": l,
            "win_pct": w / (w + l) if (w + l) > 0 else None,
            "conf_wins": cw,
            "conf_losses": cl,
            "conf_win_pct": cw / (cw + cl) if (cw + cl) > 0 else None,
            "ppg": out_ppg[i],
            "opp_ppg": out_opp_ppg[i],
            "margin": out_margin[i],
            "adj_off_rating": out_adj_off[i],
            "adj_def_rating": out_adj_def[i],
            "adj_net_rating": out_adj_net[i],
            "srs_rating": out_srs[i],
            "efg_pct": out_efg[i],
            "opp_efg_pct": out_opp_efg[i],
            "tov_ratio": out_tov[i],
            "opp_tov_ratio": out_opp_tov[i],
            "oreb_pct": out_oreb[i],
            "opp_oreb_pct": out_opp_oreb[i],
            "ft_rate": out_ftr[i],
            "opp_ft_rate": out_opp_ftr[i],
            "pace": out_pace[i],
            "recruiting_avg_rating": out_rec_avg[i],
            "recruiting_top_star": out_rec_top[i],
            "recruiting_class_size": out_rec_size[i],
        })

    if not records:
        return _empty_table()

    return normalize_records("team_season_summary", records)


# ------------------------------------------------------------------
# Private helpers
# ------------------------------------------------------------------

def _default_record() -> Dict[str, int]:
    """Return default record counters."""
    return {"wins": 0, "losses": 0, "conf_wins": 0, "conf_losses": 0}


def _compute_records(
    games: pa.Table,
    team_lookup: Dict[int, Dict[str, Optional[str]]],
) -> Dict[int, Dict[str, int]]:
    """Compute W/L and conference W/L from fct_games.

    Returns a dict keyed by teamId with wins, losses, conf_wins, conf_losses.
    """
    record: Dict[int, Dict[str, int]] = {}
    if games.num_rows == 0:
        return record

    g_home = pydict_get(games, "homeTeamId")
    g_away = pydict_get(games, "awayTeamId")
    g_hs = pydict_get_first(games, ["homeScore", "homePoints"])
    g_as = pydict_get_first(games, ["awayScore", "awayPoints"])

    for i in range(len(g_home)):
        h = g_home[i]
        a = g_away[i]
        hs = g_hs[i]
        aws = g_as[i]

        if h is None or a is None or hs is None or aws is None:
            continue

        h_int = int(h)
        a_int = int(a)

        record.setdefault(h_int, _default_record())
        record.setdefault(a_int, _default_record())

        h_conf = team_lookup.get(h_int, {}).get("conference")
        a_conf = team_lookup.get(a_int, {}).get("conference")
        is_conf = h_conf is not None and a_conf is not None and h_conf == a_conf

        try:
            hs_int = int(hs)
            as_int = int(aws)
        except (TypeError, ValueError):
            continue

        if hs_int > as_int:
            record[h_int]["wins"] += 1
            record[a_int]["losses"] += 1
            if is_conf:
                record[h_int]["conf_wins"] += 1
                record[a_int]["conf_losses"] += 1
        elif as_int > hs_int:
            record[a_int]["wins"] += 1
            record[h_int]["losses"] += 1
            if is_conf:
                record[a_int]["conf_wins"] += 1
                record[h_int]["conf_losses"] += 1

    return record


def _aggregate_recruiting(
    recruiting: pa.Table,
    team_lookup: Dict[int, Dict[str, Optional[str]]],
    tid_idx: Dict[int, int],
    out_avg: List[Optional[float]],
    out_top: List[Optional[int]],
    out_size: List[Optional[int]],
) -> None:
    """Aggregate recruiting data by team using school name matching."""
    # Try to get committedTo or school column for team association
    r_teams: Optional[List] = None
    for col in ("committedTo", "school", "team"):
        if col in recruiting.column_names:
            r_teams = pydict_get(recruiting, col)
            break

    r_stars = pydict_get(recruiting, "stars") if "stars" in recruiting.column_names else None
    r_ratings = pydict_get(recruiting, "rating") if "rating" in recruiting.column_names else None

    if r_teams is None:
        return

    # Build school -> teamId map from team_lookup
    school_to_tid: Dict[str, int] = {}
    for tid, info in team_lookup.items():
        school = info.get("school")
        if school:
            school_to_tid[school.lower()] = tid

    # Aggregate per team
    team_ratings: Dict[int, List[float]] = {}
    team_stars: Dict[int, List[int]] = {}

    for i, team in enumerate(r_teams):
        if team is None:
            continue
        tid = school_to_tid.get(str(team).lower())
        if tid is None:
            continue
        idx = tid_idx.get(tid)
        if idx is None:
            continue

        if r_ratings and r_ratings[i] is not None:
            team_ratings.setdefault(tid, []).append(float(r_ratings[i]))
        if r_stars and r_stars[i] is not None:
            try:
                team_stars.setdefault(tid, []).append(int(r_stars[i]))
            except (TypeError, ValueError):
                pass

    for tid, ratings in team_ratings.items():
        idx = tid_idx[tid]
        out_avg[idx] = sum(ratings) / len(ratings) if ratings else None
        out_size[idx] = len(ratings)

    for tid, stars in team_stars.items():
        idx = tid_idx[tid]
        out_top[idx] = max(stars) if stars else None


def _empty_table() -> pa.Table:
    """Return an empty table with the team_season_summary schema."""
    return normalize_records("team_season_summary", [])
