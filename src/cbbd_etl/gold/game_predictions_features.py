"""Gold table: game_predictions_features.

Pre-game feature vector for each game. Two rows per game (one per team side).
Merges team ratings, season-to-date stats, and lines into a single ML-ready table.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

import pyarrow as pa

from ..config import Config
from ..normalize import normalize_records
from ..s3_io import S3IO
from ._io_helpers import dedup_by, filter_by_season, pydict_get, pydict_get_first, read_silver_table


def build(cfg: Config, season: int) -> pa.Table:
    """Build the game_predictions_features gold table for a given season.

    Args:
        cfg: Pipeline configuration.
        season: Season year (e.g. 2024).

    Returns:
        A ``pyarrow.Table`` with two rows per game (one home, one away),
        containing pre-game features and outcome labels.
    """
    s3 = S3IO(cfg.bucket, cfg.region)

    # ------------------------------------------------------------------
    # 1. Read fct_games (spine)
    # ------------------------------------------------------------------
    games = dedup_by(read_silver_table(s3, cfg, "fct_games", season=season), ["gameId"])
    if games.num_rows == 0:
        return _empty_table()

    g_ids = pydict_get(games, "gameId")
    g_home = pydict_get(games, "homeTeamId")
    g_away = pydict_get(games, "awayTeamId")
    g_home_score = pydict_get_first(games, ["homeScore", "homePoints"])
    g_away_score = pydict_get_first(games, ["awayScore", "awayPoints"])
    g_dates: List[Optional[str]] = []
    for col_name in ("startDate", "startTime", "date"):
        if col_name in games.column_names:
            g_dates = pydict_get(games, col_name)
            break
    if not g_dates:
        g_dates = [None] * games.num_rows

    # ------------------------------------------------------------------
    # 2. Build lookup dicts for ratings / SRS / rollup / dim_teams
    # ------------------------------------------------------------------
    adj_lookup = _build_adj_lookup(s3, cfg, season)
    srs_lookup = _build_srs_lookup(s3, cfg, season)
    rollup_lookup = _build_rollup_lookup(s3, cfg, season)
    team_lookup = _build_team_lookup(s3, cfg)
    lines_lookup = _build_lines_lookup(s3, cfg, season)

    # ------------------------------------------------------------------
    # 3. Generate two rows per game
    # ------------------------------------------------------------------
    records: List[Dict[str, Any]] = []
    for i in range(len(g_ids)):
        gid = g_ids[i]
        home_tid = g_home[i]
        away_tid = g_away[i]
        if gid is None:
            continue

        date_str = str(g_dates[i])[:10] if g_dates[i] else None
        hs = g_home_score[i]
        aws = g_away_score[i]

        line_info = lines_lookup.get(int(gid) if gid else None, {})

        for is_home in (True, False):
            tid = home_tid if is_home else away_tid
            oid = away_tid if is_home else home_tid
            if tid is None or oid is None:
                continue

            tid_int = int(tid)
            oid_int = int(oid)

            t_info = team_lookup.get(tid_int, {})
            o_info = team_lookup.get(oid_int, {})

            t_adj = adj_lookup.get(tid_int, {})
            o_adj = adj_lookup.get(oid_int, {})

            t_srs = srs_lookup.get(tid_int)
            o_srs = srs_lookup.get(oid_int)

            t_ru = rollup_lookup.get(tid_int, {})
            o_ru = rollup_lookup.get(oid_int, {})

            # Compute team-relative spread
            spread = line_info.get("spread")
            if spread is not None and not is_home:
                spread = -spread

            team_ml = line_info.get("homeMoneyline") if is_home else line_info.get("awayMoneyline")
            opp_ml = line_info.get("awayMoneyline") if is_home else line_info.get("homeMoneyline")

            # Compute labels
            team_score: Optional[int] = None
            opp_score: Optional[int] = None
            team_win: Optional[bool] = None
            if hs is not None and aws is not None:
                team_score = int(hs) if is_home else int(aws)
                opp_score = int(aws) if is_home else int(hs)
                team_win = team_score > opp_score

            t_conf = t_info.get("conference")
            o_conf = o_info.get("conference")
            is_conf_game = (
                t_conf is not None
                and o_conf is not None
                and t_conf == o_conf
            )

            records.append({
                "gameId": int(gid),
                "season": season,
                "game_date": date_str,
                "teamId": tid_int,
                "opponentId": oid_int,
                "is_home": is_home,
                "team_name": t_info.get("school"),
                "team_conference": t_conf,
                "opp_name": o_info.get("school"),
                "opp_conference": o_conf,
                "is_conference_game": is_conf_game,
                "spread": spread,
                "over_under": line_info.get("overUnder"),
                "team_moneyline": team_ml,
                "opp_moneyline": opp_ml,
                "team_adj_off": t_adj.get("off"),
                "team_adj_def": t_adj.get("def"),
                "team_adj_net": t_adj.get("net"),
                "opp_adj_off": o_adj.get("off"),
                "opp_adj_def": o_adj.get("def"),
                "opp_adj_net": o_adj.get("net"),
                "team_srs": t_srs,
                "opp_srs": o_srs,
                "team_ppg": t_ru.get("ppg"),
                "team_opp_ppg": t_ru.get("opp_ppg"),
                "team_pace": t_ru.get("pace"),
                "opp_ppg": o_ru.get("ppg"),
                "opp_opp_ppg": o_ru.get("opp_ppg"),
                "opp_pace": o_ru.get("pace"),
                "team_efg_pct": t_ru.get("efg"),
                "team_tov_ratio": t_ru.get("tov"),
                "team_oreb_pct": t_ru.get("oreb"),
                "team_ft_rate": t_ru.get("ftr"),
                "opp_efg_pct": o_ru.get("efg"),
                "opp_tov_ratio": o_ru.get("tov"),
                "opp_oreb_pct": o_ru.get("oreb"),
                "opp_ft_rate": o_ru.get("ftr"),
                "team_score": team_score,
                "opp_score": opp_score,
                "team_win": team_win,
            })

    if not records:
        return _empty_table()

    return normalize_records("game_predictions_features", records)


# ------------------------------------------------------------------
# Private lookup builders
# ------------------------------------------------------------------

def _build_adj_lookup(
    s3: S3IO, cfg: Config, season: int,
) -> Dict[int, Dict[str, Optional[float]]]:
    """Build a teamId -> {off, def, net} lookup from adjusted ratings."""
    adj = read_silver_table(s3, cfg, "fct_ratings_adjusted", season=season)
    lookup: Dict[int, Dict[str, Optional[float]]] = {}
    if adj.num_rows == 0:
        return lookup
    tids = pydict_get(adj, "teamid")
    offs = pydict_get_first(adj, ["offenserating", "offensiveRating"])
    defs = pydict_get_first(adj, ["defenserating", "defensiveRating"])
    nets = pydict_get(adj, "netrating")
    for i, tid in enumerate(tids):
        if tid is None:
            continue
        lookup[int(tid)] = {"off": offs[i], "def": defs[i], "net": nets[i]}
    return lookup


def _build_srs_lookup(
    s3: S3IO, cfg: Config, season: int,
) -> Dict[int, Optional[float]]:
    """Build a teamId -> srs_rating lookup."""
    srs = filter_by_season(read_silver_table(s3, cfg, "fct_ratings_srs"), season)
    lookup: Dict[int, Optional[float]] = {}
    if srs.num_rows == 0:
        return lookup
    tids = pydict_get(srs, "teamId")
    ratings = pydict_get(srs, "rating")
    for i, tid in enumerate(tids):
        if tid is None:
            continue
        lookup[int(tid)] = ratings[i]
    return lookup


def _build_rollup_lookup(
    s3: S3IO, cfg: Config, season: int,
) -> Dict[int, Dict[str, Optional[float]]]:
    """Build a teamId -> {ppg, opp_ppg, pace, efg, tov, oreb, ftr} lookup."""
    ru = read_silver_table(s3, cfg, "fct_pbp_team_daily_rollup", season=season)
    lookup: Dict[int, Dict[str, Optional[float]]] = {}
    if ru.num_rows == 0:
        return lookup
    tids = pydict_get(ru, "teamid")
    ppg = pydict_get(ru, "team_points_per_game")
    opp_ppg = pydict_get(ru, "opp_points_per_game")
    pace = pydict_get(ru, "pace")
    efg = pydict_get(ru, "team_efg_pct")
    tov = pydict_get(ru, "team_tov_ratio")
    oreb = pydict_get(ru, "team_oreb_pct")
    ftr = pydict_get(ru, "team_ft_rate")
    for i, tid in enumerate(tids):
        if tid is None:
            continue
        lookup[int(tid)] = {
            "ppg": ppg[i],
            "opp_ppg": opp_ppg[i],
            "pace": pace[i],
            "efg": efg[i],
            "tov": tov[i],
            "oreb": oreb[i],
            "ftr": ftr[i],
        }
    return lookup


def _build_team_lookup(
    s3: S3IO, cfg: Config,
) -> Dict[int, Dict[str, Optional[str]]]:
    """Build a teamId -> {school, conference} lookup from dim_teams."""
    dim = read_silver_table(s3, cfg, "dim_teams")
    lookup: Dict[int, Dict[str, Optional[str]]] = {}
    if dim.num_rows == 0:
        return lookup
    tids = pydict_get(dim, "teamId")
    schools = pydict_get(dim, "school")
    confs = pydict_get(dim, "conference")
    for i, tid in enumerate(tids):
        if tid is None:
            continue
        lookup[int(tid)] = {"school": schools[i], "conference": confs[i]}
    return lookup


def _build_lines_lookup(
    s3: S3IO, cfg: Config, season: int,
) -> Dict[int, Dict[str, Any]]:
    """Build a gameId -> {spread, overUnder, homeMoneyline, awayMoneyline} lookup.

    Uses the first available provider per game.
    """
    lines = read_silver_table(s3, cfg, "fct_lines", season=season)
    lookup: Dict[int, Dict[str, Any]] = {}
    if lines.num_rows == 0:
        return lookup
    gids = pydict_get(lines, "gameId")
    spreads = pydict_get(lines, "spread")
    ous = pydict_get(lines, "overUnder")
    hmls = pydict_get(lines, "homeMoneyline")
    amls = pydict_get(lines, "awayMoneyline")
    for i, gid in enumerate(gids):
        if gid is None:
            continue
        gid_int = int(gid)
        if gid_int in lookup:
            continue  # first provider wins
        lookup[gid_int] = {
            "spread": spreads[i] if i < len(spreads) else None,
            "overUnder": ous[i] if i < len(ous) else None,
            "homeMoneyline": hmls[i] if i < len(hmls) else None,
            "awayMoneyline": amls[i] if i < len(amls) else None,
        }
    return lookup


def _empty_table() -> pa.Table:
    """Return an empty table with the game_predictions_features schema."""
    return normalize_records("game_predictions_features", [])
