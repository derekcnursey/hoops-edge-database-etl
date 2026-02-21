"""Gold table: player_season_impact.

Player efficiency and impact metrics per season: usage, efficiency, per-40-minute
stats, shooting breakdown, and recruiting rank context.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

import pyarrow as pa

from ..config import Config
from ..normalize import normalize_records
from ..s3_io import S3IO
from ._io_helpers import pydict_get, read_silver_table


def build(cfg: Config, season: int) -> pa.Table:
    """Build the player_season_impact gold table for a given season.

    Args:
        cfg: Pipeline configuration.
        season: Season year (e.g. 2024).

    Returns:
        A ``pyarrow.Table`` with one row per player per season containing
        efficiency metrics, per-40-min stats, and recruiting context.
    """
    s3 = S3IO(cfg.bucket, cfg.region)

    # ------------------------------------------------------------------
    # 1. Read player season stats (spine)
    # ------------------------------------------------------------------
    stats = read_silver_table(s3, cfg, "fct_player_season_stats", season=season)
    if stats.num_rows == 0:
        return _empty_table()

    pids = pydict_get(stats, "playerId")
    # Try multiple column name conventions
    games_col = _first_available(stats, ["games", "gamesPlayed", "gp", "g"])
    mins_col = _first_available(stats, ["minutes", "minutesPlayed", "min", "mpg"])
    pts_col = _first_available(stats, ["points", "pts"])
    reb_col = _first_available(stats, ["rebounds", "totalRebounds", "reb", "trb"])
    ast_col = _first_available(stats, ["assists", "ast"])
    stl_col = _first_available(stats, ["steals", "stl"])
    blk_col = _first_available(stats, ["blocks", "blk"])
    tov_col = _first_available(stats, ["turnovers", "to", "tov"])
    fgm_col = _first_available(stats, ["fieldGoalsMade", "fgm", "fg"])
    fga_col = _first_available(stats, ["fieldGoalsAttempted", "fga"])
    fg3m_col = _first_available(stats, ["threePointFieldGoalsMade", "fg3m", "threeFGM", "threesMade"])
    fg3a_col = _first_available(stats, ["threePointFieldGoalsAttempted", "fg3a", "threeFGA", "threesAttempted"])
    ftm_col = _first_available(stats, ["freeThrowsMade", "ftm", "ft"])
    fta_col = _first_available(stats, ["freeThrowsAttempted", "fta"])
    team_col = _first_available(stats, ["team", "school", "teamName"])
    conf_col = _first_available(stats, ["conference", "conf"])

    # ------------------------------------------------------------------
    # 2. Recruiting data
    # ------------------------------------------------------------------
    recruit_lookup = _build_recruit_lookup(s3, cfg, season)

    # ------------------------------------------------------------------
    # 3. Team enrichment
    # ------------------------------------------------------------------
    team_lookup = _build_team_lookup(s3, cfg)

    # ------------------------------------------------------------------
    # 4. Compute derived metrics
    # ------------------------------------------------------------------
    records: List[Dict[str, Any]] = []
    for i, pid in enumerate(pids):
        if pid is None:
            continue

        pid_int = int(pid)
        gp = _to_float(games_col[i]) if games_col else None
        mins = _to_float(mins_col[i]) if mins_col else None
        pts = _to_float(pts_col[i]) if pts_col else None
        reb = _to_float(reb_col[i]) if reb_col else None
        ast = _to_float(ast_col[i]) if ast_col else None
        stl = _to_float(stl_col[i]) if stl_col else None
        blk = _to_float(blk_col[i]) if blk_col else None
        tov = _to_float(tov_col[i]) if tov_col else None
        fgm = _to_float(fgm_col[i]) if fgm_col else None
        fga = _to_float(fga_col[i]) if fga_col else None
        fg3m = _to_float(fg3m_col[i]) if fg3m_col else None
        fg3a = _to_float(fg3a_col[i]) if fg3a_col else None
        ftm = _to_float(ftm_col[i]) if ftm_col else None
        fta = _to_float(fta_col[i]) if fta_col else None
        team = team_col[i] if team_col else None
        conf = conf_col[i] if conf_col else None

        mpg = _safe_div(mins, gp)
        ppg = _safe_div(pts, gp)
        rpg = _safe_div(reb, gp)
        apg = _safe_div(ast, gp)

        fg_pct = _safe_div(fgm, fga) if fga and fga > 0 else None
        fg3_pct = _safe_div(fg3m, fg3a) if fg3a and fg3a > 0 else None
        ft_pct = _safe_div(ftm, fta) if fta and fta > 0 else None

        efg_pct = None
        if fgm is not None and fg3m is not None and fga is not None and fga > 0:
            efg_pct = (fgm + 0.5 * fg3m) / fga

        true_shooting = None
        if pts is not None and fga is not None and fta is not None:
            denom = 2 * (fga + 0.44 * fta)
            if denom > 0:
                true_shooting = pts / denom

        # Approximate usage rate: (FGA + 0.44*FTA + TOV) / minutes * mpg_factor
        # This is a simplified per-minute usage proxy
        usage_rate = None
        if fga is not None and fta is not None and tov is not None and mins is not None and mins > 0:
            usage_rate = (fga + 0.44 * fta + (tov or 0)) / mins

        per_40_pts = _safe_div(pts, mins, scale=40.0) if mins and mins > 0 else None
        per_40_reb = _safe_div(reb, mins, scale=40.0) if mins and mins > 0 else None
        per_40_ast = _safe_div(ast, mins, scale=40.0) if mins and mins > 0 else None

        ast_to = None
        if ast is not None and tov is not None and tov > 0:
            ast_to = ast / tov

        # Recruiting info
        rec_info = recruit_lookup.get(pid_int, {})

        # Team enrichment if not in stats
        if team is None or conf is None:
            # We cannot easily link playerId to teamId without additional data
            pass

        records.append({
            "playerId": pid_int,
            "season": season,
            "team": str(team) if team is not None else None,
            "conference": str(conf) if conf is not None else None,
            "games": int(gp) if gp is not None else None,
            "minutes": mins,
            "mpg": mpg,
            "points": pts,
            "ppg": ppg,
            "rebounds": reb,
            "rpg": rpg,
            "assists": ast,
            "apg": apg,
            "steals": stl,
            "blocks": blk,
            "turnovers": tov,
            "fgm": fgm,
            "fga": fga,
            "fg_pct": fg_pct,
            "fg3m": fg3m,
            "fg3a": fg3a,
            "fg3_pct": fg3_pct,
            "ftm": ftm,
            "fta": fta,
            "ft_pct": ft_pct,
            "efg_pct": efg_pct,
            "true_shooting": true_shooting,
            "usage_rate": usage_rate,
            "per_40_pts": per_40_pts,
            "per_40_reb": per_40_reb,
            "per_40_ast": per_40_ast,
            "ast_to_ratio": ast_to,
            "recruiting_rank": rec_info.get("rank"),
            "recruiting_stars": rec_info.get("stars"),
            "recruiting_rating": rec_info.get("rating"),
        })

    if not records:
        return _empty_table()

    return normalize_records("player_season_impact", records)


# ------------------------------------------------------------------
# Private helpers
# ------------------------------------------------------------------

def _first_available(table: pa.Table, candidates: List[str]) -> Optional[List]:
    """Return the first column found in the table as a Python list, or None."""
    for col in candidates:
        if col in table.column_names:
            return table.column(col).to_pylist()
    return None


def _to_float(val: Any) -> Optional[float]:
    """Convert a value to float, returning None on failure."""
    if val is None:
        return None
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def _safe_div(
    numerator: Optional[float],
    denominator: Optional[float],
    scale: float = 1.0,
) -> Optional[float]:
    """Safely divide, returning None if denominator is zero or inputs are None."""
    if numerator is None or denominator is None or denominator == 0:
        return None
    return (numerator / denominator) * scale


def _build_recruit_lookup(
    s3: S3IO, cfg: Config, season: int,
) -> Dict[int, Dict[str, Any]]:
    """Build playerId -> {rank, stars, rating} from fct_recruiting_players."""
    rec = read_silver_table(s3, cfg, "fct_recruiting_players", season=season)
    lookup: Dict[int, Dict[str, Any]] = {}
    if rec.num_rows == 0:
        return lookup
    pids = pydict_get(rec, "playerId")
    ranks = pydict_get(rec, "ranking") if "ranking" in rec.column_names else pydict_get(rec, "rank")
    stars = pydict_get(rec, "stars")
    ratings = pydict_get(rec, "rating")
    for i, pid in enumerate(pids):
        if pid is None:
            continue
        lookup[int(pid)] = {
            "rank": ranks[i] if ranks else None,
            "stars": stars[i] if stars else None,
            "rating": ratings[i] if ratings else None,
        }
    return lookup


def _build_team_lookup(
    s3: S3IO, cfg: Config,
) -> Dict[int, Dict[str, Optional[str]]]:
    """Build teamId -> {school, conference} from dim_teams."""
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


def _empty_table() -> pa.Table:
    """Return an empty table with the player_season_impact schema."""
    return normalize_records("player_season_impact", [])
