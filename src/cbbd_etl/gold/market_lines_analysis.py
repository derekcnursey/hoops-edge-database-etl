"""Gold table: market_lines_analysis.

Lines/spreads merged with actual game outcomes for against-the-spread (ATS)
analysis and market efficiency research. One row per game per provider.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

import pyarrow as pa

from ..config import Config
from ..normalize import normalize_records
from ..s3_io import S3IO
from ._io_helpers import pydict_get, read_silver_table


def build(cfg: Config, season: int) -> pa.Table:
    """Build the market_lines_analysis gold table for a given season.

    Args:
        cfg: Pipeline configuration.
        season: Season year (e.g. 2024).

    Returns:
        A ``pyarrow.Table`` with one row per game per provider, containing
        lines/spreads merged with actual outcomes.
    """
    s3 = S3IO(cfg.bucket, cfg.region)

    # ------------------------------------------------------------------
    # 1. Read fct_lines
    # ------------------------------------------------------------------
    lines = read_silver_table(s3, cfg, "fct_lines", season=season)
    if lines.num_rows == 0:
        return _empty_table()

    l_gids = pydict_get(lines, "gameId")
    l_providers = pydict_get(lines, "provider")
    l_spreads = pydict_get(lines, "spread")
    l_ous = pydict_get(lines, "overUnder")
    l_hmls = pydict_get(lines, "homeMoneyline")
    l_amls = pydict_get(lines, "awayMoneyline")

    # ------------------------------------------------------------------
    # 2. Read fct_games for outcomes
    # ------------------------------------------------------------------
    games = read_silver_table(s3, cfg, "fct_games", season=season)
    game_lookup: Dict[int, Dict[str, Any]] = {}
    if games.num_rows > 0:
        g_ids = pydict_get(games, "gameId")
        g_home = pydict_get(games, "homeTeamId")
        g_away = pydict_get(games, "awayTeamId")
        g_hs = pydict_get(games, "homeScore")
        g_as = pydict_get(games, "awayScore")

        g_dates: List[Optional[str]] = [None] * games.num_rows
        for col_name in ("startDate", "startTime", "date"):
            if col_name in games.column_names:
                g_dates = pydict_get(games, col_name)
                break

        for i, gid in enumerate(g_ids):
            if gid is None:
                continue
            game_lookup[int(gid)] = {
                "homeTeamId": g_home[i],
                "awayTeamId": g_away[i],
                "homeScore": g_hs[i],
                "awayScore": g_as[i],
                "date": str(g_dates[i])[:10] if g_dates[i] else None,
            }

    # ------------------------------------------------------------------
    # 3. Team lookup
    # ------------------------------------------------------------------
    team_lookup = _build_team_lookup(s3, cfg)

    # ------------------------------------------------------------------
    # 4. Build output rows
    # ------------------------------------------------------------------
    records: List[Dict[str, Any]] = []
    for i in range(len(l_gids)):
        gid = l_gids[i]
        if gid is None:
            continue
        gid_int = int(gid)
        game = game_lookup.get(gid_int)
        if game is None:
            continue  # inner join: only games with both lines and outcomes

        spread = _to_float(l_spreads[i])
        ou = _to_float(l_ous[i])
        home_ml = _to_float(l_hmls[i])
        away_ml = _to_float(l_amls[i])

        hs = _to_int(game.get("homeScore"))
        aws = _to_int(game.get("awayScore"))

        home_tid = game.get("homeTeamId")
        away_tid = game.get("awayTeamId")

        h_info = team_lookup.get(int(home_tid), {}) if home_tid else {}
        a_info = team_lookup.get(int(away_tid), {}) if away_tid else {}

        # Derived columns
        total_pts: Optional[int] = None
        home_margin: Optional[int] = None
        home_win: Optional[bool] = None
        home_covered: Optional[bool] = None
        over_hit: Optional[bool] = None
        ats_margin: Optional[float] = None
        total_vs_line: Optional[float] = None
        spread_error: Optional[float] = None

        if hs is not None and aws is not None:
            total_pts = hs + aws
            home_margin = hs - aws
            home_win = hs > aws

            if spread is not None:
                ats_margin = float(home_margin) + spread
                home_covered = ats_margin > 0
                spread_error = abs(float(home_margin) - (-spread))

            if ou is not None:
                total_vs_line = float(total_pts) - ou
                over_hit = float(total_pts) > ou

        records.append({
            "gameId": gid_int,
            "season": season,
            "game_date": game.get("date"),
            "provider": l_providers[i],
            "home_team": h_info.get("school"),
            "away_team": a_info.get("school"),
            "home_conference": h_info.get("conference"),
            "away_conference": a_info.get("conference"),
            "spread": spread,
            "over_under": ou,
            "home_moneyline": home_ml,
            "away_moneyline": away_ml,
            "home_score": hs,
            "away_score": aws,
            "total_points": total_pts,
            "home_margin": home_margin,
            "home_win": home_win,
            "home_covered": home_covered,
            "over_hit": over_hit,
            "ats_margin": ats_margin,
            "total_vs_line": total_vs_line,
            "spread_error": spread_error,
        })

    if not records:
        return _empty_table()

    return normalize_records("market_lines_analysis", records)


# ------------------------------------------------------------------
# Private helpers
# ------------------------------------------------------------------

def _to_float(val: Any) -> Optional[float]:
    """Convert to float, returning None on failure."""
    if val is None:
        return None
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def _to_int(val: Any) -> Optional[int]:
    """Convert to int, returning None on failure."""
    if val is None:
        return None
    try:
        return int(val)
    except (TypeError, ValueError):
        return None


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
    """Return an empty table with the market_lines_analysis schema."""
    return normalize_records("market_lines_analysis", [])
