"""Gold tables: team_adjusted_efficiencies and team_adjusted_efficiencies_no_garbage.

Computes iterative SOS-adjusted offensive/defensive efficiency ratings
(Pomeroy-style) with per-date snapshots across the season. Two variants:

- ``build``: All D1-vs-D1 possessions from API box scores (``fct_game_teams``).
- ``build_no_garbage``: Garbage time excluded (``fct_pbp_game_teams_flat_garbage_removed``).

Non-D1 games are excluded from the solver — they are essentially garbage time
and would distort the ratings by introducing teams with no meaningful schedule.
D1 teams are identified via ``dim_teams`` conference membership (365 teams with
a conference assignment are D1).
"""

from __future__ import annotations

import ast
import io
import logging
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Set, Tuple

import pyarrow as pa
import pyarrow.parquet as pq

from ..config import Config
from ..normalize import normalize_records
from ..s3_io import S3IO
from ._io_helpers import dedup_by, pydict_get, pydict_get_first, read_silver_table
from .iterative_ratings import (
    GameObs,
    compute_barthag,
    exponential_decay_weight,
    solve_ratings,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Public entry points
# ---------------------------------------------------------------------------


def _get_rating_params(cfg: Config) -> Dict:
    """Read adjusted efficiency parameters from config, with defaults."""
    adj_cfg = cfg.raw.get("gold", {}).get("adjusted_efficiencies", {})
    hl = adj_cfg.get("half_life", 30.0)
    return {
        "half_life": float(hl) if hl is not None else None,
        "hca_oe": float(adj_cfg.get("hca_oe", 1.4)),
        "hca_de": float(adj_cfg.get("hca_de", 1.4)),
        "barthag_exp": float(adj_cfg.get("barthag_exp", 11.5)),
        "sos_exponent": float(adj_cfg.get("sos_exponent", 1.0)),
        "shrinkage": float(adj_cfg.get("shrinkage", 0.0)),
    }


def _get_margin_cap(cfg: Config) -> Optional[float]:
    """Read optional margin cap from config."""
    adj_cfg = cfg.raw.get("gold", {}).get("adjusted_efficiencies", {})
    cap = adj_cfg.get("margin_cap")
    return float(cap) if cap is not None else None


def _get_preseason_regression(cfg: Config) -> Optional[float]:
    """Read optional preseason regression factor from config.

    Returns None to disable preseason priors, or a float in [0, 1]
    where 0 = use raw prior, 1 = all league average.
    """
    adj_cfg = cfg.raw.get("gold", {}).get("adjusted_efficiencies", {})
    val = adj_cfg.get("preseason_regression")
    return float(val) if val is not None else None


def _load_preseason_prior(
    s3: S3IO,
    cfg: Config,
    season: int,
    gold_table_name: str,
    regression: float,
) -> Optional[Dict[int, Tuple[float, float]]]:
    """Load previous season's final ratings and regress toward league average.

    For each team, takes their last rating_date in the prior season's gold table
    and computes: prior = (1 - regression) * final + regression * league_avg.

    Args:
        s3: S3IO instance.
        cfg: Pipeline config.
        season: Current season (will load season - 1).
        gold_table_name: Gold table to read (e.g. "team_adjusted_efficiencies_no_garbage").
        regression: Regression factor toward league mean (0-1).

    Returns:
        Dict of {team_id: (prior_oe, prior_de)} or None if no prior data.
    """
    prior_season = season - 1
    gold_prefix = cfg.s3_layout["gold_prefix"]
    prefix = f"{gold_prefix}/{gold_table_name}/season={prior_season}/"

    keys = s3.list_keys(prefix)
    parquet_keys = [k for k in keys if k.endswith(".parquet")]

    if not parquet_keys:
        logger.info("no prior season %d gold data for %s", prior_season, gold_table_name)
        return None

    # Read all parquet files for the prior season
    tables = []
    for key in parquet_keys:
        data = s3.get_object_bytes(key)
        tbl = pq.read_table(
            io.BytesIO(data),
            columns=["teamId", "rating_date", "adj_oe", "adj_de"],
        )
        tables.append(tbl)

    if not tables:
        return None

    combined = pa.concat_tables(tables, promote_options="permissive")
    if combined.num_rows == 0:
        return None

    # Extract columns
    team_ids = combined.column("teamId").to_pylist()
    dates = combined.column("rating_date").to_pylist()
    adj_oes = combined.column("adj_oe").to_pylist()
    adj_des = combined.column("adj_de").to_pylist()

    # Find the latest rating_date per team
    latest: Dict[int, Tuple[str, float, float]] = {}
    for i in range(len(team_ids)):
        tid = team_ids[i]
        dt = str(dates[i])[:10] if dates[i] is not None else ""
        oe = adj_oes[i]
        de = adj_des[i]
        if tid is None or oe is None or de is None:
            continue
        tid = int(tid)
        existing = latest.get(tid)
        if existing is None or dt > existing[0]:
            latest[tid] = (dt, float(oe), float(de))

    if not latest:
        return None

    # Compute league average from final ratings
    all_oe = [v[1] for v in latest.values()]
    all_de = [v[2] for v in latest.values()]
    league_avg_oe = sum(all_oe) / len(all_oe)
    league_avg_de = sum(all_de) / len(all_de)

    # Recenter so OE and DE have the same mean. Prior seasons can have
    # systematic OE/DE offset that distorts SOS adjustments in the solver
    # (league_avg / opp_de ratio shifts when means differ).
    grand_mean = (league_avg_oe + league_avg_de) / 2.0
    oe_shift = grand_mean - league_avg_oe
    de_shift = grand_mean - league_avg_de

    # Regress toward league average, with recentering
    prior: Dict[int, Tuple[float, float]] = {}
    for tid, (_, final_oe, final_de) in latest.items():
        centered_oe = final_oe + oe_shift
        centered_de = final_de + de_shift
        prior_oe = (1.0 - regression) * centered_oe + regression * grand_mean
        prior_de = (1.0 - regression) * centered_de + regression * grand_mean
        prior[tid] = (prior_oe, prior_de)

    logger.info(
        "loaded preseason prior from season %d: %d teams, regression=%.2f, "
        "league_avg_oe=%.2f, league_avg_de=%.2f, recentered to grand_mean=%.2f "
        "(oe_shift=%+.2f, de_shift=%+.2f)",
        prior_season, len(prior), regression, league_avg_oe, league_avg_de,
        grand_mean, oe_shift, de_shift,
    )

    return prior


def _apply_margin_cap(
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


def build(cfg: Config, season: int) -> pa.Table:
    """Build team_adjusted_efficiencies from fct_game_teams (API box scores)."""
    s3 = S3IO(cfg.bucket, cfg.region)
    params = _get_rating_params(cfg)
    margin_cap = _get_margin_cap(cfg)
    preseason_regression = _get_preseason_regression(cfg)

    d1_ids = _load_d1_team_ids(s3, cfg)
    team_info = _load_team_info(s3, cfg)
    games_by_date = _load_box_score_games(s3, cfg, season, d1_ids)

    if not games_by_date:
        return normalize_records("team_adjusted_efficiencies", [])

    if margin_cap is not None:
        logger.info("applying margin cap of %s pts", margin_cap)
        games_by_date = _apply_margin_cap(games_by_date, margin_cap)

    # Load preseason prior from previous season if configured
    preseason_prior = None
    if preseason_regression is not None:
        preseason_prior = _load_preseason_prior(
            s3, cfg, season, "team_adjusted_efficiencies", preseason_regression
        )

    records = _run_per_date_ratings(
        games_by_date, team_info, season, preseason_prior=preseason_prior, **params
    )
    if not records:
        return normalize_records("team_adjusted_efficiencies", [])

    return normalize_records("team_adjusted_efficiencies", records)


def build_no_garbage(cfg: Config, season: int) -> pa.Table:
    """Build team_adjusted_efficiencies_no_garbage from PBP garbage-removed data."""
    s3 = S3IO(cfg.bucket, cfg.region)
    params = _get_rating_params(cfg)
    margin_cap = _get_margin_cap(cfg)
    preseason_regression = _get_preseason_regression(cfg)

    d1_ids = _load_d1_team_ids(s3, cfg)
    team_info = _load_team_info(s3, cfg)
    games_by_date = _load_pbp_no_garbage_games(s3, cfg, season, d1_ids)

    if not games_by_date:
        return normalize_records("team_adjusted_efficiencies_no_garbage", [])

    if margin_cap is not None:
        logger.info("applying margin cap of %s pts", margin_cap)
        games_by_date = _apply_margin_cap(games_by_date, margin_cap)

    # Load preseason prior from previous season if configured
    preseason_prior = None
    if preseason_regression is not None:
        preseason_prior = _load_preseason_prior(
            s3, cfg, season, "team_adjusted_efficiencies_no_garbage",
            preseason_regression,
        )

    records = _run_per_date_ratings(
        games_by_date, team_info, season, preseason_prior=preseason_prior, **params
    )
    if not records:
        return normalize_records("team_adjusted_efficiencies_no_garbage", [])

    return normalize_records("team_adjusted_efficiencies_no_garbage", records)


# ---------------------------------------------------------------------------
# D1 identification
# ---------------------------------------------------------------------------


def _load_d1_team_ids(s3: S3IO, cfg: Config) -> Set[int]:
    """Return the set of D1 team IDs (teams with a conference in dim_teams)."""
    dim = read_silver_table(s3, cfg, "dim_teams")
    d1: Set[int] = set()
    if dim.num_rows == 0:
        return d1
    tids = pydict_get(dim, "teamId")
    confs = pydict_get(dim, "conference")
    for i, tid in enumerate(tids):
        if tid is not None and confs[i] is not None and str(confs[i]).strip():
            d1.add(int(tid))
    return d1


# ---------------------------------------------------------------------------
# Data loaders
# ---------------------------------------------------------------------------


def _load_box_score_games(
    s3: S3IO, cfg: Config, season: int, d1_ids: Set[int]
) -> Dict[str, List[GameObs]]:
    """Parse fct_game_teams.teamStats + fct_games for dates and neutralSite.

    Only includes games where BOTH teams are D1.
    """
    # Read fct_games for date + neutral site
    fct_games = dedup_by(
        read_silver_table(s3, cfg, "fct_games", season=season), ["gameId"]
    )
    if fct_games.num_rows == 0:
        return {}

    game_dates: Dict[int, str] = {}
    game_neutral: Dict[int, bool] = {}
    game_home: Dict[int, int] = {}
    game_away: Dict[int, int] = {}

    g_ids = pydict_get(fct_games, "gameId")
    g_starts = pydict_get_first(fct_games, ["startDate", "start_date", "date"])
    g_neutral = pydict_get(fct_games, "neutralSite")
    g_home = pydict_get(fct_games, "homeTeamId")
    g_away = pydict_get(fct_games, "awayTeamId")

    for i, gid in enumerate(g_ids):
        if gid is None:
            continue
        gid = int(gid)
        home_id = int(g_home[i]) if g_home[i] is not None else 0
        away_id = int(g_away[i]) if g_away[i] is not None else 0

        # Skip non-D1 games
        if home_id not in d1_ids or away_id not in d1_ids:
            continue

        dt_str = _parse_date_str(g_starts[i])
        if dt_str is None:
            continue
        game_dates[gid] = dt_str
        game_neutral[gid] = bool(g_neutral[i]) if g_neutral[i] is not None else False
        game_home[gid] = home_id
        game_away[gid] = away_id

    # Read fct_game_teams
    gt = dedup_by(
        read_silver_table(s3, cfg, "fct_game_teams", season=season),
        ["gameId", "teamId"],
    )
    if gt.num_rows == 0:
        return {}

    gt_gids = pydict_get(gt, "gameId")
    gt_tids = pydict_get(gt, "teamId")
    gt_stats = pydict_get(gt, "teamStats")
    gt_opp_stats = pydict_get(gt, "opponentStats")

    games_by_date: Dict[str, List[GameObs]] = {}

    for i in range(len(gt_gids)):
        gid = gt_gids[i]
        tid = gt_tids[i]
        if gid is None or tid is None:
            continue
        gid = int(gid)
        tid = int(tid)

        # Only D1 games (already filtered at fct_games level)
        if gid not in game_dates:
            continue

        team_poss, team_pts = _parse_team_stats(gt_stats[i])
        opp_poss, opp_pts = _parse_team_stats(gt_opp_stats[i])

        if team_poss is None or team_pts is None or team_poss <= 0:
            continue
        if opp_poss is None or opp_pts is None:
            opp_poss = team_poss
            opp_pts = 0.0

        dt_str = game_dates[gid]
        is_neutral = game_neutral.get(gid, False)
        is_home = game_home.get(gid, 0) == tid
        opp_id = game_away.get(gid, 0) if is_home else game_home.get(gid, 0)

        obs = GameObs(
            game_id=gid,
            team_id=tid,
            opp_id=opp_id,
            team_pts=float(team_pts),
            team_poss=float(team_poss),
            opp_pts=float(opp_pts),
            opp_poss=float(opp_poss),
            is_home=is_home,
            is_neutral=is_neutral,
            game_date=dt_str,
            weight=0.0,
        )
        games_by_date.setdefault(dt_str, []).append(obs)

    return games_by_date


def _load_pbp_no_garbage_games(
    s3: S3IO, cfg: Config, season: int, d1_ids: Set[int]
) -> Dict[str, List[GameObs]]:
    """Read fct_pbp_game_teams_flat_garbage_removed + fct_games for neutralSite.

    Only includes D1-vs-D1 games. Uses ``team_possessions_formula`` (statistical
    formula: FGA - OREB + TOV + 0.44*FTA) instead of ``team_possessions``
    (event-counted) to match box-score possession methodology.
    """
    # Read fct_games for neutral site + D1 filtering
    fct_games = dedup_by(
        read_silver_table(s3, cfg, "fct_games", season=season), ["gameId"]
    )
    game_neutral: Dict[int, bool] = {}
    d1_game_ids: Set[int] = set()

    if fct_games.num_rows > 0:
        g_ids = pydict_get(fct_games, "gameId")
        g_neutral = pydict_get(fct_games, "neutralSite")
        g_home = pydict_get(fct_games, "homeTeamId")
        g_away = pydict_get(fct_games, "awayTeamId")
        for i, gid in enumerate(g_ids):
            if gid is None:
                continue
            gid_int = int(gid)
            home_id = int(g_home[i]) if g_home[i] is not None else 0
            away_id = int(g_away[i]) if g_away[i] is not None else 0
            if home_id in d1_ids and away_id in d1_ids:
                d1_game_ids.add(gid_int)
                game_neutral[gid_int] = bool(g_neutral[i]) if g_neutral[i] is not None else False

    # Read PBP garbage-removed flat table
    pbp = dedup_by(
        read_silver_table(
            s3, cfg, "fct_pbp_game_teams_flat_garbage_removed", season=season
        ),
        ["gameid", "teamid"],
    )
    if pbp.num_rows == 0:
        return {}

    p_gids = pydict_get(pbp, "gameid")
    p_tids = pydict_get(pbp, "teamid")
    p_opp = pydict_get(pbp, "opponentid")
    p_dates = pydict_get(pbp, "startdate")
    p_home = pydict_get(pbp, "ishometeam")
    p_tpts = pydict_get(pbp, "team_points_total")
    p_opts = pydict_get(pbp, "opp_points_total")

    # Use formula-based possessions (FGA - OREB + TOV + 0.44*FTA) to match
    # box-score methodology. Fall back to event-counted if formula unavailable.
    has_formula = "team_possessions_formula" in pbp.column_names
    if has_formula:
        p_tposs = pydict_get(pbp, "team_possessions_formula")
        p_oposs = pydict_get(pbp, "opp_possessions_formula")
        logger.info("pbp_no_garbage: using team_possessions_formula")
    else:
        p_tposs = pydict_get(pbp, "team_possessions")
        p_oposs = pydict_get(pbp, "opp_possessions")
        logger.warning("pbp_no_garbage: team_possessions_formula not found, falling back to event count")

    games_by_date: Dict[str, List[GameObs]] = {}

    for i in range(len(p_gids)):
        gid = p_gids[i]
        tid = p_tids[i]
        if gid is None or tid is None:
            continue
        gid = int(gid)
        tid = int(tid)

        # D1 filter
        if gid not in d1_game_ids:
            continue

        dt_str = _parse_date_str(p_dates[i])
        if dt_str is None:
            continue

        team_poss = p_tposs[i]
        team_pts = p_tpts[i]
        opp_poss = p_oposs[i]
        opp_pts = p_opts[i]

        if team_poss is None or team_poss <= 0 or team_pts is None:
            continue
        if opp_poss is None or opp_poss <= 0:
            opp_poss = team_poss
        if opp_pts is None:
            opp_pts = 0.0

        opp_id = int(p_opp[i]) if p_opp[i] is not None else 0
        is_home = bool(p_home[i]) if p_home[i] is not None else False
        is_neutral = game_neutral.get(gid, False)

        obs = GameObs(
            game_id=gid,
            team_id=tid,
            opp_id=opp_id,
            team_pts=float(team_pts),
            team_poss=float(team_poss),
            opp_pts=float(opp_pts),
            opp_poss=float(opp_poss),
            is_home=is_home,
            is_neutral=is_neutral,
            game_date=dt_str,
            weight=0.0,
        )
        games_by_date.setdefault(dt_str, []).append(obs)

    return games_by_date


# ---------------------------------------------------------------------------
# Per-date rating loop
# ---------------------------------------------------------------------------


def _run_per_date_ratings(
    games_by_date: Dict[str, List[GameObs]],
    team_info: Dict[int, Dict[str, Optional[str]]],
    season: int,
    half_life: Optional[float] = None,
    hca_oe: float = 1.4,
    hca_de: float = 1.4,
    barthag_exp: float = 11.5,
    preseason_prior: Optional[Dict[int, Tuple[float, float]]] = None,
    sos_exponent: float = 1.0,
    shrinkage: float = 0.0,
) -> List[Dict[str, Any]]:
    """For each unique game date, run iterative solver with recency weighting.

    Warm-starts from the previous date's solution to speed convergence.
    If ``preseason_prior`` is provided, uses it as the initial warm-start
    for the first date instead of starting from raw averages.

    Returns a flat list of per-team-per-date records.
    """
    sorted_dates = sorted(games_by_date.keys())
    if not sorted_dates:
        return []

    # Use preseason prior as warm-start for the first date
    prior: Optional[Dict[int, Tuple[float, float]]] = preseason_prior
    records: List[Dict[str, Any]] = []
    max_iters_seen = 0
    total_iters = 0

    for rating_date in sorted_dates:
        rd = _parse_date_obj(rating_date)
        if rd is None:
            continue

        # Collect all games on or before rating_date, apply recency weighting
        all_games: List[GameObs] = []
        for dt_str, day_games in games_by_date.items():
            gd = _parse_date_obj(dt_str)
            if gd is None or gd > rd:
                continue
            days_ago = (rd - gd).days
            w = exponential_decay_weight(days_ago, half_life=half_life) if half_life else 1.0
            for g in day_games:
                all_games.append(GameObs(
                    game_id=g.game_id,
                    team_id=g.team_id,
                    opp_id=g.opp_id,
                    team_pts=g.team_pts,
                    team_poss=g.team_poss,
                    opp_pts=g.opp_pts,
                    opp_poss=g.opp_poss,
                    is_home=g.is_home,
                    is_neutral=g.is_neutral,
                    game_date=g.game_date,
                    weight=w,
                ))

        if not all_games:
            continue

        result = solve_ratings(
            all_games, prior=prior, hca_oe=hca_oe, hca_de=hca_de,
            sos_exponent=sos_exponent, shrinkage=shrinkage,
        )
        if not result:
            continue

        # Track convergence stats
        sample_tid = next(iter(result))
        iters = result[sample_tid].get("iterations", 0)
        total_iters += iters
        max_iters_seen = max(max_iters_seen, iters)

        # Update warm-start prior for next date
        prior = {
            tid: (vals["adj_oe"], vals["adj_de"])
            for tid, vals in result.items()
        }

        # Emit records for teams that have played games
        for tid, vals in result.items():
            if vals["games_played"] == 0:
                continue
            info = team_info.get(tid, {})
            adj_oe = vals["adj_oe"]
            adj_de = vals["adj_de"]
            records.append({
                "teamId": tid,
                "season": season,
                "rating_date": rating_date,
                "team": info.get("school"),
                "conference": info.get("conference"),
                "adj_oe": round(adj_oe, 4),
                "adj_de": round(adj_de, 4),
                "adj_tempo": round(vals["adj_tempo"], 4),
                "barthag": round(compute_barthag(adj_oe, adj_de, exp=barthag_exp), 6),
                "adj_margin": round(adj_oe - adj_de, 4),
                "games_played": vals["games_played"],
                "raw_oe": round(vals["raw_oe"], 4),
                "raw_de": round(vals["raw_de"], 4),
                "sos_oe": round(vals["sos_oe"], 4),
                "sos_de": round(vals["sos_de"], 4),
            })

    num_dates = len(sorted_dates)
    avg_iters = total_iters / num_dates if num_dates > 0 else 0
    logger.info(
        "ratings_convergence dates=%d max_iters=%d avg_iters=%.1f",
        num_dates, max_iters_seen, avg_iters,
    )

    return records


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _load_team_info(
    s3: S3IO, cfg: Config
) -> Dict[int, Dict[str, Optional[str]]]:
    """Load dim_teams into a {teamId: {school, conference}} lookup."""
    dim = read_silver_table(s3, cfg, "dim_teams")
    result: Dict[int, Dict[str, Optional[str]]] = {}
    if dim.num_rows == 0:
        return result
    tids = pydict_get(dim, "teamId")
    schools = pydict_get(dim, "school")
    confs = pydict_get(dim, "conference")
    for i, tid in enumerate(tids):
        if tid is not None:
            result[int(tid)] = {"school": schools[i], "conference": confs[i]}
    return result


def _parse_team_stats(stats_str: Any) -> Tuple[Optional[float], Optional[float]]:
    """Parse fct_game_teams.teamStats dict string, extract (possessions, points_total).

    The teamStats field is a Python dict string like:
    ``{"possessions": 68, "points": {"total": 75}, ...}``

    Returns (possessions, points) or (None, None) on failure.
    """
    if stats_str is None:
        return None, None
    try:
        if isinstance(stats_str, str):
            d = ast.literal_eval(stats_str)
        elif isinstance(stats_str, dict):
            d = stats_str
        else:
            return None, None

        poss = d.get("possessions")
        pts_obj = d.get("points")
        if isinstance(pts_obj, dict):
            pts = pts_obj.get("total")
        elif isinstance(pts_obj, (int, float)):
            pts = pts_obj
        else:
            pts = None

        if poss is not None:
            poss = float(poss)
        if pts is not None:
            pts = float(pts)
        return poss, pts
    except Exception:
        return None, None


def _parse_date_str(val: Any) -> Optional[str]:
    """Extract YYYY-MM-DD from a date/datetime string."""
    if val is None:
        return None
    s = str(val).strip()
    if not s:
        return None
    return s[:10]


def _parse_date_obj(date_str: str) -> Optional[date]:
    """Parse YYYY-MM-DD string to a date object."""
    try:
        return datetime.strptime(date_str, "%Y-%m-%d").date()
    except (ValueError, TypeError):
        return None
