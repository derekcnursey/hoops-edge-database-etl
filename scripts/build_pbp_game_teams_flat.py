from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from dataclasses import dataclass, field
import io
from typing import Dict, Iterable, List, Optional, Tuple

import pyarrow as pa
import pyarrow.parquet as pq

from cbbd_etl.config import load_config
from cbbd_etl.glue_catalog import GlueCatalog
from cbbd_etl.normalize import normalize_records
from cbbd_etl.s3_io import S3IO, make_part_key, new_run_id


def _to_int(value: object) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _to_float(value: object) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _date_from_ts(value: object) -> Optional[str]:
    if not value:
        return None
    if isinstance(value, str):
        return value[:10]
    date_attr = getattr(value, "date", None)
    if callable(date_attr):
        return date_attr().isoformat()
    return None


def _load_table(s3: S3IO, key: str, columns: Iterable[str]) -> pa.Table:
    raw = s3.get_object_bytes(key)
    pf = pq.ParquetFile(io.BytesIO(raw))
    available = set(pf.schema_arrow.names)
    use_cols = [c for c in columns if c in available]
    return pf.read(columns=use_cols)


def _get_col(table: pa.Table, name: str) -> List[object]:
    if name in table.column_names:
        return table.column(name).to_pylist()
    return [None] * table.num_rows


def _safe_div(num: float, denom: float) -> float:
    if denom <= 0:
        return 0.0
    return num / denom


def _compute_game_minutes(max_period: int, regulation_periods: int, regulation_minutes: int, ot_minutes: int) -> float:
    if max_period < regulation_periods:
        max_period = regulation_periods
    ot_periods = max(0, max_period - regulation_periods)
    return float(regulation_minutes + ot_periods * ot_minutes)


@dataclass
class Agg:
    points: float = 0.0
    fg2m: float = 0.0
    fg2a: float = 0.0
    fg3m: float = 0.0
    fg3a: float = 0.0
    ftm: float = 0.0
    fta: float = 0.0
    tov: float = 0.0
    reb_off: float = 0.0
    reb_def: float = 0.0
    assists: float = 0.0
    steals: float = 0.0
    blocks: float = 0.0
    possessions: float = 0.0
    max_period: int = 0
    game_date: Optional[str] = None
    opponent_counts: Counter = field(default_factory=Counter)
    is_home_team: Optional[bool] = None


def main() -> None:
    parser = argparse.ArgumentParser(description="Build game-team flat stats from fct_pbp_plays_enriched.")
    parser.add_argument("--season", type=int, default=2026)
    parser.add_argument("--limit-keys", type=int, default=0)
    parser.add_argument("--log-every", type=int, default=25)
    parser.add_argument("--purge", action="store_true")
    parser.add_argument("--exclude-garbage-time", action="store_true")
    parser.add_argument("--output-table", default="fct_pbp_game_teams_flat")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--regulation-periods", type=int, default=2)
    parser.add_argument("--regulation-minutes", type=int, default=40)
    parser.add_argument("--ot-minutes", type=int, default=5)
    args = parser.parse_args()

    cfg = load_config()
    s3 = S3IO(cfg.bucket, cfg.region)
    silver_prefix = cfg.s3_layout["silver_prefix"].strip("/")
    enriched_prefix = f"{silver_prefix}/fct_pbp_plays_enriched/season={args.season}/"
    out_prefix = f"{silver_prefix}/{args.output_table}/season={args.season}/"

    keys = [k for k in s3.list_keys(enriched_prefix) if k.endswith(".parquet")]
    if args.limit_keys > 0:
        keys = keys[: args.limit_keys]
    if not keys:
        raise SystemExit(f"No parquet files found under {enriched_prefix}")

    if args.purge and not args.dry_run:
        existing = s3.list_keys(out_prefix)
        if existing:
            print(f"[pbp] purging {len(existing)} keys under {out_prefix}")
            s3.delete_keys(existing)

    desired_cols = [
        "gameId",
        "teamId",
        "opponentId",
        "period",
        "secondsRemaining",
        "playType",
        "scoringPlay",
        "shootingPlay",
        "scoreValue",
        "gameStartDate",
        "possession_end",
        "offense_team_id",
        "shot_assisted",
        "garbage_time",
        "isHomeTeam",
    ]

    aggs: Dict[Tuple[int, int], Agg] = {}
    game_max_period: Dict[int, int] = defaultdict(int)
    game_teams: Dict[int, List[int]] = defaultdict(list)
    garbage_flags: Dict[Tuple[int, int, int], bool] = {}

    total_keys = len(keys)
    for idx_key, key in enumerate(keys, start=1):
        if args.log_every > 0 and (idx_key == 1 or idx_key % args.log_every == 0 or idx_key == total_keys):
            print(f"[pbp] {idx_key}/{total_keys} reading {key}")
        table = _load_table(s3, key, desired_cols)
        game_ids = _get_col(table, "gameId")
        team_ids = _get_col(table, "teamId")
        opponent_ids = _get_col(table, "opponentId")
        periods = _get_col(table, "period")
        seconds_remaining = _get_col(table, "secondsRemaining")
        play_types = _get_col(table, "playType")
        scoring_plays = _get_col(table, "scoringPlay")
        shooting_plays = _get_col(table, "shootingPlay")
        score_values = _get_col(table, "scoreValue")
        game_start_dates = _get_col(table, "gameStartDate")
        possession_end = _get_col(table, "possession_end")
        offense_team_ids = _get_col(table, "offense_team_id")
        shot_assisted = _get_col(table, "shot_assisted")
        garbage_time = _get_col(table, "garbage_time")
        is_home_team = _get_col(table, "isHomeTeam")

        for i in range(table.num_rows):
            game_id = _to_int(game_ids[i])
            team_id = _to_int(team_ids[i])
            if game_id is None or team_id is None:
                continue
            period = _to_int(periods[i])
            seconds = _to_int(seconds_remaining[i])
            if period is not None and seconds is not None:
                key = (game_id, period, seconds)
                if bool(garbage_time[i]):
                    garbage_flags[key] = True
                elif key not in garbage_flags:
                    garbage_flags[key] = False
            if args.exclude_garbage_time and bool(garbage_time[i]):
                continue

            agg_key = (game_id, team_id)
            agg = aggs.get(agg_key)
            if agg is None:
                agg = Agg()
                aggs[agg_key] = agg

            if team_id not in game_teams[game_id]:
                game_teams[game_id].append(team_id)

            opponent_id = _to_int(opponent_ids[i])
            if opponent_id is not None:
                agg.opponent_counts[opponent_id] += 1

            if period is not None:
                if period > agg.max_period:
                    agg.max_period = period
                if period > game_max_period[game_id]:
                    game_max_period[game_id] = period

            if agg.game_date is None:
                agg.game_date = _date_from_ts(game_start_dates[i])
            if agg.is_home_team is None and is_home_team[i] is not None:
                agg.is_home_team = bool(is_home_team[i])

            play_type = str(play_types[i] or "").lower()
            scoring = bool(scoring_plays[i])
            shooting = bool(shooting_plays[i])
            score_value = _to_float(score_values[i])

            if shooting and score_value in (2.0, 3.0):
                if score_value == 2.0:
                    agg.fg2a += 1
                    if scoring:
                        agg.fg2m += 1
                else:
                    agg.fg3a += 1
                    if scoring:
                        agg.fg3m += 1
            if shooting and score_value == 1.0:
                agg.fta += 1
                if scoring:
                    agg.ftm += 1

            if scoring and score_value is not None:
                agg.points += float(score_value)

            if "turnover" in play_type or "offensive charge" in play_type:
                agg.tov += 1
            if "offensive rebound" in play_type:
                agg.reb_off += 1
            if "defensive rebound" in play_type:
                agg.reb_def += 1
            if "steal" in play_type:
                agg.steals += 1
            if "block" in play_type:
                agg.blocks += 1
            if scoring and bool(shot_assisted[i]):
                agg.assists += 1

            if bool(possession_end[i]):
                offense_id = _to_int(offense_team_ids[i])
                if offense_id is not None:
                    poss_key = (game_id, offense_id)
                    poss_agg = aggs.get(poss_key)
                    if poss_agg is None:
                        poss_agg = Agg()
                        aggs[poss_key] = poss_agg
                    poss_agg.possessions += 1

    records: List[Dict[str, object]] = []
    for (game_id, team_id), agg in aggs.items():
        opponent_id = None
        if agg.opponent_counts:
            opponent_id = agg.opponent_counts.most_common(1)[0][0]
        else:
            for other in game_teams.get(game_id, []):
                if other != team_id:
                    opponent_id = other
                    break

        opp = aggs.get((game_id, opponent_id)) if opponent_id is not None else None

        fg_made = agg.fg2m + agg.fg3m
        fg_att = agg.fg2a + agg.fg3a
        opp_fg_made = (opp.fg2m + opp.fg3m) if opp else 0.0
        opp_fg_att = (opp.fg2a + opp.fg3a) if opp else 0.0

        team_possessions = agg.possessions
        opp_possessions = opp.possessions if opp else 0.0
        team_possessions_formula = fg_att + 0.44 * agg.fta - agg.reb_off + agg.tov
        opp_possessions_formula = opp_fg_att + 0.44 * (opp.fta if opp else 0.0) - (opp.reb_off if opp else 0.0) + (opp.tov if opp else 0.0)

        team_points = agg.points
        opp_points = opp.points if opp else 0.0

        team_efg = _safe_div(fg_made + 0.5 * agg.fg3m, fg_att)
        team_ts = _safe_div(team_points, 2.0 * (fg_att + 0.44 * agg.fta))
        team_ft_rate = _safe_div(agg.fta, fg_att)
        team_tov_ratio = _safe_div(agg.tov, team_possessions)
        team_oreb_pct = _safe_div(agg.reb_off, agg.reb_off + (opp.reb_def if opp else 0.0))

        opp_efg = _safe_div(opp_fg_made + 0.5 * (opp.fg3m if opp else 0.0), opp_fg_att)
        opp_ts = _safe_div(opp_points, 2.0 * (opp_fg_att + 0.44 * (opp.fta if opp else 0.0)))
        opp_ft_rate = _safe_div(opp.fta if opp else 0.0, opp_fg_att)
        opp_tov_ratio = _safe_div(opp.tov if opp else 0.0, opp_possessions)
        opp_oreb_pct = _safe_div(opp.reb_off if opp else 0.0, (opp.reb_off if opp else 0.0) + agg.reb_def)

        max_period = game_max_period.get(game_id, agg.max_period or args.regulation_periods)
        game_minutes = _compute_game_minutes(max_period, args.regulation_periods, args.regulation_minutes, args.ot_minutes)
        pace = 0.0
        if team_possessions > 0 and opp_possessions > 0 and game_minutes > 0:
            pace = ((team_possessions + opp_possessions) / 2.0) * (40.0 / game_minutes)

        # Compute garbage time minutes for the game.
        garbage_seconds = 0
        period_seconds: Dict[int, Dict[int, bool]] = defaultdict(dict)
        for (gid, period, sec), is_garbage in garbage_flags.items():
            if gid != game_id:
                continue
            period_seconds[period][sec] = period_seconds[period].get(sec, False) or is_garbage
        for period, sec_map in period_seconds.items():
            secs = sorted(sec_map.keys(), reverse=True)
            for idx, sec in enumerate(secs):
                next_sec = secs[idx + 1] if idx + 1 < len(secs) else 0
                if sec_map.get(sec, False):
                    garbage_seconds += max(0, sec - next_sec)
        garbage_minutes = garbage_seconds / 60.0

        records.append(
            {
                "gameid": game_id,
                "teamid": team_id,
                "opponentid": opponent_id,
                "startdate": agg.game_date,
                "ishometeam": agg.is_home_team,
                "team_possessions": team_possessions,
                "team_possessions_formula": team_possessions_formula,
                "opp_possessions": opp_possessions,
                "opp_possessions_formula": opp_possessions_formula,
                "team_points_total": team_points,
                "opp_points_total": opp_points,
                "team_2fg_made": agg.fg2m,
                "team_2fg_att": agg.fg2a,
                "team_3fg_made": agg.fg3m,
                "team_3fg_att": agg.fg3a,
                "team_ft_made": agg.ftm,
                "team_ft_att": agg.fta,
                "team_fg_made": fg_made,
                "team_fg_att": fg_att,
                "team_tov_total": agg.tov,
                "team_reb_off": agg.reb_off,
                "team_reb_def": agg.reb_def,
                "team_assists": agg.assists,
                "team_steals": agg.steals,
                "team_blocks": agg.blocks,
                "team_efg_pct": team_efg,
                "team_true_shooting": team_ts,
                "team_ft_rate": team_ft_rate,
                "team_tov_ratio": team_tov_ratio,
                "team_oreb_pct": team_oreb_pct,
                "opp_2fg_made": opp.fg2m if opp else 0.0,
                "opp_2fg_att": opp.fg2a if opp else 0.0,
                "opp_3fg_made": opp.fg3m if opp else 0.0,
                "opp_3fg_att": opp.fg3a if opp else 0.0,
                "opp_ft_made": opp.ftm if opp else 0.0,
                "opp_ft_att": opp.fta if opp else 0.0,
                "opp_fg_made": opp_fg_made,
                "opp_fg_att": opp_fg_att,
                "opp_tov_total": opp.tov if opp else 0.0,
                "opp_reb_off": opp.reb_off if opp else 0.0,
                "opp_reb_def": opp.reb_def if opp else 0.0,
                "opp_assists": opp.assists if opp else 0.0,
                "opp_steals": opp.steals if opp else 0.0,
                "opp_blocks": opp.blocks if opp else 0.0,
                "opp_efg_pct": opp_efg,
                "opp_true_shooting": opp_ts,
                "opp_ft_rate": opp_ft_rate,
                "opp_tov_ratio": opp_tov_ratio,
                "opp_oreb_pct": opp_oreb_pct,
                "pace": pace,
                "garbage_time_minutes": garbage_minutes,
                "game_minutes": game_minutes,
                "max_period": max_period,
            }
        )

    if not records:
        raise SystemExit("No records produced; check source data.")

    # Partition by date derived from startdate.
    by_date: Dict[str, List[Dict[str, object]]] = defaultdict(list)
    for rec in records:
        date = rec.get("startdate")
        if not date:
            continue
        by_date[str(date)].append(rec)

    if args.dry_run:
        for date, rows in sorted(by_date.items()):
            print(f"[pbp] {date} produced {len(rows)} rows")
        return

    run_id = new_run_id()
    for date, rows in sorted(by_date.items()):
        table = normalize_records(args.output_table, rows)
        part = f"season={args.season}/date={date}"
        key = make_part_key(
            silver_prefix,
            args.output_table,
            part,
            f"part-{run_id[:8]}.parquet",
        )
        s3.put_parquet(key, table)

    glue = GlueCatalog(cfg.region)
    glue.ensure_database("cbbd_silver")
    location = f"s3://{cfg.bucket}/{silver_prefix}/{args.output_table}/"
    sample = normalize_records(args.output_table, records[:1])
    glue.ensure_table(
        database="cbbd_silver",
        name=args.output_table,
        location=location,
        schema=sample.schema,
        partition_keys=["season", "date"],
    )


if __name__ == "__main__":
    main()
