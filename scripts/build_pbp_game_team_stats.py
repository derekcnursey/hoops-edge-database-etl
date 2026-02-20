from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from dataclasses import dataclass, field
import io
import math
from typing import Dict, Iterable, List, Optional, Tuple

import pyarrow as pa
import pyarrow.parquet as pq

from cbbd_etl.config import load_config
from cbbd_etl.glue_catalog import GlueCatalog
from cbbd_etl.normalize import normalize_records
from cbbd_etl.pbp_stats import PlayClassifier, PlayTypePatterns
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


@dataclass
class Agg:
    points: int = 0
    fga: int = 0
    fg2a: int = 0
    fg2m: int = 0
    fg3a: int = 0
    fg3m: int = 0
    fta: int = 0
    ftm: int = 0
    oreb: int = 0
    turnovers: int = 0
    possessions_ends: int = 0
    max_period: int = 0
    pts_2: int = 0
    pts_3: int = 0
    pts_ft: int = 0
    opponent_counts: Counter = field(default_factory=Counter)
    game_date: Optional[str] = None


def _iter_play_keys(s3: S3IO, prefix: str) -> List[str]:
    keys = [k for k in s3.list_keys(prefix) if k.endswith(".parquet")]
    return sorted(keys)


def _infer_opponent_id(agg: Agg, game_id: int, team_id: int, game_teams: Dict[int, List[int]]) -> Optional[int]:
    if agg.opponent_counts:
        return agg.opponent_counts.most_common(1)[0][0]
    teams = game_teams.get(game_id, [])
    for other in teams:
        if other != team_id:
            return other
    return None


def _compute_game_minutes(max_period: int, regulation_periods: int, regulation_minutes: int, ot_minutes: int) -> float:
    if max_period < regulation_periods:
        max_period = regulation_periods
    ot_periods = max(0, max_period - regulation_periods)
    return float(regulation_minutes + ot_periods * ot_minutes)


def _safe_div(num: float, denom: float) -> float:
    if denom <= 0:
        return 0.0
    return num / denom


def _list_playtypes(s3: S3IO, keys: List[str]) -> Counter:
    counts: Counter = Counter()
    for key in keys:
        table = _load_table(s3, key, ["playType"])
        play_types = _get_col(table, "playType")
        counts.update(pt for pt in play_types if pt)
    return counts


def main() -> None:
    parser = argparse.ArgumentParser(description="Build game-team advanced stats from fct_plays.")
    parser.add_argument("--season", type=int, default=2026)
    parser.add_argument("--playtype-map", default="docs/pbp_playtype_patterns.yaml")
    parser.add_argument("--limit-keys", type=int, default=0)
    parser.add_argument("--list-playtypes", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--log-every", type=int, default=25)
    parser.add_argument("--regulation-periods", type=int, default=2)
    parser.add_argument("--regulation-minutes", type=int, default=40)
    parser.add_argument("--ot-minutes", type=int, default=5)
    args = parser.parse_args()

    cfg = load_config()
    s3 = S3IO(cfg.bucket, cfg.region)
    silver_prefix = cfg.s3_layout["silver_prefix"].strip("/")
    plays_prefix = f"{silver_prefix}/fct_plays/season={args.season}/"

    keys = _iter_play_keys(s3, plays_prefix)
    if args.limit_keys > 0:
        keys = keys[: args.limit_keys]

    if not keys:
        raise SystemExit(f"No parquet files found under {plays_prefix}")

    if args.list_playtypes:
        counts = _list_playtypes(s3, keys)
        for play_type, count in counts.most_common():
            print(f"{count}\t{play_type}")
        return

    patterns = PlayTypePatterns.from_yaml(args.playtype_map)
    classifier = PlayClassifier(patterns)

    aggs: Dict[Tuple[int, int], Agg] = {}
    game_max_period: Dict[int, int] = defaultdict(int)
    game_teams: Dict[int, List[int]] = defaultdict(list)

    desired_cols = [
        "gameId",
        "teamId",
        "opponentId",
        "playType",
        "playText",
        "scoringPlay",
        "scoreValue",
        "period",
        "gameStartDate",
    ]

    total_keys = len(keys)
    for idx_key, key in enumerate(keys, start=1):
        if args.log_every > 0 and (idx_key == 1 or idx_key % args.log_every == 0 or idx_key == total_keys):
            print(f"[pbp] {idx_key}/{total_keys} reading {key}")
        table = _load_table(s3, key, desired_cols)
        game_ids = _get_col(table, "gameId")
        team_ids = _get_col(table, "teamId")
        opponent_ids = _get_col(table, "opponentId")
        play_types = _get_col(table, "playType")
        play_texts = _get_col(table, "playText")
        scoring_plays = _get_col(table, "scoringPlay")
        score_values = _get_col(table, "scoreValue")
        periods = _get_col(table, "period")
        game_start_dates = _get_col(table, "gameStartDate")

        for idx in range(table.num_rows):
            game_id = _to_int(game_ids[idx])
            team_id = _to_int(team_ids[idx])
            if game_id is None or team_id is None:
                continue

            agg_key = (game_id, team_id)
            agg = aggs.get(agg_key)
            if agg is None:
                agg = Agg()
                aggs[agg_key] = agg

            if team_id not in game_teams[game_id]:
                game_teams[game_id].append(team_id)

            opponent_id = _to_int(opponent_ids[idx])
            if opponent_id is not None:
                agg.opponent_counts[opponent_id] += 1

            period = _to_int(periods[idx])
            if period is not None:
                if period > agg.max_period:
                    agg.max_period = period
                if period > game_max_period[game_id]:
                    game_max_period[game_id] = period

            if agg.game_date is None:
                agg.game_date = _date_from_ts(game_start_dates[idx])

            flags = classifier.classify(
                play_types[idx],
                play_texts[idx],
                scoring_plays[idx],
                None,
                score_values[idx],
            )

            if flags.is_fga:
                agg.fga += 1
            if flags.is_fta:
                agg.fta += 1
            if flags.is_off_rebound:
                agg.oreb += 1
            if flags.is_turnover:
                agg.turnovers += 1
            if flags.ends_possession:
                end_team_id = team_id
                if flags.is_def_rebound and opponent_id is not None:
                    end_team_id = opponent_id
                end_key = (game_id, end_team_id)
                end_agg = aggs.get(end_key)
                if end_agg is None:
                    end_agg = Agg()
                    aggs[end_key] = end_agg
                    if end_team_id not in game_teams[game_id]:
                        game_teams[game_id].append(end_team_id)
                if end_agg.game_date is None:
                    end_agg.game_date = agg.game_date
                end_agg.possessions_ends += 1

            scoring = scoring_plays[idx]
            score_value = _to_float(score_values[idx])
            if scoring and score_value is not None and not math.isnan(score_value):
                agg.points += int(round(score_value))
                if flags.is_fta and score_value == 1.0:
                    agg.ftm += 1
                    agg.pts_ft += 1
                if flags.is_fga and score_value in (2.0, 3.0):
                    if score_value == 2.0:
                        agg.pts_2 += 2
                    else:
                        agg.pts_3 += 3
            if flags.is_fga and score_value in (2.0, 3.0):
                if score_value == 2.0:
                    agg.fg2a += 1
                    if scoring:
                        agg.fg2m += 1
                else:
                    agg.fg3a += 1
                    if scoring:
                        agg.fg3m += 1

    records: List[Dict[str, object]] = []
    for (game_id, team_id), agg in aggs.items():
        opponent_id = _infer_opponent_id(agg, game_id, team_id, game_teams)
        possessions_formula = agg.fga + 0.44 * agg.fta - agg.oreb + agg.turnovers
        possessions_formula = max(0.0, possessions_formula)
        possessions = float(agg.possessions_ends) if agg.possessions_ends > 0 else possessions_formula

        opp_points = 0
        opp_possessions = 0.0
        if opponent_id is not None:
            opp = aggs.get((game_id, opponent_id))
            if opp is not None:
                opp_points = opp.points
                opp_possessions_formula = opp.fga + 0.44 * opp.fta - opp.oreb + opp.turnovers
                opp_possessions_formula = max(0.0, opp_possessions_formula)
                opp_possessions = float(opp.possessions_ends) if opp.possessions_ends > 0 else opp_possessions_formula

        max_period = game_max_period.get(game_id, agg.max_period or args.regulation_periods)
        game_minutes = _compute_game_minutes(max_period, args.regulation_periods, args.regulation_minutes, args.ot_minutes)

        ppp = _safe_div(float(agg.points), possessions)
        off_eff = _safe_div(float(agg.points) * 100.0, possessions)
        def_eff = _safe_div(float(opp_points) * 100.0, opp_possessions)
        pace = 0.0
        if possessions > 0 and opp_possessions > 0 and game_minutes > 0:
            pace = ((possessions + opp_possessions) / 2.0) * (40.0 / game_minutes)

        record = {
            "gameId": game_id,
            "teamId": team_id,
            "opponentId": opponent_id,
            "season": args.season,
            "date": agg.game_date,
            "points": agg.points,
            "opp_points": opp_points,
            "possessions": possessions,
            "possessions_ends": agg.possessions_ends,
            "possessions_formula": possessions_formula,
            "fga": agg.fga,
            "fg2a": agg.fg2a,
            "fg2m": agg.fg2m,
            "fg3a": agg.fg3a,
            "fg3m": agg.fg3m,
            "fta": agg.fta,
            "ftm": agg.ftm,
            "oreb": agg.oreb,
            "turnovers": agg.turnovers,
            "ppp": ppp,
            "pace": pace,
            "off_eff": off_eff,
            "def_eff": def_eff,
            "adj_off_eff": off_eff,
            "adj_def_eff": def_eff,
            "pts_2": agg.pts_2,
            "pts_3": agg.pts_3,
            "pts_ft": agg.pts_ft,
            "game_minutes": game_minutes,
            "max_period": max_period,
        }
        if record["date"] is None:
            continue
        records.append(record)

    if not records:
        raise SystemExit("No records produced; check playtype mapping and source data.")

    if args.dry_run:
        print(f"Prepared {len(records)} records for season {args.season}.")
        return

    run_id = new_run_id()
    grouped: Dict[str, List[Dict[str, object]]] = defaultdict(list)
    for rec in records:
        grouped[str(rec["date"])].append(rec)

    glue = GlueCatalog(cfg.region)
    glue.ensure_database("cbbd_silver")

    for date, group in grouped.items():
        table = normalize_records("fct_pbp_game_team_stats", group)
        part = f"season={args.season}/date={date}"
        key = make_part_key(
            silver_prefix,
            "fct_pbp_game_team_stats",
            part,
            f"part-{run_id[:8]}.parquet",
        )
        s3.put_parquet(key, table)

    first_table = normalize_records("fct_pbp_game_team_stats", records[:1])
    location = f"s3://{cfg.bucket}/{silver_prefix}/fct_pbp_game_team_stats/"
    glue.ensure_table(
        database="cbbd_silver",
        name="fct_pbp_game_team_stats",
        location=location,
        schema=first_table.schema,
        partition_keys=["season", "date"],
    )


if __name__ == "__main__":
    main()
            if flags.is_fta and score_value == 1.0:
                agg.fta += 1
