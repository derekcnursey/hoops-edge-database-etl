from __future__ import annotations

import argparse
from collections import defaultdict
from dataclasses import dataclass
import io
import re
from typing import Dict, Iterable, List, Optional, Tuple

import pyarrow as pa
import pyarrow.parquet as pq

from cbbd_etl.config import load_config
from cbbd_etl.glue_catalog import GlueCatalog
from cbbd_etl.normalize import normalize_records
from cbbd_etl.pbp_stats import PlayClassifier, PlayFlags, PlayTypePatterns
from cbbd_etl.s3_io import S3IO, make_part_key, new_run_id


DATE_RE = re.compile(r"season=(\\d+)/date=([0-9]{4}-[0-9]{2}-[0-9]{2})/")


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


def _group_keys_by_date(keys: Iterable[str]) -> Dict[str, List[str]]:
    grouped: Dict[str, List[str]] = defaultdict(list)
    for key in keys:
        match = DATE_RE.search(key)
        if not match:
            grouped["__all__"].append(key)
            continue
        date = match.group(2)
        grouped[date].append(key)
    return grouped


@dataclass
class PlayRow:
    id: int
    game_id: int
    team_id: Optional[int]
    opponent_id: Optional[int]
    period: Optional[int]
    seconds_remaining: Optional[int]
    play_type: Optional[str]
    play_text: Optional[str]
    scoring_play: Optional[bool]
    shooting_play: Optional[bool]
    score_value: Optional[float]
    game_start_date: Optional[str]
    is_home_team: Optional[bool]
    home_score: Optional[int]
    away_score: Optional[int]
    shot_shooter_id: Optional[int]
    shot_shooter_name: Optional[str]
    shot_made: Optional[bool]
    shot_range: Optional[str]
    shot_assisted: Optional[bool]
    shot_assisted_by_id: Optional[int]
    shot_assisted_by_name: Optional[str]
    shot_loc_x: Optional[float]
    shot_loc_y: Optional[float]


def _sort_key(row: PlayRow) -> Tuple[int, int, int]:
    period = row.period or 0
    seconds = row.seconds_remaining if row.seconds_remaining is not None else -1
    # Period ascending, clock descending within period.
    return (period, -seconds, row.id)


def _build_enriched(
    plays: List[PlayRow],
    classifier: PlayClassifier,
) -> List[Dict[str, object]]:
    plays.sort(key=_sort_key)
    flags_by_idx: List[PlayFlags] = []
    is_ft_play: List[bool] = []
    is_foul_play: List[bool] = []
    for row in plays:
        flags = classifier.classify(
            row.play_type,
            row.play_text,
            row.scoring_play,
            row.shooting_play,
            row.score_value,
        )
        flags_by_idx.append(flags)
        is_ft = False
        if row.score_value is not None:
            try:
                is_ft = float(row.score_value) == 1.0
            except (TypeError, ValueError):
                is_ft = False
        if not is_ft and row.shot_range:
            is_ft = str(row.shot_range).lower() == "free_throw"
        if not is_ft and row.play_type:
            is_ft = "freethrow" in str(row.play_type).lower()
        is_ft_play.append(is_ft)
        is_foul_play.append("foul" in str(row.play_type or "").lower())

    def is_action_start(idx: int) -> bool:
        f = flags_by_idx[idx]
        return f.is_fga or f.is_turnover or f.is_def_rebound or f.is_off_rebound or f.is_period_end

    # Identify made-shot + foul sequences and last-FT indices.
    shot_with_and1 = set()
    last_ft_indices = set()
    for i, row in enumerate(plays):
        f = flags_by_idx[i]
        if not f.is_fga:
            continue
        # Look ahead until next action play.
        foul_idx = None
        for j in range(i + 1, len(plays)):
            if is_action_start(j):
                break
            if is_foul_play[j] and plays[j].seconds_remaining == row.seconds_remaining:
                foul_idx = j
                break
        if foul_idx is None:
            continue
        # Determine expected FT count based on shot result + value.
        expected = 1 if row.scoring_play else 2
        if not row.scoring_play and row.score_value == 3.0:
            expected = 3
        # Count FT plays after foul until next action play.
        ft_indices: List[int] = []
        for k in range(foul_idx + 1, len(plays)):
            if is_action_start(k):
                break
            if is_ft_play[k]:
                ft_indices.append(k)
                if len(ft_indices) >= expected:
                    break
        if ft_indices:
            shot_with_and1.add(i)
            last_ft_indices.add(ft_indices[-1])

    out: List[Dict[str, object]] = []
    current_possession_id = 0
    current_offense: Optional[int] = None
    pending_offense: Optional[int] = None
    next_new_possession = True
    last_period: Optional[int] = None
    last_index: Optional[int] = None

    for idx, row in enumerate(plays):
        if last_period is not None and row.period is not None and row.period != last_period:
            if last_index is not None:
                out[last_index]["possession_end"] = True
            next_new_possession = True
            current_offense = None
            pending_offense = None

        flags = flags_by_idx[idx]
        is_action_play = (
            flags.is_fga
            or flags.is_turnover
            or flags.is_def_rebound
            or flags.is_off_rebound
        )
        if current_possession_id == 0 and is_ft_play[idx]:
            is_action_play = True

        if next_new_possession and is_action_play:
            current_possession_id += 1
            if pending_offense is not None:
                current_offense = pending_offense
            elif row.team_id is not None:
                current_offense = row.team_id
            pending_offense = None
            next_new_possession = False

        offense_team_id = current_offense if current_offense is not None else row.team_id
        defense_team_id = row.opponent_id
        possession_end = flags.ends_possession
        if flags.is_fga and idx in shot_with_and1:
            possession_end = False
        if is_ft_play[idx]:
            possession_end = idx in last_ft_indices and bool(row.scoring_play)

        garbage_time = False
        if row.period is not None and row.seconds_remaining is not None and row.period >= 2:
            if row.home_score is not None and row.away_score is not None:
                margin = abs(row.home_score - row.away_score)
                if (margin >= 20 and row.seconds_remaining <= 600) or (margin >= 15 and row.seconds_remaining <= 300):
                    garbage_time = True

        rec = {
            "id": row.id,
            "gameId": row.game_id,
            "teamId": row.team_id,
            "opponentId": row.opponent_id,
            "period": row.period,
            "secondsRemaining": row.seconds_remaining,
            "playType": row.play_type,
            "playText": row.play_text,
            "scoringPlay": row.scoring_play,
            "shootingPlay": row.shooting_play,
            "scoreValue": row.score_value,
            "gameStartDate": row.game_start_date,
            "homeScore": row.home_score,
            "awayScore": row.away_score,
            "isHomeTeam": row.is_home_team,
            "possession_id": current_possession_id,
            "offense_team_id": offense_team_id,
            "defense_team_id": defense_team_id,
            "possession_end": possession_end,
            "garbage_time": garbage_time,
            "shot_shooter_id": row.shot_shooter_id,
            "shot_shooter_name": row.shot_shooter_name,
            "shot_made": row.shot_made,
            "shot_range": row.shot_range,
            "shot_assisted": row.shot_assisted,
            "shot_assisted_by_id": row.shot_assisted_by_id,
            "shot_assisted_by_name": row.shot_assisted_by_name,
            "shot_loc_x": row.shot_loc_x,
            "shot_loc_y": row.shot_loc_y,
        }
        out.append(rec)
        last_index = len(out) - 1

        if possession_end:
            if flags.is_def_rebound and row.team_id is not None:
                pending_offense = row.team_id
            elif row.opponent_id is not None:
                pending_offense = row.opponent_id
            else:
                pending_offense = None
            next_new_possession = True

        last_period = row.period

    return out


def main() -> None:
    parser = argparse.ArgumentParser(description="Build enriched play-by-play table with possession context.")
    parser.add_argument("--season", type=int, default=2026)
    parser.add_argument("--playtype-map", default="docs/pbp_playtype_patterns.yaml")
    parser.add_argument("--limit-dates", type=int, default=0)
    parser.add_argument("--limit-keys", type=int, default=0)
    parser.add_argument("--log-every", type=int, default=10)
    parser.add_argument("--purge", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    cfg = load_config()
    s3 = S3IO(cfg.bucket, cfg.region)
    silver_prefix = cfg.s3_layout["silver_prefix"].strip("/")
    plays_prefix = f"{silver_prefix}/fct_plays/season={args.season}/"
    enriched_prefix = f"{silver_prefix}/fct_pbp_plays_enriched/season={args.season}/"

    keys = [k for k in s3.list_keys(plays_prefix) if k.endswith(".parquet")]
    if args.limit_keys > 0:
        keys = keys[: args.limit_keys]

    grouped = _group_keys_by_date(keys)
    dates = sorted(grouped.keys())
    if args.limit_dates > 0:
        dates = dates[: args.limit_dates]

    if not dates:
        raise SystemExit(f"No parquet files found under {plays_prefix}")

    if args.purge and not args.dry_run:
        existing = s3.list_keys(enriched_prefix)
        if existing:
            print(f"[pbp] purging {len(existing)} keys under {enriched_prefix}")
            s3.delete_keys(existing)

    patterns = PlayTypePatterns.from_yaml(args.playtype_map)
    classifier = PlayClassifier(patterns)

    desired_cols = [
        "id",
        "gameId",
        "teamId",
        "opponentId",
        "period",
        "secondsRemaining",
        "playType",
        "playText",
        "scoringPlay",
        "shootingPlay",
        "scoreValue",
        "gameStartDate",
        "isHomeTeam",
        "homeScore",
        "awayScore",
        "shot_shooter_id",
        "shot_shooter_name",
        "shot_made",
        "shot_range",
        "shot_assisted",
        "shot_assisted_by_id",
        "shot_assisted_by_name",
        "shot_loc_x",
        "shot_loc_y",
    ]

    run_id = new_run_id()
    glue = GlueCatalog(cfg.region)
    glue.ensure_database("cbbd_silver")
    location = f"s3://{cfg.bucket}/{silver_prefix}/fct_pbp_plays_enriched/"

    plays_by_game: Dict[int, List[PlayRow]] = defaultdict(list)
    total_keys = sum(len(grouped[date]) for date in dates)
    key_index = 0
    for date in dates:
        for key in grouped[date]:
            key_index += 1
            if args.log_every > 0 and (key_index == 1 or key_index % args.log_every == 0 or key_index == total_keys):
                print(f"[pbp] {key_index}/{total_keys} reading {key}")
            table = _load_table(s3, key, desired_cols)
            ids = _get_col(table, "id")
            game_ids = _get_col(table, "gameId")
            team_ids = _get_col(table, "teamId")
            opponent_ids = _get_col(table, "opponentId")
            periods = _get_col(table, "period")
            seconds_remaining = _get_col(table, "secondsRemaining")
            play_types = _get_col(table, "playType")
            play_texts = _get_col(table, "playText")
            scoring_plays = _get_col(table, "scoringPlay")
            shooting_plays = _get_col(table, "shootingPlay")
            score_values = _get_col(table, "scoreValue")
            game_start_dates = _get_col(table, "gameStartDate")
            is_home_team = _get_col(table, "isHomeTeam")
            home_scores = _get_col(table, "homeScore")
            away_scores = _get_col(table, "awayScore")
            shot_shooter_id = _get_col(table, "shot_shooter_id")
            shot_shooter_name = _get_col(table, "shot_shooter_name")
            shot_made = _get_col(table, "shot_made")
            shot_range = _get_col(table, "shot_range")
            shot_assisted = _get_col(table, "shot_assisted")
            shot_assisted_by_id = _get_col(table, "shot_assisted_by_id")
            shot_assisted_by_name = _get_col(table, "shot_assisted_by_name")
            shot_loc_x = _get_col(table, "shot_loc_x")
            shot_loc_y = _get_col(table, "shot_loc_y")

            for i in range(table.num_rows):
                play_id = _to_int(ids[i])
                game_id = _to_int(game_ids[i])
                if play_id is None or game_id is None:
                    continue
                row = PlayRow(
                    id=play_id,
                    game_id=game_id,
                    team_id=_to_int(team_ids[i]),
                    opponent_id=_to_int(opponent_ids[i]),
                    period=_to_int(periods[i]),
                    seconds_remaining=_to_int(seconds_remaining[i]),
                    play_type=play_types[i],
                    play_text=play_texts[i],
                    scoring_play=bool(scoring_plays[i]) if scoring_plays[i] is not None else None,
                    shooting_play=bool(shooting_plays[i]) if shooting_plays[i] is not None else None,
                    score_value=_to_float(score_values[i]),
                    game_start_date=game_start_dates[i],
                    is_home_team=bool(is_home_team[i]) if is_home_team[i] is not None else None,
                    home_score=_to_int(home_scores[i]),
                    away_score=_to_int(away_scores[i]),
                    shot_shooter_id=_to_int(shot_shooter_id[i]),
                    shot_shooter_name=shot_shooter_name[i],
                    shot_made=bool(shot_made[i]) if shot_made[i] is not None else None,
                    shot_range=shot_range[i],
                    shot_assisted=bool(shot_assisted[i]) if shot_assisted[i] is not None else None,
                    shot_assisted_by_id=_to_int(shot_assisted_by_id[i]),
                    shot_assisted_by_name=shot_assisted_by_name[i],
                    shot_loc_x=_to_float(shot_loc_x[i]),
                    shot_loc_y=_to_float(shot_loc_y[i]),
                )
                plays_by_game[game_id].append(row)

    records: List[Dict[str, object]] = []
    for game_id, plays in plays_by_game.items():
        records.extend(_build_enriched(plays, classifier))

    if not records:
        raise SystemExit("No records produced; check source data and playtype mapping.")

    by_date: Dict[str, List[Dict[str, object]]] = defaultdict(list)
    for rec in records:
        rec_date = _date_from_ts(rec.get("gameStartDate"))
        if not rec_date:
            continue
        by_date[str(rec_date)].append(rec)

    if args.dry_run:
        for date, rows in sorted(by_date.items()):
            print(f"[pbp] {date} produced {len(rows)} rows")
        return

    for date, rows in sorted(by_date.items()):
        table = normalize_records("fct_pbp_plays_enriched", rows)
        part = f"season={args.season}/date={date}"
        key = make_part_key(
            silver_prefix,
            "fct_pbp_plays_enriched",
            part,
            f"part-{run_id[:8]}.parquet",
        )
        s3.put_parquet(key, table)

    if not args.dry_run:
        if records:
            sample = normalize_records("fct_pbp_plays_enriched", records[:1])
            glue.ensure_table(
                database="cbbd_silver",
                name="fct_pbp_plays_enriched",
                location=location,
                schema=sample.schema,
                partition_keys=["season", "date"],
            )


if __name__ == "__main__":
    main()
