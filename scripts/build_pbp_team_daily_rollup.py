from __future__ import annotations

import argparse
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, timedelta
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


def _parse_date(value: object) -> Optional[date]:
    if not value:
        return None
    if isinstance(value, str):
        return date.fromisoformat(value[:10])
    date_attr = getattr(value, "date", None)
    if callable(date_attr):
        return date_attr()
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


@dataclass
class Totals:
    games_played: int = 0
    team_possessions: float = 0.0
    team_possessions_formula: float = 0.0
    opp_possessions: float = 0.0
    opp_possessions_formula: float = 0.0
    team_points_total: float = 0.0
    opp_points_total: float = 0.0
    team_2fg_made: float = 0.0
    team_2fg_att: float = 0.0
    team_3fg_made: float = 0.0
    team_3fg_att: float = 0.0
    team_ft_made: float = 0.0
    team_ft_att: float = 0.0
    team_fg_made: float = 0.0
    team_fg_att: float = 0.0
    team_tov_total: float = 0.0
    team_reb_off: float = 0.0
    team_reb_def: float = 0.0
    team_assists: float = 0.0
    team_steals: float = 0.0
    team_blocks: float = 0.0
    opp_2fg_made: float = 0.0
    opp_2fg_att: float = 0.0
    opp_3fg_made: float = 0.0
    opp_3fg_att: float = 0.0
    opp_ft_made: float = 0.0
    opp_ft_att: float = 0.0
    opp_fg_made: float = 0.0
    opp_fg_att: float = 0.0
    opp_tov_total: float = 0.0
    opp_reb_off: float = 0.0
    opp_reb_def: float = 0.0
    opp_assists: float = 0.0
    opp_steals: float = 0.0
    opp_blocks: float = 0.0
    game_minutes_total: float = 0.0

    def add(self, other: "Totals") -> None:
        for field in self.__dataclass_fields__:
            setattr(self, field, getattr(self, field) + getattr(other, field))


def main() -> None:
    parser = argparse.ArgumentParser(description="Build team daily rollups from PBP game-team flat table.")
    parser.add_argument("--season", type=int, default=2026)
    parser.add_argument("--source-table", default="fct_pbp_game_teams_flat")
    parser.add_argument("--output-table", default="fct_pbp_team_daily_rollup")
    parser.add_argument("--log-every", type=int, default=25)
    parser.add_argument("--purge", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    cfg = load_config()
    s3 = S3IO(cfg.bucket, cfg.region)
    silver_prefix = cfg.s3_layout["silver_prefix"].strip("/")
    src_prefix = f"{silver_prefix}/{args.source_table}/season={args.season}/"
    out_prefix = f"{silver_prefix}/{args.output_table}/season={args.season}/"

    keys = [k for k in s3.list_keys(src_prefix) if k.endswith(".parquet")]
    if not keys:
        raise SystemExit(f"No parquet files found under {src_prefix}")

    if args.purge and not args.dry_run:
        existing = s3.list_keys(out_prefix)
        if existing:
            print(f"[pbp] purging {len(existing)} keys under {out_prefix}")
            s3.delete_keys(existing)

    desired_cols = [
        "gameid",
        "teamid",
        "startdate",
        "team_possessions",
        "team_possessions_formula",
        "opp_possessions",
        "opp_possessions_formula",
        "team_points_total",
        "opp_points_total",
        "team_2fg_made",
        "team_2fg_att",
        "team_3fg_made",
        "team_3fg_att",
        "team_ft_made",
        "team_ft_att",
        "team_fg_made",
        "team_fg_att",
        "team_tov_total",
        "team_reb_off",
        "team_reb_def",
        "team_assists",
        "team_steals",
        "team_blocks",
        "opp_2fg_made",
        "opp_2fg_att",
        "opp_3fg_made",
        "opp_3fg_att",
        "opp_ft_made",
        "opp_ft_att",
        "opp_fg_made",
        "opp_fg_att",
        "opp_tov_total",
        "opp_reb_off",
        "opp_reb_def",
        "opp_assists",
        "opp_steals",
        "opp_blocks",
        "game_minutes",
    ]

    by_team_date: Dict[Tuple[int, date], Totals] = defaultdict(Totals)
    teams: set[int] = set()
    all_dates: set[date] = set()

    total_keys = len(keys)
    for idx_key, key in enumerate(keys, start=1):
        if args.log_every > 0 and (idx_key == 1 or idx_key % args.log_every == 0 or idx_key == total_keys):
            print(f"[pbp] {idx_key}/{total_keys} reading {key}")
        table = _load_table(s3, key, desired_cols)
        team_ids = _get_col(table, "teamid")
        startdates = _get_col(table, "startdate")

        cols = {name: _get_col(table, name) for name in desired_cols if name not in ("teamid", "startdate")}

        for i in range(table.num_rows):
            team_id = _to_int(team_ids[i])
            if team_id is None:
                continue
            d = _parse_date(startdates[i])
            if d is None:
                continue
            teams.add(team_id)
            all_dates.add(d)
            key_td = (team_id, d)
            totals = by_team_date[key_td]
            totals.games_played += 1
            totals.team_possessions += _to_float(cols["team_possessions"][i]) or 0.0
            totals.team_possessions_formula += _to_float(cols["team_possessions_formula"][i]) or 0.0
            totals.opp_possessions += _to_float(cols["opp_possessions"][i]) or 0.0
            totals.opp_possessions_formula += _to_float(cols["opp_possessions_formula"][i]) or 0.0
            totals.team_points_total += _to_float(cols["team_points_total"][i]) or 0.0
            totals.opp_points_total += _to_float(cols["opp_points_total"][i]) or 0.0
            totals.team_2fg_made += _to_float(cols["team_2fg_made"][i]) or 0.0
            totals.team_2fg_att += _to_float(cols["team_2fg_att"][i]) or 0.0
            totals.team_3fg_made += _to_float(cols["team_3fg_made"][i]) or 0.0
            totals.team_3fg_att += _to_float(cols["team_3fg_att"][i]) or 0.0
            totals.team_ft_made += _to_float(cols["team_ft_made"][i]) or 0.0
            totals.team_ft_att += _to_float(cols["team_ft_att"][i]) or 0.0
            totals.team_fg_made += _to_float(cols["team_fg_made"][i]) or 0.0
            totals.team_fg_att += _to_float(cols["team_fg_att"][i]) or 0.0
            totals.team_tov_total += _to_float(cols["team_tov_total"][i]) or 0.0
            totals.team_reb_off += _to_float(cols["team_reb_off"][i]) or 0.0
            totals.team_reb_def += _to_float(cols["team_reb_def"][i]) or 0.0
            totals.team_assists += _to_float(cols["team_assists"][i]) or 0.0
            totals.team_steals += _to_float(cols["team_steals"][i]) or 0.0
            totals.team_blocks += _to_float(cols["team_blocks"][i]) or 0.0
            totals.opp_2fg_made += _to_float(cols["opp_2fg_made"][i]) or 0.0
            totals.opp_2fg_att += _to_float(cols["opp_2fg_att"][i]) or 0.0
            totals.opp_3fg_made += _to_float(cols["opp_3fg_made"][i]) or 0.0
            totals.opp_3fg_att += _to_float(cols["opp_3fg_att"][i]) or 0.0
            totals.opp_ft_made += _to_float(cols["opp_ft_made"][i]) or 0.0
            totals.opp_ft_att += _to_float(cols["opp_ft_att"][i]) or 0.0
            totals.opp_fg_made += _to_float(cols["opp_fg_made"][i]) or 0.0
            totals.opp_fg_att += _to_float(cols["opp_fg_att"][i]) or 0.0
            totals.opp_tov_total += _to_float(cols["opp_tov_total"][i]) or 0.0
            totals.opp_reb_off += _to_float(cols["opp_reb_off"][i]) or 0.0
            totals.opp_reb_def += _to_float(cols["opp_reb_def"][i]) or 0.0
            totals.opp_assists += _to_float(cols["opp_assists"][i]) or 0.0
            totals.opp_steals += _to_float(cols["opp_steals"][i]) or 0.0
            totals.opp_blocks += _to_float(cols["opp_blocks"][i]) or 0.0
            totals.game_minutes_total += _to_float(cols["game_minutes"][i]) or 0.0

    if not all_dates:
        raise SystemExit("No dates found in source table.")

    min_date = min(all_dates)
    max_date = max(all_dates)
    all_calendar_dates: List[date] = []
    current = min_date
    while current <= max_date:
        all_calendar_dates.append(current)
        current += timedelta(days=1)

    records: List[Dict[str, object]] = []
    for team_id in sorted(teams):
        running = Totals()
        for d in all_calendar_dates:
            daily = by_team_date.get((team_id, d))
            if daily:
                running.add(daily)

            games = running.games_played
            fga = running.team_fg_att
            fta = running.team_ft_att
            opp_fga = running.opp_fg_att
            opp_fta = running.opp_ft_att

            team_efg = _safe_div(running.team_fg_made + 0.5 * running.team_3fg_made, fga)
            team_ts = _safe_div(running.team_points_total, 2.0 * (fga + 0.44 * fta))
            team_ft_rate = _safe_div(fta, fga)
            team_tov_ratio = _safe_div(running.team_tov_total, running.team_possessions)
            team_oreb_pct = _safe_div(running.team_reb_off, running.team_reb_off + running.opp_reb_def)

            opp_efg = _safe_div(running.opp_fg_made + 0.5 * running.opp_3fg_made, opp_fga)
            opp_ts = _safe_div(running.opp_points_total, 2.0 * (opp_fga + 0.44 * opp_fta))
            opp_ft_rate = _safe_div(opp_fta, opp_fga)
            opp_tov_ratio = _safe_div(running.opp_tov_total, running.opp_possessions)
            opp_oreb_pct = _safe_div(running.opp_reb_off, running.opp_reb_off + running.team_reb_def)

            pace = 0.0
            pace_formula = 0.0
            if running.game_minutes_total > 0:
                pace = ((running.team_possessions + running.opp_possessions) / 2.0) * (40.0 / running.game_minutes_total)
                pace_formula = ((running.team_possessions_formula + running.opp_possessions_formula) / 2.0) * (40.0 / running.game_minutes_total)

            team_points_per_game = _safe_div(running.team_points_total, games)
            opp_points_per_game = _safe_div(running.opp_points_total, games)
            team_points_per_poss = _safe_div(running.team_points_total, running.team_possessions)
            team_points_per_poss_formula = _safe_div(running.team_points_total, running.team_possessions_formula)
            opp_points_per_poss = _safe_div(running.opp_points_total, running.opp_possessions)
            opp_points_per_poss_formula = _safe_div(running.opp_points_total, running.opp_possessions_formula)

            record = {
                "teamid": team_id,
                "games_played": games,
                "team_possessions": running.team_possessions,
                "team_possessions_formula": running.team_possessions_formula,
                "opp_possessions": running.opp_possessions,
                "opp_possessions_formula": running.opp_possessions_formula,
                "team_points_total": running.team_points_total,
                "opp_points_total": running.opp_points_total,
                "team_2fg_made": running.team_2fg_made,
                "team_2fg_att": running.team_2fg_att,
                "team_3fg_made": running.team_3fg_made,
                "team_3fg_att": running.team_3fg_att,
                "team_ft_made": running.team_ft_made,
                "team_ft_att": running.team_ft_att,
                "team_fg_made": running.team_fg_made,
                "team_fg_att": running.team_fg_att,
                "team_tov_total": running.team_tov_total,
                "team_reb_off": running.team_reb_off,
                "team_reb_def": running.team_reb_def,
                "team_assists": running.team_assists,
                "team_steals": running.team_steals,
                "team_blocks": running.team_blocks,
                "opp_2fg_made": running.opp_2fg_made,
                "opp_2fg_att": running.opp_2fg_att,
                "opp_3fg_made": running.opp_3fg_made,
                "opp_3fg_att": running.opp_3fg_att,
                "opp_ft_made": running.opp_ft_made,
                "opp_ft_att": running.opp_ft_att,
                "opp_fg_made": running.opp_fg_made,
                "opp_fg_att": running.opp_fg_att,
                "opp_tov_total": running.opp_tov_total,
                "opp_reb_off": running.opp_reb_off,
                "opp_reb_def": running.opp_reb_def,
                "opp_assists": running.opp_assists,
                "opp_steals": running.opp_steals,
                "opp_blocks": running.opp_blocks,
                "team_efg_pct": team_efg,
                "team_true_shooting": team_ts,
                "team_ft_rate": team_ft_rate,
                "team_tov_ratio": team_tov_ratio,
                "team_oreb_pct": team_oreb_pct,
                "opp_efg_pct": opp_efg,
                "opp_true_shooting": opp_ts,
                "opp_ft_rate": opp_ft_rate,
                "opp_tov_ratio": opp_tov_ratio,
                "opp_oreb_pct": opp_oreb_pct,
                "pace": pace,
                "pace_formula": pace_formula,
                "team_points_per_game": team_points_per_game,
                "opp_points_per_game": opp_points_per_game,
                "team_points_per_poss": team_points_per_poss,
                "team_points_per_poss_formula": team_points_per_poss_formula,
                "opp_points_per_poss": opp_points_per_poss,
                "opp_points_per_poss_formula": opp_points_per_poss_formula,
                "team_fga_per_game": _safe_div(running.team_fg_att, games),
                "team_fta_per_game": _safe_div(running.team_ft_att, games),
                "team_tov_per_game": _safe_div(running.team_tov_total, games),
                "team_reb_off_per_game": _safe_div(running.team_reb_off, games),
                "team_reb_def_per_game": _safe_div(running.team_reb_def, games),
                "team_assists_per_game": _safe_div(running.team_assists, games),
                "team_steals_per_game": _safe_div(running.team_steals, games),
                "team_blocks_per_game": _safe_div(running.team_blocks, games),
                "team_fga_per_poss": _safe_div(running.team_fg_att, running.team_possessions),
                "team_fta_per_poss": _safe_div(running.team_ft_att, running.team_possessions),
                "team_tov_per_poss": _safe_div(running.team_tov_total, running.team_possessions),
                "team_reb_off_per_poss": _safe_div(running.team_reb_off, running.team_possessions),
                "team_reb_def_per_poss": _safe_div(running.team_reb_def, running.opp_possessions),
                "team_assists_per_poss": _safe_div(running.team_assists, running.team_possessions),
                "team_steals_per_poss": _safe_div(running.team_steals, running.opp_possessions),
                "team_blocks_per_poss": _safe_div(running.team_blocks, running.opp_possessions),
                "team_fga_per_poss_formula": _safe_div(running.team_fg_att, running.team_possessions_formula),
                "team_fta_per_poss_formula": _safe_div(running.team_ft_att, running.team_possessions_formula),
                "team_tov_per_poss_formula": _safe_div(running.team_tov_total, running.team_possessions_formula),
                "team_reb_off_per_poss_formula": _safe_div(running.team_reb_off, running.team_possessions_formula),
                "team_reb_def_per_poss_formula": _safe_div(running.team_reb_def, running.opp_possessions_formula),
                "team_assists_per_poss_formula": _safe_div(running.team_assists, running.team_possessions_formula),
                "team_steals_per_poss_formula": _safe_div(running.team_steals, running.opp_possessions_formula),
                "team_blocks_per_poss_formula": _safe_div(running.team_blocks, running.opp_possessions_formula),
                "opp_fga_per_game": _safe_div(running.opp_fg_att, games),
                "opp_fta_per_game": _safe_div(running.opp_ft_att, games),
                "opp_tov_per_game": _safe_div(running.opp_tov_total, games),
                "opp_reb_off_per_game": _safe_div(running.opp_reb_off, games),
                "opp_reb_def_per_game": _safe_div(running.opp_reb_def, games),
                "opp_assists_per_game": _safe_div(running.opp_assists, games),
                "opp_steals_per_game": _safe_div(running.opp_steals, games),
                "opp_blocks_per_game": _safe_div(running.opp_blocks, games),
                "opp_fga_per_poss": _safe_div(running.opp_fg_att, running.opp_possessions),
                "opp_fta_per_poss": _safe_div(running.opp_ft_att, running.opp_possessions),
                "opp_tov_per_poss": _safe_div(running.opp_tov_total, running.opp_possessions),
                "opp_reb_off_per_poss": _safe_div(running.opp_reb_off, running.opp_possessions),
                "opp_reb_def_per_poss": _safe_div(running.opp_reb_def, running.team_possessions),
                "opp_assists_per_poss": _safe_div(running.opp_assists, running.opp_possessions),
                "opp_steals_per_poss": _safe_div(running.opp_steals, running.team_possessions),
                "opp_blocks_per_poss": _safe_div(running.opp_blocks, running.team_possessions),
                "opp_fga_per_poss_formula": _safe_div(running.opp_fg_att, running.opp_possessions_formula),
                "opp_fta_per_poss_formula": _safe_div(running.opp_ft_att, running.opp_possessions_formula),
                "opp_tov_per_poss_formula": _safe_div(running.opp_tov_total, running.opp_possessions_formula),
                "opp_reb_off_per_poss_formula": _safe_div(running.opp_reb_off, running.opp_possessions_formula),
                "opp_reb_def_per_poss_formula": _safe_div(running.opp_reb_def, running.team_possessions_formula),
                "opp_assists_per_poss_formula": _safe_div(running.opp_assists, running.opp_possessions_formula),
                "opp_steals_per_poss_formula": _safe_div(running.opp_steals, running.team_possessions_formula),
                "opp_blocks_per_poss_formula": _safe_div(running.opp_blocks, running.team_possessions_formula),
                "game_minutes_total": running.game_minutes_total,
            }
            records.append((d.isoformat(), record))

    if not records:
        raise SystemExit("No records produced; check source data.")

    by_date: Dict[str, List[Dict[str, object]]] = defaultdict(list)
    for rec_date, rec in records:
        by_date[rec_date].append(rec)

    if args.dry_run:
        for d, rows in sorted(by_date.items()):
            print(f"[pbp] {d} produced {len(rows)} rows")
        return

    run_id = new_run_id()
    for d, rows in sorted(by_date.items()):
        table = normalize_records(args.output_table, rows)
        part = f"season={args.season}/date={d}"
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
    sample = normalize_records(args.output_table, [records[0][1]])
    glue.ensure_table(
        database="cbbd_silver",
        name=args.output_table,
        location=location,
        schema=sample.schema,
        partition_keys=["season", "date"],
    )


if __name__ == "__main__":
    main()
