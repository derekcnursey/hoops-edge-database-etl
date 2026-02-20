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
    if denom == 0:
        return 0.0
    return num / denom


@dataclass
class GameRow:
    team_id: int
    opp_id: int
    is_home: Optional[bool]
    date: date
    off_eff: float
    def_eff: float
    off_eff_formula: float
    def_eff_formula: float


def _estimate_hca(games: List[GameRow]) -> float:
    home_vals = [g.off_eff for g in games if g.is_home is True]
    away_vals = [g.off_eff for g in games if g.is_home is False]
    if not home_vals or not away_vals:
        return 0.0
    return (sum(home_vals) / len(home_vals) - sum(away_vals) / len(away_vals)) / 2.0


def _iterate_ratings(
    games: List[GameRow],
    teams: List[int],
    hca: float,
    iterations: int,
) -> Tuple[Dict[int, float], Dict[int, float], Dict[int, float], Dict[int, float]]:
    off = {t: 0.0 for t in teams}
    deff = {t: 0.0 for t in teams}
    off_f = {t: 0.0 for t in teams}
    def_f = {t: 0.0 for t in teams}

    if games:
        off_avg = sum(g.off_eff for g in games) / len(games)
        def_avg = sum(g.def_eff for g in games) / len(games)
        off_f_avg = sum(g.off_eff_formula for g in games) / len(games)
        def_f_avg = sum(g.def_eff_formula for g in games) / len(games)
        for t in teams:
            off[t] = off_avg
            deff[t] = def_avg
            off_f[t] = off_f_avg
            def_f[t] = def_f_avg

    for _ in range(iterations):
        off_sum = defaultdict(float)
        off_cnt = defaultdict(int)
        def_sum = defaultdict(float)
        def_cnt = defaultdict(int)
        off_sum_f = defaultdict(float)
        off_cnt_f = defaultdict(int)
        def_sum_f = defaultdict(float)
        def_cnt_f = defaultdict(int)
        for g in games:
            if g.is_home is True:
                hca_sign = 1.0
            elif g.is_home is False:
                hca_sign = -1.0
            else:
                hca_sign = 0.0
            off_sum[g.team_id] += g.off_eff - deff.get(g.opp_id, 0.0) - hca_sign * hca
            off_cnt[g.team_id] += 1
            def_sum[g.team_id] += g.def_eff - off.get(g.opp_id, 0.0) + hca_sign * hca
            def_cnt[g.team_id] += 1

            off_sum_f[g.team_id] += g.off_eff_formula - def_f.get(g.opp_id, 0.0) - hca_sign * hca
            off_cnt_f[g.team_id] += 1
            def_sum_f[g.team_id] += g.def_eff_formula - off_f.get(g.opp_id, 0.0) + hca_sign * hca
            def_cnt_f[g.team_id] += 1

        for t in teams:
            if off_cnt[t]:
                off[t] = off_sum[t] / off_cnt[t]
            if def_cnt[t]:
                deff[t] = def_sum[t] / def_cnt[t]
            if off_cnt_f[t]:
                off_f[t] = off_sum_f[t] / off_cnt_f[t]
            if def_cnt_f[t]:
                def_f[t] = def_sum_f[t] / def_cnt_f[t]

    return off, deff, off_f, def_f


def main() -> None:
    parser = argparse.ArgumentParser(description="Build adjusted team daily rollups with competition and HCA.")
    parser.add_argument("--season", type=int, default=2026)
    parser.add_argument("--source-table", default="fct_pbp_game_teams_flat")
    parser.add_argument("--output-table", default="fct_pbp_team_daily_rollup_adj")
    parser.add_argument("--iterations", type=int, default=25)
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
        "teamid",
        "opponentid",
        "startdate",
        "ishometeam",
        "team_points_total",
        "opp_points_total",
        "team_possessions",
        "opp_possessions",
        "team_possessions_formula",
        "opp_possessions_formula",
    ]

    games_by_date: Dict[date, List[GameRow]] = defaultdict(list)
    teams_set: set[int] = set()
    all_dates: set[date] = set()

    total_keys = len(keys)
    for idx_key, key in enumerate(keys, start=1):
        if args.log_every > 0 and (idx_key == 1 or idx_key % args.log_every == 0 or idx_key == total_keys):
            print(f"[pbp] {idx_key}/{total_keys} reading {key}")
        table = _load_table(s3, key, desired_cols)
        team_ids = _get_col(table, "teamid")
        opp_ids = _get_col(table, "opponentid")
        startdates = _get_col(table, "startdate")
        is_home = _get_col(table, "ishometeam")
        team_points = _get_col(table, "team_points_total")
        opp_points = _get_col(table, "opp_points_total")
        team_poss = _get_col(table, "team_possessions")
        opp_poss = _get_col(table, "opp_possessions")
        team_poss_f = _get_col(table, "team_possessions_formula")
        opp_poss_f = _get_col(table, "opp_possessions_formula")

        for i in range(table.num_rows):
            team_id = _to_int(team_ids[i])
            opp_id = _to_int(opp_ids[i])
            d = _parse_date(startdates[i])
            if team_id is None or opp_id is None or d is None:
                continue
            teams_set.add(team_id)
            teams_set.add(opp_id)
            all_dates.add(d)
            tp = _to_float(team_points[i]) or 0.0
            op = _to_float(opp_points[i]) or 0.0
            tposs = _to_float(team_poss[i]) or 0.0
            oposs = _to_float(opp_poss[i]) or 0.0
            tposs_f = _to_float(team_poss_f[i]) or 0.0
            oposs_f = _to_float(opp_poss_f[i]) or 0.0
            off_eff = _safe_div(tp * 100.0, tposs) if tposs > 0 else 0.0
            def_eff = _safe_div(op * 100.0, oposs) if oposs > 0 else 0.0
            off_eff_f = _safe_div(tp * 100.0, tposs_f) if tposs_f > 0 else 0.0
            def_eff_f = _safe_div(op * 100.0, oposs_f) if oposs_f > 0 else 0.0
            games_by_date[d].append(
                GameRow(
                    team_id=team_id,
                    opp_id=opp_id,
                    is_home=bool(is_home[i]) if is_home[i] is not None else None,
                    date=d,
                    off_eff=off_eff,
                    def_eff=def_eff,
                    off_eff_formula=off_eff_f,
                    def_eff_formula=def_eff_f,
                )
            )

    if not all_dates:
        raise SystemExit("No dates found in source table.")

    teams = sorted(teams_set)
    min_date = min(all_dates)
    max_date = max(all_dates)
    calendar: List[date] = []
    current = min_date
    while current <= max_date:
        calendar.append(current)
        current += timedelta(days=1)

    records: List[Tuple[str, Dict[str, object]]] = []
    all_prior_games: List[GameRow] = []
    for idx_date, d in enumerate(calendar, start=1):
        if args.log_every > 0 and (idx_date == 1 or idx_date % args.log_every == 0 or idx_date == len(calendar)):
            print(f"[pbp] day {idx_date}/{len(calendar)} {d.isoformat()}")
        # Prior games only for no leakage.
        hca = _estimate_hca(all_prior_games)
        off, deff, off_f, def_f = _iterate_ratings(all_prior_games, teams, hca, args.iterations)
        # League averages from prior games (unadjusted per-100 efficiencies).
        league_off_avg = sum(g.off_eff for g in all_prior_games) / len(all_prior_games) if all_prior_games else 0.0
        league_def_avg = sum(g.def_eff for g in all_prior_games) / len(all_prior_games) if all_prior_games else 0.0
        league_off_avg_f = sum(g.off_eff_formula for g in all_prior_games) / len(all_prior_games) if all_prior_games else 0.0
        league_def_avg_f = sum(g.def_eff_formula for g in all_prior_games) / len(all_prior_games) if all_prior_games else 0.0

        adj_off = {t: off.get(t, 0.0) + league_off_avg for t in teams}
        adj_def = {t: deff.get(t, 0.0) + league_def_avg for t in teams}
        adj_off_f = {t: off_f.get(t, 0.0) + league_off_avg_f for t in teams}
        adj_def_f = {t: def_f.get(t, 0.0) + league_def_avg_f for t in teams}

        # Weighted efficiencies based on opponent strength in prior games.
        weighted_off = defaultdict(list)
        weighted_def = defaultdict(list)
        weighted_off_f = defaultdict(list)
        weighted_def_f = defaultdict(list)
        for g in all_prior_games:
            if league_def_avg:
                weighted_off[g.team_id].append(g.off_eff * (adj_def.get(g.opp_id, league_def_avg) / league_def_avg))
            else:
                weighted_off[g.team_id].append(g.off_eff)
            if league_off_avg:
                weighted_def[g.team_id].append(g.def_eff * (adj_off.get(g.opp_id, league_off_avg) / league_off_avg))
            else:
                weighted_def[g.team_id].append(g.def_eff)
            if league_def_avg_f:
                weighted_off_f[g.team_id].append(g.off_eff_formula * (adj_def_f.get(g.opp_id, league_def_avg_f) / league_def_avg_f))
            else:
                weighted_off_f[g.team_id].append(g.off_eff_formula)
            if league_off_avg_f:
                weighted_def_f[g.team_id].append(g.def_eff_formula * (adj_off_f.get(g.opp_id, league_off_avg_f) / league_off_avg_f))
            else:
                weighted_def_f[g.team_id].append(g.def_eff_formula)

        for team_id in teams:
            rec = {
                "teamid": team_id,
                "adj_off_eff": adj_off.get(team_id, 0.0),
                "adj_def_eff": adj_def.get(team_id, 0.0),
                "adj_net_eff": adj_off.get(team_id, 0.0) - adj_def.get(team_id, 0.0),
                "adj_off_eff_formula": adj_off_f.get(team_id, 0.0),
                "adj_def_eff_formula": adj_def_f.get(team_id, 0.0),
                "adj_net_eff_formula": adj_off_f.get(team_id, 0.0) - adj_def_f.get(team_id, 0.0),
                "weighted_off_eff": sum(weighted_off.get(team_id, [])) / len(weighted_off.get(team_id, [])) if weighted_off.get(team_id) else 0.0,
                "weighted_def_eff": sum(weighted_def.get(team_id, [])) / len(weighted_def.get(team_id, [])) if weighted_def.get(team_id) else 0.0,
                "weighted_off_eff_formula": sum(weighted_off_f.get(team_id, [])) / len(weighted_off_f.get(team_id, [])) if weighted_off_f.get(team_id) else 0.0,
                "weighted_def_eff_formula": sum(weighted_def_f.get(team_id, [])) / len(weighted_def_f.get(team_id, [])) if weighted_def_f.get(team_id) else 0.0,
                "hca_points_per_100": hca,
            }
            records.append((d.isoformat(), rec))

        # Add current date games to prior pool after writing pre-game metrics.
        all_prior_games.extend(games_by_date.get(d, []))

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
