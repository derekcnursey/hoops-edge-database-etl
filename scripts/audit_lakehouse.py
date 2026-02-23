#!/usr/bin/env python3
"""Comprehensive S3 lakehouse data integrity audit.

Checks every silver and gold table for:
- Row counts per season
- Primary key duplicates
- Null percentages per column
- Season-over-season anomalies
- Cross-table consistency
- Domain-specific sanity checks
"""

from __future__ import annotations

import io
import sys
import time
import traceback
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import boto3
import pyarrow as pa
import pyarrow.parquet as pq

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
BUCKET = "hoops-edge"
REGION = "us-east-1"
SEASONS = list(range(2010, 2027))

# Silver tables keyed by name → (primary_keys, has_season_partition)
SILVER_TABLES: Dict[str, Tuple[Tuple[str, ...], bool]] = {
    "dim_teams": (("teamId",), True),
    "dim_conferences": (("id",), True),
    "dim_venues": (("id",), True),
    "dim_lines_providers": (("id",), True),
    "dim_play_types": (("id",), True),
    "fct_games": (("gameId",), True),
    "fct_game_media": (("gameId",), True),
    "fct_lines": (("gameId", "provider"), True),
    "fct_game_teams": (("gameId", "teamId"), True),
    "fct_game_players": (("gameId", "playerId"), True),
    "fct_pbp_game_team_stats": (("gameId", "teamId"), True),
    "fct_pbp_plays_enriched": (("id",), True),
    "fct_pbp_game_teams_flat": (("gameid", "teamid"), True),
    "fct_pbp_game_teams_flat_garbage_removed": (("gameid", "teamid"), True),
    "fct_pbp_team_daily_rollup": (("teamid",), False),
    "fct_pbp_team_daily_rollup_garbage_removed": (("teamid",), False),
    "fct_pbp_team_daily_rollup_adj": (("teamid",), False),
    "fct_pbp_team_daily_rollup_adj_garbage_removed": (("teamid",), False),
    "fct_plays": (("id",), True),
    "fct_substitutions": (("id",), True),
    "fct_lineups": (("idhash", "teamid", "totalseconds"), True),
    "fct_rankings": (("season", "pollDate", "pollType", "teamId"), True),
    "fct_ratings_adjusted": (("teamid", "season"), True),
    "fct_ratings_srs": (("teamId", "season"), False),
    "fct_team_season_stats": (("teamId", "season"), True),
    "fct_team_season_shooting": (("teamId", "season"), True),
    "fct_player_season_stats": (("playerId", "season"), True),
    "fct_player_season_shooting": (("playerId", "season"), True),
    "fct_recruiting_players": (("playerId", "season"), True),
    "fct_draft_picks": (("id",), True),
}

GOLD_TABLES: Dict[str, Tuple[Tuple[str, ...], bool]] = {
    "team_power_rankings": (("teamId", "season"), True),
    "game_predictions_features": (("gameId", "teamId"), True),
    "player_season_impact": (("playerId", "season"), True),
    "market_lines_analysis": (("gameId", "provider"), True),
    "team_season_summary": (("teamId", "season"), True),
    "team_adjusted_efficiencies": (("teamId", "season", "rating_date"), True),
    "team_adjusted_efficiencies_no_garbage": (("teamId", "season", "rating_date"), True),
}

# ---------------------------------------------------------------------------
# S3 helpers
# ---------------------------------------------------------------------------

class S3Reader:
    def __init__(self, bucket: str, region: str):
        self.bucket = bucket
        self._client = boto3.client("s3", region_name=region)

    def list_keys(self, prefix: str) -> List[str]:
        keys = []
        paginator = self._client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                keys.append(obj["Key"])
        return keys

    def read_parquet(self, key: str, columns: Optional[List[str]] = None) -> pa.Table:
        obj = self._client.get_object(Bucket=self.bucket, Key=key)
        data = obj["Body"].read()
        return pq.read_table(io.BytesIO(data), columns=columns)

    def read_table_season(
        self, layer: str, table_name: str, season: int,
        columns: Optional[List[str]] = None,
    ) -> pa.Table:
        prefix = f"{layer}/{table_name}/season={season}/"
        return self._read_prefix(prefix, columns)

    def read_table_all(
        self, layer: str, table_name: str,
        columns: Optional[List[str]] = None,
    ) -> pa.Table:
        prefix = f"{layer}/{table_name}/"
        return self._read_prefix(prefix, columns)

    def _read_prefix(self, prefix: str, columns: Optional[List[str]] = None) -> pa.Table:
        keys = self.list_keys(prefix)
        parquet_keys = [k for k in keys if k.endswith(".parquet")]
        if not parquet_keys:
            return pa.table({})
        tables = []
        for key in parquet_keys:
            try:
                tbl = self.read_parquet(key, columns)
                tables.append(tbl)
            except Exception:
                pass
        if not tables:
            return pa.table({})
        try:
            return pa.concat_tables(tables, promote_options="permissive")
        except (pa.ArrowInvalid, pa.ArrowTypeError):
            return self._concat_unified(tables)

    @staticmethod
    def _concat_unified(tables: List[pa.Table]) -> pa.Table:
        col_types: Dict[str, pa.DataType] = {}
        for tbl in tables:
            for fld in tbl.schema:
                existing = col_types.get(fld.name)
                if existing is None:
                    col_types[fld.name] = fld.type
                elif existing != fld.type:
                    numeric = {pa.int8(), pa.int16(), pa.int32(), pa.int64(),
                               pa.float16(), pa.float32(), pa.float64()}
                    if existing in numeric and fld.type in numeric:
                        col_types[fld.name] = pa.float64()
                    else:
                        col_types[fld.name] = pa.string()
        schema = pa.schema([(n, t) for n, t in col_types.items()])
        aligned = []
        for tbl in tables:
            cols = {}
            for f in schema:
                if f.name in tbl.column_names:
                    c = tbl.column(f.name)
                    if c.type != f.type:
                        c = c.cast(f.type, safe=False)
                    cols[f.name] = c
                else:
                    cols[f.name] = pa.nulls(tbl.num_rows, type=f.type)
            aligned.append(pa.table(cols))
        return pa.concat_tables(aligned)


# ---------------------------------------------------------------------------
# Audit result types
# ---------------------------------------------------------------------------

@dataclass
class TableAudit:
    table_name: str
    layer: str
    season_row_counts: Dict[int, int] = field(default_factory=dict)
    total_rows: int = 0
    duplicate_counts: Dict[int, int] = field(default_factory=dict)  # season → dup count
    null_pcts: Dict[str, float] = field(default_factory=dict)  # col → pct
    columns: List[str] = field(default_factory=list)
    issues: List[str] = field(default_factory=list)
    status: str = "PASS"  # PASS / WARN / FAIL
    extra: Dict[str, Any] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Audit logic
# ---------------------------------------------------------------------------

def count_duplicates(tbl: pa.Table, pk_cols: Tuple[str, ...]) -> int:
    """Count duplicate rows based on primary key columns."""
    available = [c for c in pk_cols if c in tbl.column_names]
    # Also check lowercase versions
    if len(available) < len(pk_cols):
        lc_map = {c.lower(): c for c in tbl.column_names}
        available = []
        for c in pk_cols:
            if c in tbl.column_names:
                available.append(c)
            elif c.lower() in lc_map:
                available.append(lc_map[c.lower()])
    if not available:
        return -1  # can't check
    key_data = [tbl.column(c).to_pylist() for c in available]
    seen = set()
    dups = 0
    for i in range(tbl.num_rows):
        key = tuple(key_data[j][i] for j in range(len(available)))
        if key in seen:
            dups += 1
        else:
            seen.add(key)
    return dups


def compute_null_pcts(tbl: pa.Table) -> Dict[str, float]:
    """Compute null percentage for each column."""
    result = {}
    if tbl.num_rows == 0:
        return result
    for col_name in tbl.column_names:
        col = tbl.column(col_name)
        null_count = col.null_count
        result[col_name] = round(100.0 * null_count / tbl.num_rows, 2)
    return result


def detect_anomalous_seasons(counts: Dict[int, int]) -> List[str]:
    """Flag seasons with suspiciously low counts vs neighbors."""
    issues = []
    sorted_seasons = sorted(counts.keys())
    for i, s in enumerate(sorted_seasons):
        if counts[s] == 0:
            continue
        neighbors = []
        if i > 0:
            neighbors.append(counts[sorted_seasons[i - 1]])
        if i < len(sorted_seasons) - 1:
            neighbors.append(counts[sorted_seasons[i + 1]])
        neighbors = [n for n in neighbors if n > 0]
        if neighbors:
            avg_neighbor = sum(neighbors) / len(neighbors)
            if avg_neighbor > 0 and counts[s] < avg_neighbor * 0.5:
                issues.append(
                    f"Season {s}: {counts[s]:,} rows is <50% of neighbors avg {avg_neighbor:,.0f}"
                )
    return issues


def audit_silver_table(
    s3: S3Reader, table_name: str, pk_cols: Tuple[str, ...], has_season_partition: bool,
) -> TableAudit:
    """Audit a single silver table."""
    audit = TableAudit(table_name=table_name, layer="silver")
    print(f"  Auditing silver/{table_name} ...", flush=True)

    if has_season_partition:
        all_tables = []
        for season in SEASONS:
            tbl = s3.read_table_season("silver", table_name, season)
            rows = tbl.num_rows
            audit.season_row_counts[season] = rows
            audit.total_rows += rows
            if rows > 0:
                dups = count_duplicates(tbl, pk_cols)
                audit.duplicate_counts[season] = dups
                if not audit.columns:
                    audit.columns = tbl.column_names
                all_tables.append(tbl)

        # Compute null pcts on merged data (sample up to 500k rows)
        if all_tables:
            try:
                merged = pa.concat_tables(all_tables, promote_options="permissive")
            except Exception:
                merged = S3Reader._concat_unified(all_tables)
            audit.null_pcts = compute_null_pcts(merged)
            if not audit.columns:
                audit.columns = merged.column_names
    else:
        # Non-partitioned: read all at once
        tbl = s3.read_table_all("silver", table_name)
        audit.total_rows = tbl.num_rows
        if tbl.num_rows > 0:
            audit.columns = tbl.column_names
            # Try to split by season column
            if "season" in tbl.column_names:
                seasons_arr = tbl.column("season").to_pylist()
                for s in SEASONS:
                    count = sum(1 for x in seasons_arr if x == s)
                    if count > 0:
                        audit.season_row_counts[s] = count
                        # Duplicate check per season
                        mask = pa.compute.equal(tbl.column("season"), s)
                        sub = tbl.filter(mask)
                        dups = count_duplicates(sub, pk_cols)
                        audit.duplicate_counts[s] = dups
            else:
                # No season column at all - report total
                audit.season_row_counts[0] = tbl.num_rows
                dups = count_duplicates(tbl, pk_cols)
                audit.duplicate_counts[0] = dups
            audit.null_pcts = compute_null_pcts(tbl)

    # Detect anomalies
    anomalies = detect_anomalous_seasons(audit.season_row_counts)
    audit.issues.extend(anomalies)

    # Total dups
    total_dups = sum(d for d in audit.duplicate_counts.values() if d > 0)
    if total_dups > 0:
        audit.issues.append(f"Total duplicates on PK: {total_dups:,}")

    if audit.total_rows == 0:
        audit.status = "FAIL"
        audit.issues.append("NO DATA")
    elif total_dups > audit.total_rows * 0.05:
        audit.status = "FAIL"
    elif total_dups > 0 or anomalies:
        audit.status = "WARN"

    return audit


def audit_gold_table(
    s3: S3Reader, table_name: str, pk_cols: Tuple[str, ...], has_season_partition: bool,
) -> TableAudit:
    """Audit a single gold table."""
    audit = TableAudit(table_name=table_name, layer="gold")
    print(f"  Auditing gold/{table_name} ...", flush=True)

    all_tables = []
    if has_season_partition:
        for season in SEASONS:
            tbl = s3.read_table_season("gold", table_name, season)
            rows = tbl.num_rows
            audit.season_row_counts[season] = rows
            audit.total_rows += rows
            if rows > 0:
                dups = count_duplicates(tbl, pk_cols)
                audit.duplicate_counts[season] = dups
                if not audit.columns:
                    audit.columns = tbl.column_names
                all_tables.append(tbl)
    else:
        tbl = s3.read_table_all("gold", table_name)
        audit.total_rows = tbl.num_rows
        if tbl.num_rows > 0:
            audit.columns = tbl.column_names
            all_tables.append(tbl)

    # Null pcts
    if all_tables:
        try:
            merged = pa.concat_tables(all_tables, promote_options="permissive")
        except Exception:
            merged = S3Reader._concat_unified(all_tables)
        audit.null_pcts = compute_null_pcts(merged)
        if not audit.columns:
            audit.columns = merged.column_names

    total_dups = sum(d for d in audit.duplicate_counts.values() if d > 0)
    if total_dups > 0:
        audit.issues.append(f"Total duplicates on PK: {total_dups:,}")

    anomalies = detect_anomalous_seasons(audit.season_row_counts)
    audit.issues.extend(anomalies)

    if audit.total_rows == 0:
        audit.status = "FAIL"
        audit.issues.append("NO DATA")
    elif total_dups > audit.total_rows * 0.05:
        audit.status = "FAIL"
    elif total_dups > 0 or anomalies:
        audit.status = "WARN"

    return audit


# ---------------------------------------------------------------------------
# Domain-specific checks
# ---------------------------------------------------------------------------

def check_fct_games_counts(audit: TableAudit) -> List[str]:
    """fct_games should have ~5,500-6,000 per season."""
    issues = []
    for s, c in sorted(audit.season_row_counts.items()):
        if c > 0 and (c < 4000 or c > 8000):
            issues.append(f"fct_games season {s}: {c:,} rows (expected 5,500-6,000)")
    return issues


def check_game_teams_ratio(
    games_audit: TableAudit, teams_audit: TableAudit,
) -> List[str]:
    """fct_game_teams should be ~2x fct_games per season."""
    issues = []
    for s in sorted(set(games_audit.season_row_counts) | set(teams_audit.season_row_counts)):
        gc = games_audit.season_row_counts.get(s, 0)
        tc = teams_audit.season_row_counts.get(s, 0)
        if gc > 0 and tc > 0:
            ratio = tc / gc
            if abs(ratio - 2.0) > 0.1:
                issues.append(f"Season {s}: game_teams/games ratio = {ratio:.2f} (expected 2.0)")
    return issues


def check_plays_growing(audit: TableAudit) -> List[str]:
    """fct_plays should generally grow over time."""
    issues = []
    sorted_seasons = sorted(
        [(s, c) for s, c in audit.season_row_counts.items() if c > 0]
    )
    for i in range(1, len(sorted_seasons)):
        prev_s, prev_c = sorted_seasons[i - 1]
        cur_s, cur_c = sorted_seasons[i]
        if cur_c < prev_c * 0.5 and cur_s < 2026:  # 2026 is current/incomplete
            issues.append(
                f"fct_plays season {cur_s} ({cur_c:,}) dropped >50% vs {prev_s} ({prev_c:,})"
            )
    return issues


def check_lines_coverage(s3: S3Reader, games_audit: TableAudit, lines_audit: TableAudit) -> Dict[int, float]:
    """Lines coverage = games with lines / total games per season."""
    coverage = {}
    for s in sorted(games_audit.season_row_counts):
        gc = games_audit.season_row_counts.get(s, 0)
        if gc == 0:
            continue
        # Lines has multiple rows per game (one per provider), so count distinct gameIds
        lc_total = lines_audit.season_row_counts.get(s, 0)
        if lc_total == 0:
            coverage[s] = 0.0
            continue
        # Read lines for this season to get distinct games
        tbl = s3.read_table_season("silver", "fct_lines", s)
        if tbl.num_rows > 0 and "gameId" in tbl.column_names:
            distinct_games = len(set(tbl.column("gameId").to_pylist()))
            coverage[s] = round(100.0 * distinct_games / gc, 1)
        else:
            coverage[s] = 0.0
    return coverage


def check_adj_efficiencies(s3: S3Reader) -> Dict[str, Any]:
    """team_adjusted_efficiencies: 364 teams/season? avg adj_oe/adj_de ≈ 100?"""
    results = {"seasons": {}}
    for season in SEASONS:
        tbl = s3.read_table_season("gold", "team_adjusted_efficiencies", season)
        if tbl.num_rows == 0:
            continue
        # Get latest rating_date
        if "rating_date" in tbl.column_names:
            dates = sorted(set(d for d in tbl.column("rating_date").to_pylist() if d))
            if dates:
                latest = dates[-1]
                mask = pa.compute.equal(tbl.column("rating_date"), latest)
                latest_tbl = tbl.filter(mask)
            else:
                latest_tbl = tbl
        else:
            latest_tbl = tbl

        n_teams = latest_tbl.num_rows
        avg_oe = None
        avg_de = None
        if "adj_oe" in latest_tbl.column_names:
            vals = [v for v in latest_tbl.column("adj_oe").to_pylist() if v is not None]
            avg_oe = round(sum(vals) / len(vals), 2) if vals else None
        if "adj_de" in latest_tbl.column_names:
            vals = [v for v in latest_tbl.column("adj_de").to_pylist() if v is not None]
            avg_de = round(sum(vals) / len(vals), 2) if vals else None

        results["seasons"][season] = {
            "total_rows": tbl.num_rows,
            "latest_date_teams": n_teams,
            "avg_adj_oe": avg_oe,
            "avg_adj_de": avg_de,
            "n_dates": len(dates) if "rating_date" in tbl.column_names and dates else 0,
        }
    return results


def check_game_predictions(s3: S3Reader, games_audit: TableAudit) -> Dict[str, Any]:
    """game_predictions_features: row count ≈ 2x games."""
    results = {}
    for season in SEASONS:
        tbl = s3.read_table_season("gold", "game_predictions_features", season)
        gc = games_audit.season_row_counts.get(season, 0)
        rows = tbl.num_rows
        if rows > 0 or gc > 0:
            ratio = round(rows / gc, 2) if gc > 0 else None
            results[season] = {"rows": rows, "games": gc, "ratio": ratio}
    return results


def check_player_season_impact(s3: S3Reader) -> Dict[int, int]:
    """player_season_impact: which seasons have data?"""
    results = {}
    for season in SEASONS:
        tbl = s3.read_table_season("gold", "player_season_impact", season)
        if tbl.num_rows > 0:
            results[season] = tbl.num_rows
    return results


def check_gold_team_consistency(gold_audits: Dict[str, TableAudit], s3: S3Reader) -> Dict[int, Dict[str, int]]:
    """Cross-table: do gold tables have same set of teams per season?"""
    team_tables = ["team_power_rankings", "team_season_summary", "team_adjusted_efficiencies"]
    season_teams: Dict[int, Dict[str, set]] = defaultdict(dict)

    for tname in team_tables:
        for season in SEASONS:
            tbl = s3.read_table_season("gold", tname, season)
            if tbl.num_rows == 0:
                continue
            if "teamId" in tbl.column_names:
                # For adj eff, get latest date only
                if tname == "team_adjusted_efficiencies" and "rating_date" in tbl.column_names:
                    dates = sorted(set(d for d in tbl.column("rating_date").to_pylist() if d))
                    if dates:
                        mask = pa.compute.equal(tbl.column("rating_date"), dates[-1])
                        tbl = tbl.filter(mask)
                teams = set(tbl.column("teamId").to_pylist())
                season_teams[season][tname] = teams

    result = {}
    for season in sorted(season_teams):
        tables_present = season_teams[season]
        if len(tables_present) < 2:
            continue
        counts = {t: len(teams) for t, teams in tables_present.items()}
        result[season] = counts
    return result


def get_top_bottom_teams(s3: S3Reader, table_name: str, season: int, metric_col: str, name_col: str = "team") -> Dict[str, List]:
    """Get top 5 and bottom 5 teams by a metric for sanity check."""
    tbl = s3.read_table_season("gold", table_name, season)
    if tbl.num_rows == 0:
        return {"top5": [], "bottom5": []}

    # For adj eff, filter to latest date
    if table_name in ("team_adjusted_efficiencies", "team_adjusted_efficiencies_no_garbage"):
        if "rating_date" in tbl.column_names:
            dates = sorted(set(d for d in tbl.column("rating_date").to_pylist() if d))
            if dates:
                mask = pa.compute.equal(tbl.column("rating_date"), dates[-1])
                tbl = tbl.filter(mask)

    if metric_col not in tbl.column_names or name_col not in tbl.column_names:
        return {"top5": [], "bottom5": [], "error": f"missing {metric_col} or {name_col}"}

    names = tbl.column(name_col).to_pylist()
    vals = tbl.column(metric_col).to_pylist()
    pairs = [(n, v) for n, v in zip(names, vals) if v is not None]
    pairs.sort(key=lambda x: x[1], reverse=True)

    return {
        "top5": pairs[:5],
        "bottom5": pairs[-5:],
    }


# ---------------------------------------------------------------------------
# Report generation
# ---------------------------------------------------------------------------

def format_season_table(counts: Dict[int, int], label: str = "Rows") -> str:
    """Format season counts as a markdown table."""
    if not counts:
        return "_No data_\n"
    lines = [f"| Season | {label} |", "|--------|------:|"]
    for s in sorted(counts):
        if s == 0:
            lines.append(f"| (all) | {counts[s]:,} |")
        elif counts[s] > 0:
            lines.append(f"| {s} | {counts[s]:,} |")
    return "\n".join(lines) + "\n"


def format_null_pcts(null_pcts: Dict[str, float], top_n: int = 15) -> str:
    """Format null percentages, showing columns with highest null %."""
    if not null_pcts:
        return "_No data_\n"
    sorted_cols = sorted(null_pcts.items(), key=lambda x: x[1], reverse=True)
    # Show all columns with >0% nulls, plus top N
    notable = [(c, p) for c, p in sorted_cols if p > 0]
    if not notable:
        return "_All columns 0% null_\n"
    lines = ["| Column | Null % |", "|--------|-------:|"]
    for col, pct in notable[:top_n]:
        lines.append(f"| {col} | {pct}% |")
    if len(notable) > top_n:
        lines.append(f"| _...{len(notable) - top_n} more_ | |")
    return "\n".join(lines) + "\n"


def format_dup_table(dup_counts: Dict[int, int]) -> str:
    """Format duplicate counts per season."""
    notable = {s: c for s, c in dup_counts.items() if c > 0}
    if not notable:
        return "_No duplicates found_\n"
    lines = ["| Season | Duplicates |", "|--------|----------:|"]
    for s in sorted(notable):
        label = "(all)" if s == 0 else str(s)
        lines.append(f"| {label} | {notable[s]:,} |")
    return "\n".join(lines) + "\n"


def generate_report(
    silver_audits: Dict[str, TableAudit],
    gold_audits: Dict[str, TableAudit],
    specific_checks: Dict[str, Any],
) -> str:
    """Generate the full markdown report."""
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    lines = [
        f"# Full Data Integrity Audit — hoops-edge S3 Lakehouse",
        f"",
        f"**Generated**: {now}",
        f"**Bucket**: `hoops-edge` (us-east-1)",
        f"**Seasons scanned**: {SEASONS[0]}–{SEASONS[-1]}",
        f"",
    ]

    # ===== SCORECARD =====
    lines.append("## Final Scorecard\n")
    lines.append("| # | Layer | Table | Status | Issues |")
    lines.append("|---|-------|-------|--------|--------|")
    idx = 1
    all_audits = []
    for name in sorted(silver_audits):
        a = silver_audits[name]
        all_audits.append(a)
        issue_str = "; ".join(a.issues[:3]) if a.issues else "—"
        status_icon = {"PASS": "PASS", "WARN": "WARN", "FAIL": "FAIL"}[a.status]
        lines.append(f"| {idx} | silver | {name} | **{status_icon}** | {issue_str} |")
        idx += 1
    for name in sorted(gold_audits):
        a = gold_audits[name]
        all_audits.append(a)
        issue_str = "; ".join(a.issues[:3]) if a.issues else "—"
        status_icon = {"PASS": "PASS", "WARN": "WARN", "FAIL": "FAIL"}[a.status]
        lines.append(f"| {idx} | gold | {name} | **{status_icon}** | {issue_str} |")
        idx += 1

    total_pass = sum(1 for a in all_audits if a.status == "PASS")
    total_warn = sum(1 for a in all_audits if a.status == "WARN")
    total_fail = sum(1 for a in all_audits if a.status == "FAIL")
    lines.append(f"\n**Summary**: {total_pass} PASS, {total_warn} WARN, {total_fail} FAIL out of {len(all_audits)} tables\n")

    # ===== SILVER DETAIL =====
    lines.append("---\n")
    lines.append("## Silver Layer Detail\n")
    for name in sorted(silver_audits):
        a = silver_audits[name]
        lines.append(f"### silver/{name}\n")
        lines.append(f"**Status**: {a.status} | **Total rows**: {a.total_rows:,} | **Columns**: {len(a.columns)}\n")
        if a.issues:
            lines.append("**Issues**:")
            for issue in a.issues:
                lines.append(f"- {issue}")
            lines.append("")

        lines.append("#### Row Counts by Season\n")
        lines.append(format_season_table(a.season_row_counts))

        lines.append("#### Duplicates on Primary Key\n")
        lines.append(format_dup_table(a.duplicate_counts))

        lines.append("#### Null Percentages (columns with nulls)\n")
        lines.append(format_null_pcts(a.null_pcts))
        lines.append("")

    # ===== GOLD DETAIL =====
    lines.append("---\n")
    lines.append("## Gold Layer Detail\n")
    for name in sorted(gold_audits):
        a = gold_audits[name]
        lines.append(f"### gold/{name}\n")
        lines.append(f"**Status**: {a.status} | **Total rows**: {a.total_rows:,} | **Columns**: {len(a.columns)}\n")
        if a.issues:
            lines.append("**Issues**:")
            for issue in a.issues:
                lines.append(f"- {issue}")
            lines.append("")

        lines.append("#### Row Counts by Season\n")
        lines.append(format_season_table(a.season_row_counts))

        lines.append("#### Duplicates on Primary Key\n")
        lines.append(format_dup_table(a.duplicate_counts))

        lines.append("#### Null Percentages (columns with nulls)\n")
        lines.append(format_null_pcts(a.null_pcts))
        lines.append("")

    # ===== SPECIFIC CHECKS =====
    lines.append("---\n")
    lines.append("## Specific Checks\n")

    # fct_games counts
    if "fct_games_checks" in specific_checks:
        lines.append("### fct_games: Row Count Check (expected ~5,500-6,000/season)\n")
        checks = specific_checks["fct_games_checks"]
        if checks:
            for issue in checks:
                lines.append(f"- {issue}")
        else:
            lines.append("_All seasons within expected range_")
        lines.append("")

    # game_teams ratio
    if "game_teams_ratio" in specific_checks:
        lines.append("### fct_game_teams / fct_games Ratio (expected 2.0x)\n")
        checks = specific_checks["game_teams_ratio"]
        if checks:
            for issue in checks:
                lines.append(f"- {issue}")
        else:
            lines.append("_All seasons have expected 2:1 ratio_")
        lines.append("")

    # plays growing
    if "plays_growing" in specific_checks:
        lines.append("### fct_plays: Growth Over Time\n")
        checks = specific_checks["plays_growing"]
        if checks:
            for issue in checks:
                lines.append(f"- {issue}")
        else:
            lines.append("_Play-by-play data growing as expected_")
        lines.append("")

    # lines coverage
    if "lines_coverage" in specific_checks:
        lines.append("### fct_lines: Coverage (games with lines / total games)\n")
        cov = specific_checks["lines_coverage"]
        lines.append("| Season | Coverage % |")
        lines.append("|--------|----------:|")
        for s in sorted(cov):
            lines.append(f"| {s} | {cov[s]}% |")
        lines.append("")

    # adj efficiencies
    if "adj_eff" in specific_checks:
        lines.append("### team_adjusted_efficiencies: Team Counts & Avg Ratings\n")
        data = specific_checks["adj_eff"]["seasons"]
        lines.append("| Season | Total Rows | Latest Date Teams | Avg adj_oe | Avg adj_de | # Dates |")
        lines.append("|--------|----------:|------------------:|-----------:|-----------:|--------:|")
        for s in sorted(data):
            d = data[s]
            lines.append(
                f"| {s} | {d['total_rows']:,} | {d['latest_date_teams']:,} | "
                f"{d['avg_adj_oe']} | {d['avg_adj_de']} | {d['n_dates']} |"
            )
        lines.append("")

    # game predictions
    if "game_predictions" in specific_checks:
        lines.append("### game_predictions_features: Row Count vs 2x Games\n")
        preds = specific_checks["game_predictions"]
        lines.append("| Season | Pred Rows | Games | Ratio |")
        lines.append("|--------|----------:|------:|------:|")
        for s in sorted(preds):
            d = preds[s]
            ratio_str = str(d["ratio"]) if d["ratio"] is not None else "—"
            lines.append(f"| {s} | {d['rows']:,} | {d['games']:,} | {ratio_str} |")
        lines.append("")

    # player season impact
    if "player_impact" in specific_checks:
        lines.append("### player_season_impact: Seasons with Data\n")
        pi = specific_checks["player_impact"]
        if pi:
            lines.append("| Season | Players |")
            lines.append("|--------|--------:|")
            for s in sorted(pi):
                lines.append(f"| {s} | {pi[s]:,} |")
        else:
            lines.append("_No data found_")
        lines.append("")

    # gold top/bottom teams
    if "top_bottom_teams" in specific_checks:
        lines.append("### Gold Layer: Top 5 / Bottom 5 Teams (Season 2025, Sanity Check)\n")
        for table_name, metric, data in specific_checks["top_bottom_teams"]:
            lines.append(f"#### {table_name} — {metric}\n")
            if "error" in data:
                lines.append(f"_Error: {data['error']}_\n")
                continue
            if data["top5"]:
                lines.append("**Top 5**:")
                for name, val in data["top5"]:
                    lines.append(f"- {name}: {val:.2f}" if isinstance(val, float) else f"- {name}: {val}")
            if data["bottom5"]:
                lines.append("\n**Bottom 5**:")
                for name, val in data["bottom5"]:
                    lines.append(f"- {name}: {val:.2f}" if isinstance(val, float) else f"- {name}: {val}")
            lines.append("")

    # gold cross-table consistency
    if "gold_team_consistency" in specific_checks:
        lines.append("### Gold Cross-Table Consistency: Teams per Season\n")
        cons = specific_checks["gold_team_consistency"]
        if cons:
            tables_seen = set()
            for s in cons.values():
                tables_seen.update(s.keys())
            sorted_tables = sorted(tables_seen)
            header = "| Season | " + " | ".join(sorted_tables) + " |"
            sep = "|--------|" + "|".join(["------:" for _ in sorted_tables]) + "|"
            lines.append(header)
            lines.append(sep)
            for s in sorted(cons):
                vals = " | ".join(str(cons[s].get(t, 0)) for t in sorted_tables)
                lines.append(f"| {s} | {vals} |")
        else:
            lines.append("_No overlapping data to compare_")
        lines.append("")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    start = time.time()
    s3 = S3Reader(BUCKET, REGION)

    print("=" * 60)
    print("HOOPS-EDGE LAKEHOUSE — FULL DATA INTEGRITY AUDIT")
    print("=" * 60)

    # ---- Silver tables ----
    print("\n[1/4] Auditing SILVER tables...")
    silver_audits: Dict[str, TableAudit] = {}
    for name, (pk, has_part) in SILVER_TABLES.items():
        try:
            silver_audits[name] = audit_silver_table(s3, name, pk, has_part)
            sc = silver_audits[name].season_row_counts
            non_zero = {s: c for s, c in sc.items() if c > 0}
            print(f"    → {name}: {silver_audits[name].total_rows:,} total, "
                  f"{len(non_zero)} seasons with data, status={silver_audits[name].status}")
        except Exception as e:
            print(f"    → ERROR on {name}: {e}")
            traceback.print_exc()
            a = TableAudit(table_name=name, layer="silver")
            a.status = "FAIL"
            a.issues.append(f"Read error: {e}")
            silver_audits[name] = a

    # ---- Gold tables ----
    print("\n[2/4] Auditing GOLD tables...")
    gold_audits: Dict[str, TableAudit] = {}
    for name, (pk, has_part) in GOLD_TABLES.items():
        try:
            gold_audits[name] = audit_gold_table(s3, name, pk, has_part)
            sc = gold_audits[name].season_row_counts
            non_zero = {s: c for s, c in sc.items() if c > 0}
            print(f"    → {name}: {gold_audits[name].total_rows:,} total, "
                  f"{len(non_zero)} seasons with data, status={gold_audits[name].status}")
        except Exception as e:
            print(f"    → ERROR on {name}: {e}")
            traceback.print_exc()
            a = TableAudit(table_name=name, layer="gold")
            a.status = "FAIL"
            a.issues.append(f"Read error: {e}")
            gold_audits[name] = a

    # ---- Specific checks ----
    print("\n[3/4] Running specific checks...")
    specific_checks = {}

    # fct_games count check
    if "fct_games" in silver_audits:
        specific_checks["fct_games_checks"] = check_fct_games_counts(silver_audits["fct_games"])
        print(f"    → fct_games count: {len(specific_checks['fct_games_checks'])} issues")

    # game_teams ratio
    if "fct_games" in silver_audits and "fct_game_teams" in silver_audits:
        specific_checks["game_teams_ratio"] = check_game_teams_ratio(
            silver_audits["fct_games"], silver_audits["fct_game_teams"]
        )
        print(f"    → game_teams ratio: {len(specific_checks['game_teams_ratio'])} issues")

    # plays growing
    if "fct_plays" in silver_audits:
        specific_checks["plays_growing"] = check_plays_growing(silver_audits["fct_plays"])
        print(f"    → plays growing: {len(specific_checks['plays_growing'])} issues")

    # lines coverage
    if "fct_games" in silver_audits and "fct_lines" in silver_audits:
        print("    → Computing lines coverage (reading distinct gameIds)...")
        specific_checks["lines_coverage"] = check_lines_coverage(
            s3, silver_audits["fct_games"], silver_audits["fct_lines"]
        )
        print(f"    → lines coverage: done for {len(specific_checks['lines_coverage'])} seasons")

    # adj efficiencies
    print("    → Checking team_adjusted_efficiencies...")
    specific_checks["adj_eff"] = check_adj_efficiencies(s3)
    print(f"    → adj_eff: {len(specific_checks['adj_eff']['seasons'])} seasons")

    # game predictions
    if "fct_games" in silver_audits:
        print("    → Checking game_predictions_features...")
        specific_checks["game_predictions"] = check_game_predictions(s3, silver_audits["fct_games"])
        print(f"    → game_predictions: {len(specific_checks['game_predictions'])} seasons")

    # player impact
    print("    → Checking player_season_impact...")
    specific_checks["player_impact"] = check_player_season_impact(s3)
    print(f"    → player_impact: {len(specific_checks['player_impact'])} seasons")

    # top/bottom teams for gold (season 2025 as most complete)
    print("    → Getting top/bottom teams for gold sanity check (season 2025)...")
    top_bottom = []
    sanity_metrics = [
        ("team_adjusted_efficiencies", "adj_oe", "team"),
        ("team_adjusted_efficiencies", "adj_de", "team"),
        ("team_adjusted_efficiencies", "barthag", "team"),
        ("team_season_summary", "wins", "team"),
        ("team_season_summary", "ppg", "team"),
        ("team_power_rankings", "composite_rank", "team"),
    ]
    for tname, metric, name_col in sanity_metrics:
        try:
            data = get_top_bottom_teams(s3, tname, 2025, metric, name_col)
            top_bottom.append((tname, metric, data))
        except Exception as e:
            top_bottom.append((tname, metric, {"top5": [], "bottom5": [], "error": str(e)}))
    specific_checks["top_bottom_teams"] = top_bottom

    # gold team consistency
    print("    → Checking gold cross-table team consistency...")
    specific_checks["gold_team_consistency"] = check_gold_team_consistency(gold_audits, s3)

    # ---- Generate report ----
    print("\n[4/4] Generating report...")
    report = generate_report(silver_audits, gold_audits, specific_checks)

    output_path = "reports/full_data_audit.md"
    with open(output_path, "w") as f:
        f.write(report)

    elapsed = time.time() - start
    print(f"\nDone in {elapsed:.0f}s. Report saved to {output_path}")
    print(f"\nScorecard: {sum(1 for a in list(silver_audits.values()) + list(gold_audits.values()) if a.status == 'PASS')} PASS, "
          f"{sum(1 for a in list(silver_audits.values()) + list(gold_audits.values()) if a.status == 'WARN')} WARN, "
          f"{sum(1 for a in list(silver_audits.values()) + list(gold_audits.values()) if a.status == 'FAIL')} FAIL")


if __name__ == "__main__":
    main()
