from __future__ import annotations

import asyncio
import json
import ast
import re
from datetime import date, datetime, timedelta
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

import boto3

from .api_client import ApiClient, ApiConfig
from .checkpoint import CheckpointStore
from .config import Config, get_api_token
from .extractors import build_registry
from .glue_catalog import GlueCatalog
from .logging_utils import log_json
from .normalize import TABLE_SPECS, dedupe_records, normalize_records
from .s3_io import S3IO, make_part_key, new_run_id
from .utils import stable_hash


BRONZE_TABLES = {
    "conferences": "conferences",
    "conferences_history": "conferences_history",
    "draft_picks": "draft_picks",
    "draft_positions": "draft_positions",
    "draft_teams": "draft_teams",
    "games": "games",
    "games_media": "games_media",
    "games_players": "games_players",
    "games_teams": "games_teams",
    "lines": "lines",
    "lines_providers": "lines_providers",
    "lineups_game": "lineups_game",
    "lineups_team": "lineups_team",
    "plays_types": "plays_types",
    "plays_game": "plays_game",
    "plays_date": "plays_date",
    "plays_player": "plays_player",
    "plays_team": "plays_team",
    "plays_tournament": "plays_tournament",
    "substitutions_game": "substitutions_game",
    "substitutions_player": "substitutions_player",
    "substitutions_team": "substitutions_team",
    "rankings": "rankings",
    "ratings_adjusted": "ratings_adjusted",
    "ratings_srs": "ratings_srs",
    "recruiting_players": "recruiting_players",
    "stats_player_shooting_season": "stats_player_shooting_season",
    "stats_player_season": "stats_player_season",
    "stats_team_shooting_season": "stats_team_shooting_season",
    "stats_team_season": "stats_team_season",
    "teams": "teams",
    "teams_roster": "teams_roster",
    "venues": "venues",
}

SILVER_TABLES = {
    "teams": "dim_teams",
    "conferences": "dim_conferences",
    "venues": "dim_venues",
    "lines_providers": "dim_lines_providers",
    "plays_types": "dim_play_types",
    "games": "fct_games",
    "games_media": "fct_game_media",
    "lines": "fct_lines",
    "games_teams": "fct_game_teams",
    "games_players": "fct_game_players",
    "plays_game": "fct_plays",
    "plays_date": "fct_plays",
    "plays_player": "fct_plays",
    "plays_team": "fct_plays",
    "plays_tournament": "fct_plays",
    "substitutions_game": "fct_substitutions",
    "substitutions_player": "fct_substitutions",
    "substitutions_team": "fct_substitutions",
    "lineups_game": "fct_lineups",
    "lineups_team": "fct_lineups",
    "rankings": "fct_rankings",
    "ratings_adjusted": "fct_ratings_adjusted",
    "ratings_srs": "fct_ratings_srs",
    "stats_team_season": "fct_team_season_stats",
    "stats_team_shooting_season": "fct_team_season_shooting",
    "stats_player_season": "fct_player_season_stats",
    "stats_player_shooting_season": "fct_player_season_shooting",
    "recruiting_players": "fct_recruiting_players",
    "draft_picks": "fct_draft_picks",
}

RAW_PREFIX_OVERRIDES = {
    "plays_game": "plays/game",
    "plays_date": "plays/date",
    "plays_player": "plays/player",
    "plays_team": "plays/team",
    "plays_tournament": "plays/tournament",
    "lineups_game": "lineups/game",
    "substitutions_game": "substitutions/game",
    "substitutions_player": "substitutions/player",
    "substitutions_team": "substitutions/team",
}

GOLD_TABLES = {
    "ratings_adjusted": "team_quality_daily",
    "lines": "market_lines_history",
}

def _today() -> str:
    return datetime.utcnow().date().isoformat()


class Orchestrator:
    def __init__(self, config: Config, logger) -> None:
        self.config = config
        self.logger = logger
        self.s3 = S3IO(config.bucket, config.region)
        self.checkpoints = CheckpointStore(config.region)
        self.glue = GlueCatalog(config.region)
        api_cfg = ApiConfig(**config.api)
        self.api = ApiClient(get_api_token(), api_cfg)
        self.api.set_logger(self.logger)
        self.registry = build_registry(config.endpoints)
        self._game_ids: Set[int] = set()
        self._game_meta: Dict[int, Dict[str, Any]] = {}
        self._player_ids: Set[int] = set()
        self._player_seasons: Set[Tuple[int, int]] = set()
        self._run_id = new_run_id()
        self._summary: Dict[str, Any] = {"run_id": self._run_id, "started_at": datetime.utcnow().isoformat()}
        self._request_counts: Dict[str, int] = {}
        self._log_every = int(self.config.api.get("log_every_requests", 100))
        self._seen_keys: Dict[str, set[Tuple[Any, ...]]] = {}

    async def close(self) -> None:
        await self.api.close()

    def ensure_prefixes(self) -> None:
        layout = self.config.s3_layout
        prefixes = [
            layout["raw_prefix"],
            layout["bronze_prefix"],
            layout["silver_prefix"],
            layout["gold_prefix"],
            layout["ref_prefix"],
            layout["meta_prefix"],
            layout["deadletter_prefix"],
            layout["tmp_prefix"],
            layout["athena_prefix"],
        ]
        self.s3.ensure_prefixes(prefixes)

    async def run_backfill(
        self,
        seasons: Optional[List[int]] = None,
        skip_fanout: bool = False,
        only_endpoints: Optional[List[str]] = None,
    ) -> None:
        self.ensure_prefixes()
        if seasons is None:
            seasons = list(range(self.config.seasons["start"], self.config.seasons["end"] + 1))
        for name, spec in self.registry.items():
            if only_endpoints and name not in only_endpoints:
                continue
            if skip_fanout and spec.type in ("game_fanout", "player_fanout"):
                log_json(self.logger, "skip_fanout_endpoint", endpoint=name)
                continue
            await self._run_endpoint(name, spec, seasons=seasons, mode="backfill")
        await self._finalize_summary()

    async def run_incremental(
        self,
        seasons: Optional[List[int]] = None,
        skip_fanout: bool = False,
        only_endpoints: Optional[List[str]] = None,
    ) -> None:
        self.ensure_prefixes()
        if seasons is None:
            seasons = list(range(self.config.seasons["start"], self.config.seasons["end"] + 1))
        for name, spec in self.registry.items():
            if only_endpoints and name not in only_endpoints:
                continue
            if skip_fanout and spec.type in ("game_fanout", "player_fanout"):
                log_json(self.logger, "skip_fanout_endpoint", endpoint=name)
                continue
            await self._run_endpoint(name, spec, seasons=seasons, mode="incremental")
        await self._finalize_summary()

    async def run_one(self, endpoint: str, params: Dict[str, Any]) -> None:
        self.ensure_prefixes()
        spec = self.registry[endpoint]
        await self._run_single_call(spec, params, mode="one")
        await self._finalize_summary()

    async def validate(self) -> None:
        s3 = boto3.client("s3", region_name=self.config.region)
        prefix = f"{self.config.s3_layout['meta_prefix']}/"
        resp = s3.list_objects_v2(Bucket=self.config.bucket, Prefix=prefix)
        contents = resp.get("Contents", [])
        if not contents:
            log_json(self.logger, "validate_no_meta_found")
            return
        latest = max(contents, key=lambda x: x["LastModified"])
        obj = s3.get_object(Bucket=self.config.bucket, Key=latest["Key"])
        summary = json.loads(obj["Body"].read())
        issues = []
        for endpoint, stats in summary.get("endpoints", {}).items():
            if stats.get("rows", 0) <= 0:
                issues.append(endpoint)
        if issues:
            log_json(self.logger, "validate_failed", endpoints=issues)
            raise RuntimeError(f"Validation failed for endpoints: {issues}")
        strict_schema = self.config.raw.get("validate", {}).get("strict_schema", False)
        missing = {}
        for table, spec in TABLE_SPECS.items():
            table_prefix = f"{self.config.s3_layout['silver_prefix']}/{table}/"
            resp = s3.list_objects_v2(Bucket=self.config.bucket, Prefix=table_prefix)
            contents = resp.get("Contents", [])
            if not contents:
                continue
            latest_obj = max(contents, key=lambda x: x["LastModified"])
            obj = s3.get_object(Bucket=self.config.bucket, Key=latest_obj["Key"])
            data = obj["Body"].read()
            try:
                import pyarrow.parquet as pq
                import io

                table_pa = pq.read_table(io.BytesIO(data), columns=None)
                cols = set(table_pa.schema.names)
                missing_keys = [k for k in spec.primary_keys if k not in cols]
                if missing_keys:
                    missing[table] = missing_keys
            except Exception as exc:
                missing[table] = [f"schema_read_error:{exc}"]
        if missing:
            log_json(self.logger, "validate_schema_failed", details=missing, strict=strict_schema)
            if strict_schema:
                raise RuntimeError(f"Schema validation failed for: {missing}")
        log_json(self.logger, "validate_ok", endpoints=len(summary.get("endpoints", {})), schema_tables=len(TABLE_SPECS))

    async def _run_endpoint(self, name, spec, seasons: List[int], mode: str) -> None:
        if self._is_skipped(spec.name):
            log_json(self.logger, "skip_endpoint", endpoint=spec.name)
            return
        if spec.name == "plays_date" and mode != "incremental":
            log_json(self.logger, "skip_incremental_only", endpoint=spec.name, mode=mode)
            return
        if spec.type == "snapshot":
            await self._run_single_call(spec, params={}, mode=mode)
            return
        if spec.type == "season":
            await self._run_season_endpoint(spec, seasons, mode)
            return
        if spec.type == "date":
            await self._run_date_endpoint(spec, mode)
            return
        if spec.type == "game_fanout":
            await self._run_game_fanout(spec, seasons, mode)
            return
        if spec.type == "player_fanout":
            await self._run_player_fanout(spec, seasons, mode)
            return
        raise ValueError(f"Unknown endpoint type: {spec.type}")

    async def run_fanout_only(
        self,
        seasons: Optional[List[int]] = None,
        endpoint: Optional[str] = None,
        limit: Optional[int] = None,
        batch_size: Optional[int] = None,
        games_from_s3: bool = False,
        resume_file: Optional[str] = None,
    ) -> None:
        self.ensure_prefixes()
        if seasons is None:
            seasons = list(range(self.config.seasons["start"], self.config.seasons["end"] + 1))
        self._games_from_s3 = games_from_s3
        self._resume_game_ids = self._load_resume_ids(resume_file) if resume_file else None
        fanout_specs = [
            spec for spec in self.registry.values() if spec.type in ("game_fanout", "player_fanout")
        ]
        if endpoint:
            fanout_specs = [spec for spec in fanout_specs if spec.name == endpoint]
            if not fanout_specs:
                raise ValueError(f"Unknown fanout endpoint: {endpoint}")
        for spec in fanout_specs:
            await self._run_fanout_with_limits(spec, seasons, limit=limit, batch_size=batch_size)
        await self._finalize_summary()
        self._games_from_s3 = False
        self._resume_game_ids = None

    async def _run_single_call(
        self,
        spec,
        params: Dict[str, Any],
        mode: str,
        season: Optional[int] = None,
        date: Optional[str] = None,
    ) -> None:
        if self._missing_required_params(spec, params):
            log_json(self.logger, "skip_missing_params", endpoint=spec.name, params=params)
            return
        payload_hash = stable_hash(params)
        try:
            resp = await self._fetch(spec, params)
            records = _coerce_records(resp)
            await self._write_layers(spec, records, params, payload_hash, mode, season=season, date=date)
            self._tick_progress(spec.name)
        except Exception as exc:
            self._deadletter(spec.name, params, f"error:{exc}")

    async def _run_season_endpoint(self, spec, seasons: List[int], mode: str) -> None:
        season_param = spec.season_param or "season"
        payload_hash = stable_hash({"season_param": season_param})
        checkpoint = self.checkpoints.get(spec.name, payload_hash)
        start_season = seasons[0]
        if mode == "incremental" and checkpoint:
            start_season = max(start_season, int(checkpoint.payload.get("last_completed_season", start_season)))
        for season in seasons:
            if season < start_season:
                continue
            if spec.name == "games":
                await self._run_games_by_chunks(spec, season, payload_hash, mode)
                if mode != "one":
                    self.checkpoints.put(
                        spec.name,
                        payload_hash,
                        {"last_completed_season": season, "last_ingested_date": _today()},
                    )
                continue
            if spec.name == "lines":
                await self._run_lines_by_chunks(spec, season, payload_hash, mode)
                if mode != "one":
                    self.checkpoints.put(
                        spec.name,
                        payload_hash,
                        {"last_completed_season": season, "last_ingested_date": _today()},
                    )
                continue
            if spec.start_date_param and spec.end_date_param:
                await self._run_season_by_chunks(spec, season, payload_hash, mode)
                if mode != "one":
                    self.checkpoints.put(
                        spec.name,
                        payload_hash,
                        {"last_completed_season": season, "last_ingested_date": _today()},
                    )
                continue
            params = {season_param: season}
            if self._missing_required_params(spec, params):
                log_json(self.logger, "skip_missing_params", endpoint=spec.name, params=params)
                continue
            try:
                resp = await self._fetch(spec, params)
                records = _coerce_records(resp)
                await self._write_layers(spec, records, params, payload_hash, mode, season=season)
                if mode != "one":
                    self.checkpoints.put(
                        spec.name,
                        payload_hash,
                        {"last_completed_season": season, "last_ingested_date": _today()},
                    )
            except Exception as exc:
                self._deadletter(spec.name, params, f"error:{exc}")
                continue

    async def _run_date_endpoint(self, spec, mode: str) -> None:
        date_param = spec.date_param or "date"
        window_days = self.config.api.get("rolling_window_days", 7)
        end_date = datetime.utcnow().date()
        start_date = end_date - timedelta(days=window_days)
        for i in range((end_date - start_date).days + 1):
            day = start_date + timedelta(days=i)
            params = {date_param: day.isoformat()}
            payload_hash = stable_hash(params)
            resp = await self._fetch(spec, params)
            records = _coerce_records(resp)
            await self._write_layers(spec, records, params, payload_hash, mode, date=day.isoformat())
            if mode != "one":
                self.checkpoints.put(spec.name, payload_hash, {"last_ingested_date": day.isoformat()})

    async def _run_game_fanout(self, spec, seasons: List[int], mode: str) -> None:
        await self._ensure_game_ids(seasons, mode)
        game_ids = sorted(self._game_ids)
        if self._resume_game_ids:
            game_ids = [gid for gid in game_ids if gid not in self._resume_game_ids]
        log_json(self.logger, "fanout_start", endpoint=spec.name, count=len(game_ids))
        tasks = []
        for game_id in game_ids:
            params = {"gameId": game_id}
            meta = self._game_meta.get(game_id, {})
            tasks.append(self._run_single_call(spec, params, mode, season=meta.get("season"), date=meta.get("date")))
        await _gather(tasks)
        log_json(self.logger, "fanout_done", endpoint=spec.name, count=len(game_ids))

    async def _run_player_fanout(self, spec, seasons: List[int], mode: str) -> None:
        await self._ensure_player_ids(seasons, mode)
        use_pairs = self._fanout_requires_season(spec)
        total = len(self._player_seasons) if use_pairs else len(self._player_ids)
        log_json(self.logger, "fanout_start", endpoint=spec.name, count=total)
        tasks = []
        if use_pairs:
            for player_id, season in sorted(self._player_seasons):
                params = {"playerId": player_id, "season": season}
                tasks.append(self._run_single_call(spec, params, mode, season=season))
        else:
            for player_id in sorted(self._player_ids):
                params = {"playerId": player_id}
                tasks.append(self._run_single_call(spec, params, mode))
        await _gather(tasks)
        log_json(self.logger, "fanout_done", endpoint=spec.name, count=total)

    async def _run_fanout_with_limits(
        self,
        spec,
        seasons: List[int],
        limit: Optional[int],
        batch_size: Optional[int],
    ) -> None:
        if spec.type == "game_fanout":
            await self._ensure_game_ids(seasons, mode="backfill")
            ids = sorted(self._game_ids)
            if self._resume_game_ids:
                ids = [gid for gid in ids if gid not in self._resume_game_ids]
            meta = self._game_meta
            key_name = "gameId"
        elif spec.type == "player_fanout":
            await self._ensure_player_ids(seasons, mode="backfill")
            if self._fanout_requires_season(spec):
                ids = sorted(self._player_seasons)
                meta = {}
                key_name = "playerId"
            else:
                ids = sorted(self._player_ids)
                meta = {}
                key_name = "playerId"
        else:
            raise ValueError("fanout limits only apply to fanout endpoints")

        if limit:
            ids = ids[:limit]

        total = len(ids)
        log_json(self.logger, "fanout_start", endpoint=spec.name, count=total)
        if not batch_size:
            batch_size = total

        for i in range(0, total, batch_size):
            batch = ids[i : i + batch_size]
            log_json(self.logger, "fanout_batch_start", endpoint=spec.name, batch_start=i, batch_size=len(batch), total=total)
            tasks = []
            for _id in batch:
                if spec.type == "player_fanout" and self._fanout_requires_season(spec):
                    player_id, season = _id
                    params = {key_name: player_id, "season": season}
                    tasks.append(self._run_single_call(spec, params, mode="backfill", season=season))
                    continue
                params = {key_name: _id}
                season = meta.get(_id, {}).get("season") if key_name == "gameId" else None
                date = meta.get(_id, {}).get("date") if key_name == "gameId" else None
                tasks.append(self._run_single_call(spec, params, mode="backfill", season=season, date=date))
            await _gather(tasks)
            log_json(self.logger, "fanout_batch_done", endpoint=spec.name, batch_end=i + len(batch), total=total)
        log_json(self.logger, "fanout_done", endpoint=spec.name, count=total)

    async def _ensure_game_ids(self, seasons: List[int], mode: str) -> None:
        if self._game_ids:
            return
        games_spec = self.registry["games"]
        if getattr(self, "_games_from_s3", False):
            self._load_game_ids_from_s3(seasons)
            return
        if mode == "incremental":
            start = datetime.utcnow().date() - timedelta(days=self.config.api.get("rolling_window_days", 7))
            end = datetime.utcnow().date()
            params = {}
            if games_spec.start_date_param and games_spec.end_date_param:
                params[games_spec.start_date_param] = start.isoformat()
                params[games_spec.end_date_param] = end.isoformat()
                resp = await self._fetch(games_spec, params)
                records = _coerce_records(resp)
                self._update_game_meta(records)
                return
        for season in seasons:
            records = await self._fetch_games_for_season(games_spec, season)
            self._update_game_meta(records)

    async def _ensure_player_ids(self, seasons: List[int], mode: str) -> None:
        if self._player_ids:
            return
        players_spec = self.registry["games_players"]
        for season in seasons:
            params = {players_spec.season_param or "season": season}
            resp = await self._fetch(players_spec, params)
            records = _coerce_records(resp)
            for rec in records:
                # games/players returns a list of players per game
                players = rec.get("players") if isinstance(rec, dict) else None
                if isinstance(players, list):
                    for p in players:
                        pid = p.get("playerId") or p.get("athleteId") or p.get("id")
                        if pid is None:
                            continue
                        try:
                            pid_int = int(pid)
                        except Exception:
                            continue
                        self._player_ids.add(pid_int)
                        self._player_seasons.add((pid_int, season))
                    continue
                pid = rec.get("playerId") or rec.get("athleteId") or rec.get("id")
                if pid is None:
                    continue
                try:
                    pid_int = int(pid)
                except Exception:
                    continue
                self._player_ids.add(pid_int)
                self._player_seasons.add((pid_int, season))

    async def _fetch(self, spec, params: Dict[str, Any]) -> Any:
        path = spec.path.format(**params)
        path_params = set(re.findall(r"\{([^}]+)\}", spec.path))
        safe_params = {k: v for k, v in params.items() if k not in path_params}
        return await self.api.get_json(path, params=safe_params)

    async def _write_layers(
        self,
        spec,
        records: List[Dict[str, Any]],
        params: Dict[str, Any],
        payload_hash: str,
        mode: str,
        season: Optional[int] = None,
        date: Optional[str] = None,
    ) -> None:
        layout = self.config.s3_layout
        ingested_at = _today()
        tmp_key = make_part_key(layout["tmp_prefix"], self._run_id, f"{spec.name}-{payload_hash[:8]}.tmp")
        self.s3.put_tmp(tmp_key, b"")
        if spec.name in ("lineups_game", "lineups_team") and records:
            for rec in records:
                if season is not None and rec.get("season") is None and rec.get("year") is None:
                    rec["season"] = season
                if date is not None and rec.get("date") is None:
                    rec["date"] = date
                if spec.name == "lineups_game":
                    game_id = params.get("gameId")
                    if game_id is not None and rec.get("gameId") is None and rec.get("gameid") is None:
                        rec["gameId"] = game_id
        if spec.name == "rankings" and season is not None and records:
            # Endpoint may return all seasons; trim to the requested season for partitioning
            records = [r for r in records if str(r.get("season")) == str(season)]
        raw_endpoint = RAW_PREFIX_OVERRIDES.get(spec.name, spec.name)
        raw_key = make_part_key(
            layout["raw_prefix"],
            raw_endpoint,
            f"ingested_at={ingested_at}",
            f"part-{payload_hash[:8]}.json.gz",
        )
        if records:
            self.s3.put_json_gz(raw_key, records)
        else:
            self._deadletter(spec.name, params, "empty_response")
            self._record_summary(spec.name, 0)
            return

        bronze_table = BRONZE_TABLES[spec.name]
        bronze_partition = _bronze_partition(spec, season=season, date=date, asof=ingested_at)
        bronze_key = make_part_key(
            layout["bronze_prefix"],
            bronze_table,
            bronze_partition,
            f"part-{payload_hash[:8]}.parquet",
        )
        bronze_table_pa = normalize_records(bronze_table, records)
        self.s3.put_parquet(bronze_key, bronze_table_pa)
        self._update_glue("bronze", bronze_table, bronze_table_pa.schema, bronze_partition)

        if spec.name in SILVER_TABLES:
            silver_table = SILVER_TABLES[spec.name]
            if silver_table == "fct_lines":
                records = _expand_lines_records(records)
                if not records:
                    self._record_summary(spec.name, 0)
                    return
            records = _apply_key_aliases(silver_table, records)
            spec_def = TABLE_SPECS.get(silver_table)
            if spec_def:
                _coerce_key_fields(records, spec_def.primary_keys)
                records = self._filter_seen_keys(silver_table, records, spec_def.primary_keys)
                records = dedupe_records(records, spec_def.primary_keys)
            silver_partition = _silver_partition(silver_table, season=season, date=date, asof=ingested_at)
            silver_key = make_part_key(
                layout["silver_prefix"],
                silver_table,
                silver_partition,
                f"part-{payload_hash[:8]}.parquet",
            )
            silver_table_pa = normalize_records(silver_table, records)
            self.s3.put_parquet(silver_key, silver_table_pa)
            self._update_glue("silver", silver_table, silver_table_pa.schema, silver_partition)

        if spec.name in GOLD_TABLES:
            gold_table = GOLD_TABLES[spec.name]
            gold_records = _gold_records(records, ingested_at)
            gold_partition = f"asof={ingested_at}"
            gold_key = make_part_key(
                layout["gold_prefix"],
                gold_table,
                gold_partition,
                f"part-{payload_hash[:8]}.parquet",
            )
            gold_table_pa = normalize_records(gold_table, gold_records)
            self.s3.put_parquet(gold_key, gold_table_pa)

        self._record_summary(spec.name, len(records))

    def _update_glue(self, layer: str, table: str, schema, partition: str) -> None:
        db = f"cbbd_{layer}"
        self.glue.ensure_database(db)
        location = f"s3://{self.config.bucket}/{self.config.s3_layout[layer + '_prefix']}/{table}/"
        partitions = [p.split("=")[0] for p in partition.split("/")]
        # Remove partition columns from schema to avoid duplicate column errors in Glue/Athena
        if partitions:
            for p in partitions:
                if p in schema.names:
                    idx = schema.get_field_index(p)
                    if idx >= 0:
                        schema = schema.remove(idx)
        self.glue.ensure_table(db, table, location, schema, partitions)

    def _record_summary(self, endpoint: str, rows: int) -> None:
        self._summary.setdefault("endpoints", {})[endpoint] = {
            "rows": rows,
            "timestamp": datetime.utcnow().isoformat(),
        }

    def _tick_progress(self, endpoint: str) -> None:
        count = self._request_counts.get(endpoint, 0) + 1
        self._request_counts[endpoint] = count
        if count % self._log_every == 0:
            log_json(self.logger, "endpoint_progress", endpoint=endpoint, requests=count)

    def _filter_seen_keys(
        self, table: str, records: List[Dict[str, Any]], key_fields: Tuple[str, ...]
    ) -> List[Dict[str, Any]]:
        if not key_fields:
            return records
        seen = self._seen_keys.setdefault(table, set())
        out: List[Dict[str, Any]] = []
        for rec in records:
            key = []
            missing = False
            for k in key_fields:
                v = rec.get(k)
                if v is None:
                    missing = True
                    break
                key.append(v)
            if missing:
                out.append(rec)
                continue
            t = tuple(key)
            if t in seen:
                continue
            seen.add(t)
            out.append(rec)
        return out

    async def _finalize_summary(self) -> None:
        self._summary["finished_at"] = datetime.utcnow().isoformat()
        key = make_part_key(self.config.s3_layout["meta_prefix"], f"run_id={self._run_id}.json")
        self.s3.put_deadletter(key, self._summary)

    def _deadletter(self, endpoint: str, params: Dict[str, Any], reason: str) -> None:
        key = make_part_key(
            self.config.s3_layout["deadletter_prefix"],
            endpoint,
            f"ingested_at={_today()}",
            f"part-{stable_hash(params)[:8]}.json",
        )
        self.s3.put_deadletter(key, {"reason": reason, "params": params, "endpoint": endpoint})

    def _update_game_meta(self, records: List[Dict[str, Any]]) -> None:
        for rec in records:
            game_id = rec.get("id") or rec.get("gameId")
            if game_id is None:
                continue
            try:
                game_id = int(game_id)
            except Exception:
                continue
            self._game_ids.add(game_id)
            meta = self._game_meta.setdefault(game_id, {})
            season = rec.get("season") or rec.get("year")
            date = rec.get("date") or rec.get("startDate") or rec.get("startTime")
            if season is not None:
                try:
                    meta["season"] = int(season)
                except Exception:
                    pass
            if date is not None:
                meta["date"] = str(date)[:10]

    def _load_game_ids_from_s3(self, seasons: List[int]) -> None:
        import pyarrow.parquet as pq
        import pyarrow as pa
        import io

        silver_prefix = self.config.s3_layout["silver_prefix"]
        table_prefix = f"{silver_prefix}/fct_games/"
        loaded = 0
        for season in seasons:
            prefix = f"{table_prefix}season={season}/"
            keys = self.s3.list_keys(prefix)
            if not keys:
                continue
            for key in keys:
                if not key.endswith(".parquet"):
                    continue
                try:
                    data = self.s3.get_object_bytes(key)
                    table = pq.read_table(io.BytesIO(data))
                except Exception:
                    continue
                cols = set(table.schema.names)
                if "gameId" in cols:
                    game_ids = table.column("gameId").to_pylist()
                elif "id" in cols:
                    game_ids = table.column("id").to_pylist()
                else:
                    continue
                # attempt to get season/date meta if available
                season_vals = table.column("season").to_pylist() if "season" in cols else None
                date_col = None
                for dc in ("date", "startDate", "startTime"):
                    if dc in cols:
                        date_col = table.column(dc).to_pylist()
                        break
                for idx, gid in enumerate(game_ids):
                    if gid is None:
                        continue
                    try:
                        gid_int = int(gid)
                    except Exception:
                        continue
                    self._game_ids.add(gid_int)
                    loaded += 1
                    meta = self._game_meta.setdefault(gid_int, {})
                    if season_vals is not None:
                        try:
                            meta["season"] = int(season_vals[idx])
                        except Exception:
                            pass
                    if date_col is not None and idx < len(date_col):
                        meta["date"] = str(date_col[idx])[:10]

        if loaded == 0:
            self._load_game_ids_from_bronze(seasons)

    def _load_game_ids_from_bronze(self, seasons: List[int]) -> None:
        import pyarrow.parquet as pq
        import io

        bronze_prefix = self.config.s3_layout["bronze_prefix"]
        table_prefix = f"{bronze_prefix}/games/"
        for season in seasons:
            prefix = f"{table_prefix}season={season}/"
            keys = self.s3.list_keys(prefix)
            if not keys:
                continue
            for key in keys:
                if not key.endswith(".parquet"):
                    continue
                try:
                    data = self.s3.get_object_bytes(key)
                    table = pq.read_table(io.BytesIO(data))
                except Exception:
                    continue
                cols = set(table.schema.names)
                if "gameId" in cols:
                    game_ids = table.column("gameId").to_pylist()
                elif "id" in cols:
                    game_ids = table.column("id").to_pylist()
                else:
                    continue
                season_vals = table.column("season").to_pylist() if "season" in cols else None
                date_col = None
                for dc in ("date", "startDate", "startTime"):
                    if dc in cols:
                        date_col = table.column(dc).to_pylist()
                        break
                for idx, gid in enumerate(game_ids):
                    if gid is None:
                        continue
                    try:
                        gid_int = int(gid)
                    except Exception:
                        continue
                    self._game_ids.add(gid_int)
                    meta = self._game_meta.setdefault(gid_int, {})
                    if season_vals is not None:
                        try:
                            meta["season"] = int(season_vals[idx])
                        except Exception:
                            pass
                    if date_col is not None and idx < len(date_col):
                        meta["date"] = str(date_col[idx])[:10]

    async def _run_games_by_chunks(self, spec, season: int, payload_hash: str, mode: str) -> None:
        season_param = spec.season_param or "season"
        start_date, end_date = self._season_window(season)
        chunk_days = int(self.config.api.get("games_chunk_days", 30))
        for start, end in _date_chunks(start_date, end_date, chunk_days):
            params = {
                season_param: season,
                spec.start_date_param: _iso_start(start),
                spec.end_date_param: _iso_end(end),
            }
            if self._missing_required_params(spec, params):
                log_json(self.logger, "skip_missing_params", endpoint=spec.name, params=params)
                continue
            try:
                resp = await self._fetch(spec, params)
                records = _coerce_records(resp)
                chunk_hash = stable_hash(params)
                await self._write_layers(spec, records, params, chunk_hash, mode, season=season)
            except Exception as exc:
                self._deadletter(spec.name, params, f"error:{exc}")
                continue

    async def _fetch_games_for_season(self, spec, season: int) -> List[Dict[str, Any]]:
        season_param = spec.season_param or "season"
        start_date, end_date = self._season_window(season)
        chunk_days = int(self.config.api.get("games_chunk_days", 30))
        all_records: List[Dict[str, Any]] = []
        for start, end in _date_chunks(start_date, end_date, chunk_days):
            params = {
                season_param: season,
                spec.start_date_param: _iso_start(start),
                spec.end_date_param: _iso_end(end),
            }
            resp = await self._fetch(spec, params)
            records = _coerce_records(resp)
            all_records.extend(records)
        return all_records

    async def _run_lines_by_chunks(self, spec, season: int, payload_hash: str, mode: str) -> None:
        season_param = spec.season_param or "season"
        start_date, end_date = self._season_window(season)
        chunk_days = int(self.config.api.get("lines_chunk_days", 30))
        for start, end in _date_chunks(start_date, end_date, chunk_days):
            params = {
                season_param: season,
                spec.start_date_param: _iso_start(start),
                spec.end_date_param: _iso_end(end),
            }
            if self._missing_required_params(spec, params):
                log_json(self.logger, "skip_missing_params", endpoint=spec.name, params=params)
                continue
            try:
                resp = await self._fetch(spec, params)
                records = _coerce_records(resp)
                chunk_hash = stable_hash(params)
                await self._write_layers(spec, records, params, chunk_hash, mode, season=season)
            except Exception as exc:
                self._deadletter(spec.name, params, f"error:{exc}")
                continue

    async def _run_season_by_chunks(self, spec, season: int, payload_hash: str, mode: str) -> None:
        season_param = spec.season_param or "season"
        start_date, end_date = self._season_window(season)
        chunk_days = int(self.config.api.get("games_chunk_days", 30))
        for start, end in _date_chunks(start_date, end_date, chunk_days):
            params = {
                season_param: season,
                spec.start_date_param: _iso_start(start),
                spec.end_date_param: _iso_end(end),
            }
            if self._missing_required_params(spec, params):
                log_json(self.logger, "skip_missing_params", endpoint=spec.name, params=params)
                continue
            try:
                resp = await self._fetch(spec, params)
                records = _coerce_records(resp)
                chunk_hash = stable_hash(params)
                await self._write_layers(spec, records, params, chunk_hash, mode, season=season)
            except Exception as exc:
                self._deadletter(spec.name, params, f"error:{exc}")
                continue

    def _season_window(self, season: int) -> tuple[date, date]:
        start_month = int(self.config.seasons.get("window_start_month", 8))
        start_day = int(self.config.seasons.get("window_start_day", 1))
        end_month = int(self.config.seasons.get("window_end_month", 7))
        end_day = int(self.config.seasons.get("window_end_day", 31))
        start = date(season - 1, start_month, start_day)
        end = date(season, end_month, end_day)
        return start, end

    def _is_skipped(self, endpoint: str) -> bool:
        return endpoint in self.config.raw.get("skip_endpoints", [])

    def _missing_required_params(self, spec, params: Dict[str, Any]) -> bool:
        required = self.config.endpoints.get(spec.name, {}).get("required_params", [])
        if any(p not in params for p in required):
            return True
        required_any = self.config.endpoints.get(spec.name, {}).get("required_any", [])
        for group in required_any:
            if not any(p in params for p in group):
                return True
        return False

    def _fanout_requires_season(self, spec) -> bool:
        required = self.config.endpoints.get(spec.name, {}).get("required_params", [])
        return "season" in required

    def _load_resume_ids(self, resume_file: str) -> set[int]:
        resume_ids: set[int] = set()
        try:
            with open(resume_file, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith("#"):
                        continue
                    if line.isdigit():
                        resume_ids.add(int(line))
        except FileNotFoundError:
            pass
        return resume_ids


async def _gather(tasks: Iterable[asyncio.Future]) -> None:
    if not tasks:
        return
    await asyncio.gather(*tasks)


def _coerce_records(resp: Any) -> List[Dict[str, Any]]:
    if resp is None:
        return []
    if isinstance(resp, list):
        return resp
    if isinstance(resp, dict):
        if "data" in resp and isinstance(resp["data"], list):
            return resp["data"]
        return [resp]
    return []


def _extract_ids(records: List[Dict[str, Any]], keys: List[str]) -> Set[int]:
    ids: Set[int] = set()
    for rec in records:
        for k in keys:
            val = rec.get(k)
            if val is None:
                continue
            try:
                ids.add(int(val))
                break
            except Exception:
                continue
    return ids


def _bronze_partition(spec, season: Optional[int], date: Optional[str], asof: str) -> str:
    if spec.type == "snapshot":
        return f"asof={asof}"
    if spec.type == "season":
        return f"season={season}/asof={asof}"
    if spec.type in ("game_fanout",):
        return f"season={season or 'unknown'}/date={date or asof}"
    if spec.type == "date":
        return f"season={season or date[:4]}/date={date}"
    return f"asof={asof}"


def _silver_partition(table: str, season: Optional[int], date: Optional[str], asof: str) -> str:
    if table.startswith("dim_"):
        return f"asof={asof}"
    if date:
        return f"season={season or date[:4]}/date={date}"
    if season:
        return f"season={season}"
    return f"asof={asof}"


def _gold_records(records: List[Dict[str, Any]], asof: str) -> List[Dict[str, Any]]:
    out = []
    for rec in records:
        item = dict(rec)
        item["asof"] = asof
        out.append(item)
    return out


def _apply_key_aliases(table: str, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not records:
        return records
    if table == "fct_games":
        for rec in records:
            if rec.get("gameId") is None and rec.get("id") is not None:
                rec["gameId"] = rec.get("id")
            if rec.get("gameId") is None and rec.get("gameid") is not None:
                rec["gameId"] = rec.get("gameid")
    if table == "dim_teams":
        for rec in records:
            if rec.get("teamId") is None and rec.get("id") is not None:
                rec["teamId"] = rec.get("id")
            if rec.get("teamId") is None and rec.get("teamid") is not None:
                rec["teamId"] = rec.get("teamid")
    if table == "fct_game_teams":
        for rec in records:
            if rec.get("gameId") is None and rec.get("gameid") is not None:
                rec["gameId"] = rec.get("gameid")
            if rec.get("teamId") is None and rec.get("teamid") is not None:
                rec["teamId"] = rec.get("teamid")
            if rec.get("isHome") is None and rec.get("ishome") is not None:
                rec["isHome"] = rec.get("ishome")
            rec["teamStats"] = _normalize_jsonish(rec.get("teamStats"))
            rec["opponentStats"] = _normalize_jsonish(rec.get("opponentStats"))
    if table == "fct_game_players":
        for rec in records:
            if rec.get("gameId") is None and rec.get("gameid") is not None:
                rec["gameId"] = rec.get("gameid")
            if rec.get("playerId") is None and rec.get("playerid") is not None:
                rec["playerId"] = rec.get("playerid")
            rec["players"] = _normalize_jsonish(rec.get("players"))
    if table == "fct_game_media":
        for rec in records:
            if rec.get("gameId") is None and rec.get("gameid") is not None:
                rec["gameId"] = rec.get("gameid")
            rec["broadcasts"] = _normalize_jsonish(rec.get("broadcasts"))
    if table == "fct_plays":
        for rec in records:
            if rec.get("gameId") is None and rec.get("gameid") is not None:
                rec["gameId"] = rec.get("gameid")
            onfloor_raw = rec.get("onFloor") if rec.get("onFloor") is not None else rec.get("onfloor")
            participants_raw = rec.get("participants")
            shotinfo_raw = rec.get("shotInfo") if rec.get("shotInfo") is not None else rec.get("shotinfo")

            onfloor = _parse_jsonish(onfloor_raw)
            participants = _parse_jsonish(participants_raw)
            shotinfo = _parse_jsonish(shotinfo_raw)

            rec["onFloor"] = _normalize_jsonish(onfloor_raw)
            rec["participants"] = _normalize_jsonish(participants_raw)
            rec["shotInfo"] = _normalize_jsonish(shotinfo_raw)

            if isinstance(onfloor, list):
                ids = [_to_int(p.get("id")) for p in onfloor if isinstance(p, dict)]
                for idx in range(10):
                    rec[f"onfloor_player{idx+1}"] = ids[idx] if idx < len(ids) else None
            if isinstance(participants, list):
                rec["participant_id"] = _to_int(participants[0].get("id")) if participants and isinstance(participants[0], dict) else None
            if isinstance(shotinfo, dict):
                shooter = shotinfo.get("shooter") if isinstance(shotinfo.get("shooter"), dict) else {}
                assisted_by = shotinfo.get("assistedBy") if isinstance(shotinfo.get("assistedBy"), dict) else {}
                location = shotinfo.get("location") if isinstance(shotinfo.get("location"), dict) else {}
                rec["shot_shooter_id"] = _to_int(shooter.get("id"))
                rec["shot_shooter_name"] = shooter.get("name")
                rec["shot_made"] = _to_bool(shotinfo.get("made"))
                rec["shot_range"] = shotinfo.get("range")
                rec["shot_assisted"] = _to_bool(shotinfo.get("assisted"))
                rec["shot_assisted_by_id"] = _to_int(assisted_by.get("id"))
                rec["shot_assisted_by_name"] = assisted_by.get("name")
                rec["shot_loc_x"] = _to_float(location.get("x"))
                rec["shot_loc_y"] = _to_float(location.get("y"))
    if table == "fct_lines":
        for rec in records:
            if rec.get("gameId") is None and rec.get("gameid") is not None:
                rec["gameId"] = rec.get("gameid")
    if table == "fct_lineups":
        for rec in records:
            if rec.get("teamid") is None and rec.get("teamId") is not None:
                rec["teamid"] = rec.get("teamId")
            if rec.get("teamId") is None and rec.get("teamid") is not None:
                rec["teamId"] = rec.get("teamid")
            if rec.get("idhash") is None and rec.get("idHash") is not None:
                rec["idhash"] = rec.get("idHash")
            if rec.get("idHash") is None and rec.get("idhash") is not None:
                rec["idHash"] = rec.get("idhash")
            if rec.get("totalseconds") is None and rec.get("totalSeconds") is not None:
                rec["totalseconds"] = rec.get("totalSeconds")
            if rec.get("totalSeconds") is None and rec.get("totalseconds") is not None:
                rec["totalSeconds"] = rec.get("totalseconds")

            if rec.get("defenserating") is None and rec.get("defenseRating") is not None:
                rec["defenserating"] = rec.get("defenseRating")
            if rec.get("netrating") is None and rec.get("netRating") is not None:
                rec["netrating"] = rec.get("netRating")
            if rec.get("offenserating") is None and rec.get("offenseRating") is not None:
                rec["offenserating"] = rec.get("offenseRating")

            if rec.get("opponentstats") is None and rec.get("opponentStats") is not None:
                rec["opponentstats"] = _normalize_jsonish(rec.get("opponentStats"))
            else:
                rec["opponentstats"] = _normalize_jsonish(rec.get("opponentstats"))

            if rec.get("teamstats") is None and rec.get("teamStats") is not None:
                rec["teamstats"] = _normalize_jsonish(rec.get("teamStats"))
            else:
                rec["teamstats"] = _normalize_jsonish(rec.get("teamstats"))

            rec["athletes"] = _normalize_jsonish(rec.get("athletes"))
            # drop camelCase keys to avoid case-insensitive schema clashes
            rec.pop("teamId", None)
            rec.pop("idHash", None)
            rec.pop("totalSeconds", None)
            rec.pop("defenseRating", None)
            rec.pop("netRating", None)
            rec.pop("offenseRating", None)
            rec.pop("opponentStats", None)
            rec.pop("teamStats", None)
    if table == "fct_player_season_stats":
        for rec in records:
            if rec.get("playerId") is None and rec.get("athleteId") is not None:
                rec["playerId"] = rec["athleteId"]
    if table == "fct_player_season_shooting":
        for rec in records:
            if rec.get("playerId") is None and rec.get("athleteId") is not None:
                rec["playerId"] = rec["athleteId"]
    if table == "fct_recruiting_players":
        for rec in records:
            if rec.get("playerId") is None:
                rec["playerId"] = rec.get("id") or rec.get("athleteId")
            if rec.get("season") is None and rec.get("year") is not None:
                rec["season"] = rec["year"]
    if table == "fct_ratings_adjusted":
        for rec in records:
            if rec.get("teamid") is None and rec.get("teamId") is not None:
                rec["teamid"] = rec.get("teamId")
            if rec.get("teamId") is None and rec.get("teamid") is not None:
                rec["teamId"] = rec.get("teamid")
            if rec.get("offenseRating") is not None and rec.get("offenserating") is None:
                rec["offenserating"] = rec.get("offenseRating")
            if rec.get("defenseRating") is not None and rec.get("defenserating") is None:
                rec["defenserating"] = rec.get("defenseRating")
            if rec.get("netRating") is not None and rec.get("netrating") is None:
                rec["netrating"] = rec.get("netRating")
            rankings = rec.get("rankings")
            if isinstance(rankings, dict):
                rec["ranking_offense"] = _to_int(rankings.get("offense"))
                rec["ranking_defense"] = _to_int(rankings.get("defense"))
                rec["ranking_net"] = _to_int(rankings.get("net"))
            # drop camelCase keys to avoid case-insensitive schema clashes
            rec.pop("rankings", None)
            rec.pop("teamId", None)
            rec.pop("offenseRating", None)
            rec.pop("defenseRating", None)
            rec.pop("netRating", None)
    return records


def _expand_lines_records(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    expanded: List[Dict[str, Any]] = []
    for rec in records:
        lines = rec.get("lines")
        if not isinstance(lines, list) or not lines:
            continue
        base = {k: v for k, v in rec.items() if k != "lines"}
        for line in lines:
            if not isinstance(line, dict):
                continue
            provider = line.get("provider") or line.get("providerName") or line.get("source")
            if not provider:
                continue
            row = dict(base)
            row.update(line)
            row["provider"] = provider
            expanded.append(row)
    return expanded


def _normalize_jsonish(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (list, dict)):
        return json.dumps(value)
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return value
        if raw in ("None", "null"):
            return None
        if raw[0] in "[{":
            if "'" in raw or "None" in raw or "True" in raw or "False" in raw:
                try:
                    obj = ast.literal_eval(raw)
                    return json.dumps(obj)
                except Exception:
                    return value
        return value
    return value


def _parse_jsonish(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (list, dict)):
        return value
    if isinstance(value, str):
        raw = value.strip()
        if not raw or raw in ("None", "null"):
            return None
        if raw[0] in "[{":
            try:
                return json.loads(raw)
            except Exception:
                try:
                    return ast.literal_eval(raw)
                except Exception:
                    return None
    return None


def _to_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        try:
            return int(value)
        except Exception:
            return None
    if isinstance(value, str):
        try:
            if value.isdigit():
                return int(value)
            return int(float(value))
        except Exception:
            return None
    return None


def _to_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except Exception:
            return None
    return None


def _to_bool(value: Any) -> Optional[bool]:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.lower() in ("true", "1", "yes")
    return bool(value)


def _coerce_key_fields(records: List[Dict[str, Any]], key_fields: Tuple[str, ...]) -> None:
    for rec in records:
        for k in key_fields:
            if k not in rec:
                continue
            v = rec.get(k)
            if v is None:
                continue
            if isinstance(v, bool):
                continue
            if isinstance(v, int):
                continue
            if isinstance(v, float):
                try:
                    rec[k] = int(v)
                except Exception:
                    continue
                continue
            if isinstance(v, str):
                try:
                    if v.isdigit():
                        rec[k] = int(v)
                    else:
                        rec[k] = int(float(v))
                except Exception:
                    continue


def _date_chunks(start: date, end: date, chunk_days: int) -> List[tuple[date, date]]:
    chunks = []
    current = start
    while current <= end:
        chunk_end = min(end, current + timedelta(days=chunk_days - 1))
        chunks.append((current, chunk_end))
        current = chunk_end + timedelta(days=1)
    return chunks


def _iso_start(d: date) -> str:
    return datetime(d.year, d.month, d.day, 0, 0, 0).isoformat() + "Z"


def _iso_end(d: date) -> str:
    return datetime(d.year, d.month, d.day, 23, 59, 59).isoformat() + "Z"
