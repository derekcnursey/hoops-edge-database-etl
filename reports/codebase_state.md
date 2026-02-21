# Codebase State — Session 1 Survey

## What Exists

### Core Pipeline (`src/cbbd_etl/`)
- **orchestrate.py** — Full pipeline orchestrator with backfill, incremental, single-endpoint, and fanout modes. Writes raw (gzip JSON), bronze (Parquet), silver (star schema Parquet), and gold layers. Handles 30 endpoints with DynamoDB checkpointing.
- **api_client.py** — Async httpx client with rate limiting (token bucket), concurrency semaphore, and exponential backoff retries.
- **checkpoint.py** — DynamoDB-backed checkpoint store keyed by (endpoint, parameter_hash).
- **config.py** — YAML config loader.
- **normalize.py** — TABLE_SPECS with primary keys and type hints for all silver tables. `normalize_records()` builds typed PyArrow tables. `dedupe_records()` does in-memory primary key dedup.
- **s3_io.py** — S3 read/write/list/delete with retry logic. Parquet I/O via PyArrow.
- **glue_catalog.py** — Glue database/table management.
- **pbp_stats.py** — Play-by-play classifier using regex patterns from YAML config. Identifies turnovers, rebounds, shots, FTs, possession ends.
- **logging_utils.py** — Structured JSON logging.
- **utils.py** — `stable_hash()` helper.
- **extractors/** — 30 endpoint-specific extractor modules (one per API endpoint).

### Silver Layer Tables (TABLE_SPECS)
- **Dimensions (5):** dim_teams, dim_conferences, dim_venues, dim_lines_providers, dim_play_types
- **Facts (16):** fct_games, fct_game_media, fct_lines, fct_game_teams, fct_game_players, fct_plays, fct_substitutions, fct_lineups, fct_rankings, fct_ratings_adjusted, fct_ratings_srs, fct_team_season_stats, fct_team_season_shooting, fct_player_season_stats, fct_player_season_shooting, fct_recruiting_players, fct_draft_picks
- **PBP-derived (7):** fct_pbp_game_team_stats, fct_pbp_plays_enriched, fct_pbp_game_teams_flat, fct_pbp_game_teams_flat_garbage_removed, fct_pbp_team_daily_rollup, fct_pbp_team_daily_rollup_garbage_removed, fct_pbp_team_daily_rollup_adj, fct_pbp_team_daily_rollup_adj_garbage_removed

### Gold Layer
- **GOLD_TABLES mapping:** ratings_adjusted → team_quality_daily, lines → market_lines_history
- Gold is minimal — only 2 tables defined in orchestrator.

### Scripts (15 files)
- **backfill_missing_plays.py** — Athena-driven gap-fill for plays_game via API
- **verify_missing_plays_api.py** — Verify plays availability against API
- **rebuild_from_raw.py** — Rebuild bronze/silver from raw JSON.gz
- **normalize_fct_games_partitions.py** — Fix season/date → season/asof partition layout
- **build_pbp_game_team_stats.py** — Game-team advanced stats from fct_plays
- **build_pbp_plays_enriched.py** — Enriched PBP with possession tracking, garbage time flags
- **build_pbp_game_teams_flat.py** — Flat game-team stats from enriched PBP
- **build_pbp_team_daily_rollup.py** — Cumulative daily team rollups
- **build_pbp_team_daily_rollup_adj.py** — Adjusted ratings via iterative opponent-adjustment
- **run_games_seasons.py** — Season-by-season games backfill with resume
- **check_lineups.py** — Inspect lineups data across layers
- **extract_lineups_game_ids.py** — Extract gameIds from raw lineups
- **list_non_empty_raw_parts.py** — Filter non-empty raw parts
- **delete_fct_games_asof.py** — Delete fct_games partitions by asof
- **update_required_params.py** — Parse API docs for required params, update config

### Tests (4 files)
- test_checkpoint.py — DynamoDB stub-based checkpoint test
- test_dedupe.py — Basic dedup test
- test_normalize.py — Type casting verification
- test_partitions.py — Partition string generation tests

### Infrastructure
- **Terraform:** DynamoDB checkpoints table, Glue databases (cbbd_bronze, cbbd_silver), Glue tables (placeholder schemas), Athena workgroup, IAM role/policy for ECS.
- **Config:** 30 endpoints covering conferences, draft, games, lineups, plays, substitutions, rankings, ratings, recruiting, stats, teams, venues. Seasons 2010-2026.

### Makefile
- `make backfill`, `make incremental`, `make validate`, `make one`

## What's Missing / Gaps

### Agent Instructions
- **`.claude/agents/data-engineer.md` does not exist.** Cannot dispatch data engineer subagent without it.

### Data Completeness (unknown without S3 access)
- No local audit of which seasons/endpoints have complete data
- No data quality report exists
- Dead letter queue contents unknown

### Testing
- Only 4 unit tests — no integration tests, no tests for PBP pipeline scripts
- No tests for api_client, s3_io, glue_catalog, orchestrate main flows
- No test for pbp_stats.PlayClassifier

### Gold Layer
- Only 2 gold tables defined (team_quality_daily, market_lines_history)
- No gold-layer scripts or sophisticated analytics tables
- No team power rankings, player efficiency, or predictive features in gold

### Missing Makefile targets
- No `make gap-fill` for plays backfill
- No `make pbp-pipeline` for the full PBP stats chain
- No `make test` target

### Code Quality
- `build_pbp_game_team_stats.py:370-371` has stray code after `if __name__` block (syntax artifact)
- Significant code duplication across PBP scripts (_to_int, _to_float, _load_table, _get_col, etc.)
- Scripts use `_load_env()` independently rather than sharing from config module

### Documentation
- CLAUDE.md is solid but references `extractors.py` (singular) while actual code is `extractors/` (package with 30 modules)
- No data dictionary or schema documentation
