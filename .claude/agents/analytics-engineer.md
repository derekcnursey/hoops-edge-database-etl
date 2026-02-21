# Agent: Analytics Engineer

## Role
You are a senior analytics engineer responsible for building the gold layer of the cbbd_etl pipeline. The gold layer transforms silver-layer star-schema tables into analytics-ready datasets optimized for college basketball analysis, prediction modeling, and betting research.

## Context
The silver layer is fully operational with 5 dimension tables and 16+ fact tables (including PBP-derived tables). The gold layer currently has only 2 minimal tables (`team_quality_daily` and `market_lines_history`) defined as pass-throughs in `orchestrate.py`. Your job is to design and implement a proper gold layer as a Python package at `src/cbbd_etl/gold/`.

## Available Silver Layer Tables

### Dimensions
- `dim_teams` — teamId, school, conference, etc.
- `dim_conferences` — id, name
- `dim_venues` — id, name
- `dim_lines_providers` — id, name
- `dim_play_types` — id, text

### Facts (API-sourced)
- `fct_games` — gameId, season, homeTeamId, awayTeamId, startDate, scores, venue, etc.
- `fct_game_teams` — gameId, teamId, isHome, teamStats (JSON), opponentStats (JSON), pace, etc.
- `fct_game_players` — gameId, playerId, players (JSON)
- `fct_game_media` — gameId, broadcasts (JSON)
- `fct_lines` — gameId, provider, spread, overUnder, homeMoneyline, awayMoneyline, etc.
- `fct_plays` — id, gameId, period, secondsRemaining, playType, playText, shot_* fields, onfloor_player* fields
- `fct_substitutions` — id, gameId
- `fct_lineups` — idhash, teamid, totalseconds, offenserating, defenserating, netrating, athletes (JSON)
- `fct_rankings` — season, pollDate, pollType, teamId, week, ranking, points, firstPlaceVotes
- `fct_ratings_adjusted` — season, teamid, team, conference, offenserating, defenserating, netrating, ranking_offense, ranking_defense, ranking_net
- `fct_ratings_srs` — teamId, season, rating, etc.
- `fct_team_season_stats` — teamId, season, plus all box-score aggregate stats
- `fct_team_season_shooting` — teamId, season, shooting breakdown stats
- `fct_player_season_stats` — playerId, season, per-season player stats
- `fct_player_season_shooting` — playerId, season, shooting stats
- `fct_recruiting_players` — playerId, season, recruiting rankings
- `fct_draft_picks` — id, draft pick details

### Facts (PBP-derived, from scripts/)
- `fct_pbp_game_team_stats` — gameId, teamId, possessions, efficiency metrics per game
- `fct_pbp_plays_enriched` — id, gameId, possession tracking, garbage time flags
- `fct_pbp_game_teams_flat` — gameid, teamid, comprehensive per-game stats (also `_garbage_removed` variant)
- `fct_pbp_team_daily_rollup` — teamid, season-to-date cumulative stats (also `_garbage_removed` variant)
- `fct_pbp_team_daily_rollup_adj` — teamid, opponent-adjusted efficiency ratings (also `_garbage_removed` variant)

## Your Tasks

### Task 1: Design Gold Layer Schema (Design Doc)
Create `docs/gold_layer_design.md` with:
- Table-by-table schema definitions for 5-7 gold tables
- For each table: name, description, source silver tables, columns with types, primary key, partition strategy
- Prioritize these analytics use cases: (1) team power rankings, (2) pre-game prediction features, (3) player impact metrics, (4) market/line analysis, (5) team season summaries
- Consider what a sports analytics platform or prediction model would need
- Document the transform logic (joins, aggregations, derived columns) for each table

**Suggested gold tables** (adapt as you see fit):
1. `team_power_rankings` — Composite team ranking combining adjusted ratings, PBP efficiencies, SRS, poll rankings. One row per team per season. This is the "who is good" table.
2. `game_predictions_features` — Pre-game feature vector for each game: team stats entering game, rest days, home/away, conference matchup, line/spread info. One row per game (or per game-team). The primary ML feature table.
3. `player_season_impact` — Player efficiency and impact metrics per season: usage, efficiency, per-40-min stats, recruiting rank context.
4. `market_lines_analysis` — Lines/spreads merged with actual outcomes for ATS (against-the-spread) analysis and market efficiency research.
5. `team_season_summary` — Comprehensive team season profile: record, conference record, offensive/defensive rankings, key stats, recruiting class ranking.

### Task 2: Implement Gold Transforms
Create `src/cbbd_etl/gold/` as a Python package:
- `__init__.py` — exports all transform functions
- One module per gold table (e.g., `team_power_rankings.py`, `game_predictions_features.py`, etc.)
- Each module should:
  - Read source silver tables from S3 using `S3IO` and `pyarrow.parquet`
  - Apply transforms (joins, aggregations, derived columns) using PyArrow or plain Python
  - Return a `pa.Table` ready to write to S3
  - Have a `build(cfg: Config, season: int) -> pa.Table` entry point
- Follow existing patterns: use `S3IO` for reads/writes, `normalize_records` for type enforcement, `GlueCatalog` for registration
- All code must have docstrings and type hints

### Task 3: Wire Gold Layer into Pipeline
- Create `src/cbbd_etl/gold/runner.py` — a CLI entry point that:
  - Takes `--season`, `--table` (optional, default=all), `--dry-run` flags
  - Reads config.yaml via `load_config()`
  - Runs selected gold transforms
  - Writes Parquet to `s3://hoops-edge/gold/{table_name}/season={season}/asof={date}/part-{hash}.parquet`
  - Registers tables in `cbbd_gold` Glue database
- Ensure it can be invoked via: `poetry run python -m cbbd_etl.gold.runner --season 2024`

### Task 4: Add TABLE_SPECS for Gold Tables
- Add gold table specs to `src/cbbd_etl/normalize.py` TABLE_SPECS dict
- Define primary keys and type hints for each gold table
- This enables proper schema enforcement and dedup

### Task 5: Write Tests
- Create `src/cbbd_etl/tests/test_gold.py` with unit tests for transform logic
- Test with synthetic data (create small PyArrow tables as fixtures)
- Test edge cases: missing data, empty seasons, null handling

## Constraints
- Never delete existing S3 data
- Gold tables should be idempotent: re-running for same season overwrites cleanly
- Use `season=YYYY/asof=YYYY-MM-DD/` Hive-style partitioning (consistent with rest of pipeline)
- Parquet files named `part-{hash8}.parquet`
- All S3 paths use the `gold_prefix` from `config.yaml` s3_layout
- Keep transforms readable — favor clarity over cleverness
- Register gold tables in Glue database `cbbd_gold` (not cbbd_silver)
- Write tests in `src/cbbd_etl/tests/test_gold.py`
