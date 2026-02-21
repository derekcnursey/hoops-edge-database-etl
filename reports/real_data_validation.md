# Real Data Validation Report

Generated: 2026-02-21
Test Season: 2025 (most complete season available)

## Summary

All 5 gold transforms now build successfully against real S3 data. 134 tests pass (127 original + 7 new).

| Gold Table | Status | Rows (season 2025) | PK Duplicates |
|------------|--------|---------------------|---------------|
| team_power_rankings | PASS | 364 | 0 |
| game_predictions_features | PASS | 12,598 | 0 |
| player_season_impact | PASS (empty) | 0 | N/A |
| market_lines_analysis | PASS | 5,440 | 0 |
| team_season_summary | PASS | 701 | 0 |

## Bugs Found and Fixed

### Bug 1: Column name mismatch — offensiveRating/defensiveRating
- **File**: `_io_helpers.py`, `team_season_summary.py`, `team_power_rankings.py`, `game_predictions_features.py`
- **Problem**: Code expected `offenserating`/`defenserating` but real data has `offensiveRating`/`defensiveRating`
- **Impact**: All adjusted ratings were silently null (pydict_get returned [None]*N)
- **Fix**: Changed to `pydict_get_first(table, ["offenserating", "offensiveRating"])` — tries both names
- **Tested**: Both mock (old names) and real (new names) patterns verified

### Bug 2: Column name mismatch — homePoints/awayPoints
- **File**: `team_season_summary.py`, `game_predictions_features.py`, `market_lines_analysis.py`
- **Problem**: Code expected `homeScore`/`awayScore` but real data has `homePoints`/`awayPoints`
- **Impact**: All win/loss records were 0, all scores null
- **Fix**: Changed to `pydict_get_first(games, ["homeScore", "homePoints"])`
- **Tested**: Duke 2025 shows 35-4 record (verified correct)

### Bug 3: fct_ratings_srs has no season partition
- **File**: `team_season_summary.py`, `team_power_rankings.py`, `game_predictions_features.py`
- **Problem**: `read_silver_table(..., season=2025)` looked for `silver/fct_ratings_srs/season=2025/` but data is at `silver/fct_ratings_srs/asof=2026-01-28/`
- **Impact**: SRS ratings were always empty (0 rows returned)
- **Fix**: Added `filter_by_season()` helper. Changed to `filter_by_season(read_silver_table(s3, cfg, "fct_ratings_srs"), season)` — reads all data then filters by season column in memory
- **Tested**: SRS ratings now range -22.9 to 22.2 for season 2025

### Bug 4: Schema type conflicts across Parquet files
- **File**: `_io_helpers.py`
- **Problem**: `pa.concat_tables` failed when same column had `string` in one file and `double` in another (e.g., `overUnderOpen`, `excitement`)
- **Impact**: `market_lines_analysis` and `team_season_summary` builds crashed with `ArrowTypeError`
- **Fix**: Added `(pa.ArrowInvalid, pa.ArrowTypeError)` exception handling with `_concat_with_unified_schema()` fallback that unifies types before concat
- **Tested**: Both transforms now build successfully

### Bug 5: Duplicate records from overlapping pipeline runs
- **File**: `team_season_summary.py`, `game_predictions_features.py`, `market_lines_analysis.py`
- **Problem**: 3,000 duplicate gameIds in fct_games (season 2025) from overlapping pipeline ingestion runs
- **Impact**: `game_predictions_features` had 6,000 duplicate PK rows; `team_season_summary` had inflated win/loss counts
- **Fix**: Added `dedup_by()` helper. Applied `dedup_by(read_silver_table(...), ["gameId"])` before processing
- **Tested**: PK duplicates now 0 across all tables

### Bug 6: fct_player_season_stats uses string dict fields
- **File**: `player_season_impact.py`
- **Problem**: Real data stores shooting stats as string dicts like `"{'made': 175, 'attempted': 367, 'pct': 47.7}"` instead of separate numeric columns
- **Impact**: All shooting stats (FGM, FGA, 3PM, 3PA, FTM, FTA) were null
- **Fix**: Added `_parse_made_attempted()` and `_parse_stat_total()` helpers using `ast.literal_eval` to parse the string dicts. Also handles `rebounds` as a string dict with `total` key
- **Tested**: New test verifies FGM=175, FGA=367 parsed correctly

### Bug 7: Player ID column mismatch
- **File**: `player_season_impact.py`
- **Problem**: Code expected `playerId` but real data has `athleteId` (fct_player_season_stats) and `id` (fct_recruiting_players)
- **Fix**: Changed to `pydict_get_first(stats, ["playerId", "athleteId", "id"])`

## New Helper Functions Added

| Function | File | Purpose |
|----------|------|---------|
| `pydict_get_first(table, candidates)` | `_io_helpers.py` | Try multiple column names, return first match |
| `filter_by_season(table, season)` | `_io_helpers.py` | Filter table by season column (for tables without season partition) |
| `dedup_by(table, key_cols)` | `_io_helpers.py` | Deduplicate table by primary key columns |
| `_concat_with_unified_schema(tables)` | `_io_helpers.py` | Concatenate tables with conflicting schemas |
| `_promote_types(a, b)` | `_io_helpers.py` | Choose broader type for two incompatible types |
| `_parse_stat_dict(val)` | `player_season_impact.py` | Parse string dict from API (e.g. `"{'made': 175, ...}"`) |
| `_parse_made_attempted(raw_col)` | `player_season_impact.py` | Parse list of string dicts into (made, attempted) lists |
| `_parse_stat_total(raw_col)` | `player_season_impact.py` | Parse list of string dicts and extract 'total' key |

## New Tests Added (7)

| Test | Validates |
|------|-----------|
| `test_pydict_get_first_finds_second_candidate` | Fallback column name lookup works |
| `test_pydict_get_first_all_missing` | Returns Nones when no candidates match |
| `test_filter_by_season` | In-memory season filtering works |
| `test_dedup_by` | Primary key deduplication works |
| `test_ratings_adjusted_real_columns` | offensiveRating/defensiveRating handled |
| `test_games_homepoints_columns` | homePoints/awayPoints handled |
| `test_player_stats_string_dict_fields` | String dict field parsing works |

## Spot-Check Values (Season 2025)

### Duke Blue Devils
| Metric | Value | Reasonable? |
|--------|-------|-------------|
| Record | 35-4 | Yes (elite team) |
| Conf Record | 22-1 | Yes |
| PPG | 82.8 | Yes |
| Opp PPG | 62.6 | Yes |
| Adj Net Rating | 39.8 | Yes (top team) |
| SRS Rating | 22.2 | Yes (highest in dataset) |
| Win % | 89.7% | Yes |

### Market Lines (All Games)
| Metric | Value | Reasonable? |
|--------|-------|-------------|
| Home cover rate | 50.1% | Yes (efficient market) |
| Mean spread | -5.4 | Yes (home favorite bias) |
| Spread range | -45.5 to 25.5 | Yes |

## Data Gaps Requiring Attention

1. **Season 2024 missing** from `fct_games`, `fct_pbp_team_daily_rollup`, `fct_pbp_team_daily_rollup_adj` — needs re-ingestion
2. **fct_player_season_stats**: Only 1 row ingested total — needs full backfill for all seasons
3. **fct_recruiting_players**: Only 1 row ingested total — needs full backfill
4. **Duplicate records**: 3,000 duplicate gameIds in season 2025 — silver layer needs dedup pass or pipeline needs idempotent writes

## Recommended Backfill Commands

```bash
# Backfill missing season 2024 games
poetry run python -m cbbd_etl.orchestrate --endpoint games --season 2024

# Backfill player season stats (all seasons)
poetry run python -m cbbd_etl.orchestrate --endpoint stats_player_season --season-range 2010 2026

# Backfill recruiting players (all seasons)
poetry run python -m cbbd_etl.orchestrate --endpoint recruiting_players --season-range 2010 2026

# Rebuild PBP rollups for season 2024 (requires fct_games first)
poetry run python -m cbbd_etl.orchestrate --endpoint plays_game --season 2024
# Then rebuild PBP enriched/rollup tables
```

## Files Modified

| File | Changes |
|------|---------|
| `src/cbbd_etl/gold/_io_helpers.py` | Added `pydict_get_first`, `filter_by_season`, `dedup_by`, `_concat_with_unified_schema`, `_promote_types` |
| `src/cbbd_etl/gold/team_season_summary.py` | Fixed column names, SRS read, dedup |
| `src/cbbd_etl/gold/team_power_rankings.py` | Fixed column names, SRS read |
| `src/cbbd_etl/gold/game_predictions_features.py` | Fixed column names, SRS read, dedup |
| `src/cbbd_etl/gold/market_lines_analysis.py` | Fixed column names, dedup |
| `src/cbbd_etl/gold/player_season_impact.py` | Fixed player ID, string dict parsing, recruiting ID |
| `src/cbbd_etl/tests/test_gold.py` | Added 7 new tests for real-data patterns |
