# Real Data Inventory — s3://hoops-edge

Generated: 2026-02-21

## Silver Layer Tables

| Table | Seasons Available | Partition Pattern | Sample Row Count | Notes |
|-------|-------------------|-------------------|------------------|-------|
| dim_teams | N/A | `asof=YYYY-MM-DD/` | 1,516 | All D1+ teams |
| dim_conferences | N/A | `asof=YYYY-MM-DD/` | — | Reference only |
| dim_venues | N/A | `asof=YYYY-MM-DD/` | — | Reference only |
| dim_lines_providers | N/A | `asof=YYYY-MM-DD/` | — | Reference only |
| dim_play_types | N/A | `asof=YYYY-MM-DD/` | — | Reference only |
| fct_games | 2010-2023, 2025-2026 | `season=YYYY/date=YYYY-MM-DD/` | ~9,299 (2025) | **Missing season=2024** |
| fct_game_media | 2010-2026 | `season=YYYY/asof=YYYY-MM-DD/` | — | |
| fct_lines | 2013-2026 | `season=YYYY/asof=YYYY-MM-DD/` | ~5,440 (2025) | |
| fct_game_teams | — | `season=YYYY/asof=YYYY-MM-DD/` | — | |
| fct_game_players | — | `season=YYYY/asof=YYYY-MM-DD/` | — | |
| fct_plays | — | `season=YYYY/asof=YYYY-MM-DD/` | — | |
| fct_substitutions | — | `season=YYYY/asof=YYYY-MM-DD/` | — | |
| fct_lineups | — | `season=YYYY/asof=YYYY-MM-DD/` | — | |
| fct_rankings | 2010-2026 | `season=YYYY/asof=YYYY-MM-DD/` | ~1,765 (2025) | |
| fct_ratings_adjusted | 2014-2026 | `season=YYYY/asof=YYYY-MM-DD/` | 362 (2024) | |
| fct_ratings_srs | N/A | `asof=YYYY-MM-DD/` only | 7,915 (all seasons) | **No season partition** |
| fct_recruiting_players | 2024 only | `season=YYYY/asof=YYYY-MM-DD/` | **1 row** | Severely underingested |
| fct_player_season_stats | 2024 only | `season=YYYY/asof=YYYY-MM-DD/` | **1 row** | Severely underingested |
| fct_team_season_stats | — | — | — | Not checked |
| fct_draft_picks | 2010-2026 | `season=YYYY/asof=YYYY-MM-DD/` | — | |
| fct_pbp_plays_enriched | — | `season=YYYY/date=YYYY-MM-DD/` | — | |
| fct_pbp_game_teams_flat | — | `season=YYYY/date=YYYY-MM-DD/` | — | |
| fct_pbp_game_teams_flat_garbage_removed | — | `season=YYYY/date=YYYY-MM-DD/` | — | |
| fct_pbp_team_daily_rollup | 2010-2023, 2025-2026 | `season=YYYY/date=YYYY-MM-DD/` | ~68,200 (2025) | **Missing season=2024** |
| fct_pbp_team_daily_rollup_adj | 2010-2023, 2025-2026 | `season=YYYY/date=YYYY-MM-DD/` | ~68,200 (2025) | **Missing season=2024** |
| fct_pbp_team_daily_rollup_garbage_removed | — | `season=YYYY/date=YYYY-MM-DD/` | — | |
| fct_pbp_team_daily_rollup_adj_garbage_removed | — | `season=YYYY/date=YYYY-MM-DD/` | — | |

## Gold Layer Tables (Pre-existing)

| Table | Contents |
|-------|----------|
| market_lines_history | Multiple asof partitions, varying sizes |
| team_quality_daily | Placeholder/legacy |

## Key Data Gaps

1. **Season 2024 missing** from `fct_games`, `fct_pbp_team_daily_rollup`, and `fct_pbp_team_daily_rollup_adj`
2. **fct_player_season_stats** has only 1 row (Ali Dibba, Abilene Christian, 2024)
3. **fct_recruiting_players** has only 1 row (season 2024)
4. **fct_ratings_srs** has no season partition — all 7,915 rows in a single `asof=` partition
5. **Duplicate records** in fct_games: ~3,000 duplicate gameIds from overlapping pipeline runs (season 2025)

## Partition Pattern Anomalies

- Most fact tables use `season=YYYY/asof=YYYY-MM-DD/`
- PBP-derived tables use `season=YYYY/date=YYYY-MM-DD/` (date = game date, not ingest date)
- `fct_ratings_srs` has NO season partition at all
- `fct_games` uses `season=YYYY/date=YYYY-MM-DD/` (date subpartitions by game date)

## Schema Mismatches vs Code Expectations

| Table | Expected Column | Real Column | Type |
|-------|----------------|-------------|------|
| fct_ratings_adjusted | offenserating | offensiveRating | double |
| fct_ratings_adjusted | defenserating | defensiveRating | double |
| fct_games | homeScore | homePoints | int64 |
| fct_games | awayScore | awayPoints | int64 |
| fct_player_season_stats | playerId | athleteId | int64 |
| fct_player_season_stats | fieldGoalsMade (int) | fieldGoals (string dict) | string |
| fct_player_season_stats | freeThrowsMade (int) | freeThrows (string dict) | string |
| fct_player_season_stats | threePointFieldGoalsMade (int) | threePointFieldGoals (string dict) | string |
| fct_player_season_stats | rebounds (numeric) | rebounds (string dict) | string |
| fct_recruiting_players | playerId | id | int64 |
| fct_lines | overUnderOpen | overUnderOpen | string in some files, double in others |
| fct_games | excitement | excitement | double in some files, int64 in others |
