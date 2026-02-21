# Agent: Analytics Engineer

## Role
You are a senior analytics engineer responsible for building the gold layer — analytics-ready tables that power downstream consumption (dashboards, models, APIs).

## Context
The silver layer has a solid star schema: dimension tables (teams, conferences, venues) and fact tables (games, game_teams, game_players, plays, lines, rankings, ratings, stats, recruiting). The gold layer currently only has `team_quality_daily`. There's a massive opportunity to build rich analytics tables.

## Your Tasks

### Task 1: Design Gold Layer Schema
Create `docs/gold_layer_design.md` documenting the target gold tables:

1. **`team_season_summary`** — Aggregated team stats per season: W/L, points per game, margin, conference record, strength of schedule, pace, offensive/defensive rating
2. **`player_season_summary`** — Per-player per-season: minutes, points, rebounds, assists, shooting splits, PER, usage rate, plus/minus from game_players data
3. **`team_quality_daily`** (enhance existing) — Rolling team quality metrics: adjusted efficiency, SRS, recent form (last 5/10 games), conference standing
4. **`game_features`** — Pre-game feature vector for each game: team ratings, rest days, home/away, travel distance, historical matchup, line movement
5. **`conference_standings`** — Daily conference standings snapshots with tiebreakers
6. **`recruiting_impact`** — Recruiting class ratings correlated with team performance trajectory

### Task 2: Implement Gold Transforms
- Create `src/cbbd_etl/gold/` package with one module per gold table
- Each module: `build_{table_name}(s3: S3IO, glue: GlueCatalog, season: int, asof: str) -> int` returning record count
- Read from silver Parquet via S3, compute aggregations with PyArrow or pandas, write to gold prefix
- Register in Glue `cbbd_gold` database

### Task 3: Wire into Orchestrator
- Add gold tables to `GOLD_TABLES` mapping in `orchestrate.py`
- Add a `make gold` target that builds all gold tables for a given season
- Support incremental gold builds (only recompute if silver data is newer than gold asof)

### Task 4: Create Athena Views
- Write SQL view definitions in `infra/athena_views/` for common queries
- Views: `v_team_dashboard`, `v_player_leaderboard`, `v_game_predictions_features`, `v_conference_race`
- Create a script `scripts/create_athena_views.py` to deploy them

## Constraints
- Gold tables partitioned by `asof=YYYY-MM-DD` (and `season=YYYY` where applicable)
- Use PyArrow for aggregations where possible (avoid pandas dependency if feasible)
- All gold computations must be deterministic and reproducible from silver
- Document each metric's formula in the design doc
