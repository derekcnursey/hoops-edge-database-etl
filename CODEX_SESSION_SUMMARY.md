# Codex Session Summary — hoops_edge_database_etl

Date: 2026-02-01

## Current status (high level)
- **Games (fct_games)** repaired and now queryable (seed types fixed, bad partitions removed, schema rebuilt). Counts by season look correct (≈5.2k–6.4k per season).
- **games_teams / games_players / games_media** capped endpoints fixed via date chunking; data now looks correct after re-runs and repairs.
- **Plays (plays_game / fct_plays)** rebuild-from-raw is running for `ingested_at=2026-01-31` with `--skip-existing`; output is being written across many seasons. Validation pending once complete.
- **Plays JSON flattening** added (onfloor/participants/shot) without increasing row count.
- **Lineups** backfill finished but table is broken in Glue (missing `season`/`date` columns and wrong partitioning). Needs schema fix and re-repair.
- **Ratings adjusted** still broken (only 1 row per season) — requires pipeline/schema fix and re-backfill.

## Recent code changes (important)
### Glue update throttling / version limit fix
- `src/cbbd_etl/glue_catalog.py`: only calls `update_table` when schema/partitions/location changed to avoid `ResourceNumberLimitExceededException` (100k table versions).

### S3 helpers
- `src/cbbd_etl/s3_io.py`: added `exists(key)` using `head_object`.

### rebuild_from_raw improvements
- `scripts/rebuild_from_raw.py`:
  - **`--skip-existing`** to avoid rewriting existing silver files.
  - **`--keys-file`** to process specific raw keys.
  - **Mixed-season handling**: when `--filter-season` is **not** used, mixed-season parts are split and written by season in one pass (no “mixed season” skips).

### Raw part scanner
- `scripts/list_non_empty_raw_parts.py`:
  - Added `--min-records` and `--require-field` (counts JSON lines with required field). Useful for verifying raw is non-empty.

### JSON flattening (plays)
- `src/cbbd_etl/orchestrate.py` `_apply_key_aliases`:
  - **fct_plays** now flattens:
    - `onFloor` -> `onfloor_player1..10`
    - `participants` -> `participant_id` (single ID)
    - `shotInfo` -> `shot_*` columns
  - JSON string columns are retained (and normalized via `_normalize_jsonish`).

### CamelCase to snake_case normalizations
- `src/cbbd_etl/orchestrate.py` `_apply_key_aliases`:
  - **fct_lineups**: camelCase -> snake_case (`teamStats`, `opponentStats`, `idHash`, `netRating`, etc.).
  - **fct_ratings_adjusted**: camelCase -> snake_case; `rankings` dict split into `ranking_offense/defense/net`.

### Schema updates
- `src/cbbd_etl/normalize.py`:
  - **fct_lineups**: schema uses snake_case fields; partitions should be `season` + `date` (but Glue currently doesn’t reflect this).
  - **fct_ratings_adjusted**: schema includes offense/defense/net ratings and rankings.
  - **fct_plays**: schema includes flattened columns described above.

## Notable runtime behavior & findings
- **Raw plays_game parts are not empty** (verified via `aws s3 cp ... | gunzip` on `ingested_at=2026-01-31` keys).
- The earlier “empty records” skips came from per-season filtering, not actual empty raw data.
- A full rebuild of `plays_game` previously hit Glue table version limit; fixed with conditional update.
- `rebuild_from_raw.py --skip-existing` prints both “skip” and “silver” lines (logging cosmetic; writes are skipped when file exists).

## Tables currently healthy / validated
### fct_games
- Partitioning: `season=YYYY/asof=YYYY-MM-DD`.
- Schema repaired; seed fields coerced to string.
- Counts (asof=2026-01-30) are sane (roughly 5.2k–6.4k per season).

### fct_game_teams
- After re-run + repair: rows ≈ 2–3 per game (includes neutral-site and tourney cases) and JSON parse counts look good.

### fct_game_players_flat / fct_game_media_flat
- Flattened CTAS tables exist and validate as expected (not re-verified in this session; assumed still OK).

## Tables still broken / need fixes
### fct_lineups
- Glue table shows **only `asof` partition**, missing `season`/`date` columns.
- `SELECT season...` fails with column not found / duplicate column errors.
- Needs: drop & recreate Glue table with correct schema/partitions, then `MSCK REPAIR`.

### fct_ratings_adjusted
- Current query shows **1 row per season**, indicating bad ingestion or schema.
- `rebuild_from_raw.py --endpoint ratings_adjusted --ingested-at 2026-01-28 --no-bronze` reported mixed seasons in raw and wrote nothing.
- Needs: pipeline change + re-backfill; maybe split by season like plays_game.

### fct_plays
- Rebuild for `ingested_at=2026-01-31` is running (all seasons); still needs repair + validation when done.
- Validation query used:
  - `SELECT COUNT(*) total, SUM(TRY(JSON_PARSE(onfloor)) IS NOT NULL), ... FROM fct_plays;`
  - Interim results looked OK (shotInfo parse lower expected; not all plays are shots).

## Commands / runs in progress
- **Plays rebuild** currently running:
  ```bash
  poetry run python scripts/rebuild_from_raw.py \
    --endpoint plays_game \
    --ingested-at 2026-01-31 \
    --no-bronze \
    --skip-existing
  ```
  (writes many season partitions; should continue until done, then repair fct_plays)

## Known S3 structure
- Raw plays: `s3://hoops-edge/raw/plays/game/ingested_at=YYYY-MM-DD/part-*.json.gz`
- Silver plays: `s3://hoops-edge/silver/fct_plays/season=YYYY/asof=YYYY-MM-DD/part-*.parquet`

## Suggested next steps
1. **Wait for plays_game rebuild to finish**, then:
   - `MSCK REPAIR TABLE cbbd_silver.fct_plays;`
   - Validate counts per season vs games.
2. **Fix fct_lineups Glue schema**:
   - Drop table, recreate with correct columns + partitions (`season`, `date`), then repair.
3. **Fix fct_ratings_adjusted**:
   - Ensure raw is split by season (like plays_game), then rebuild & repair.

## Useful Athena queries
- Plays counts by season:
  - `SELECT season, COUNT(*) FROM fct_plays GROUP BY season ORDER BY season;`
- Games vs plays coverage (2024 example):
  - `WITH g AS (SELECT DISTINCT gameid FROM fct_games WHERE season='2024'), p AS (SELECT DISTINCT gameid FROM fct_plays WHERE season='2024') SELECT COUNT(*) total_games, COUNT(p.gameid) games_with_plays, COUNT(*) - COUNT(p.gameid) missing FROM g LEFT JOIN p ON g.gameid=p.gameid;`
- Team stats JSON validity:
  - `SELECT COUNT(*) total, SUM(CASE WHEN TRY(JSON_PARSE(teamstats)) IS NOT NULL THEN 1 ELSE 0 END) team_ok FROM fct_game_teams;`
- Lineups quick check (after schema fix):
  - `SELECT season, COUNT(*) FROM fct_lineups GROUP BY season ORDER BY season;`

