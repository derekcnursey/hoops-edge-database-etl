# Data Completeness Audit Report

## Overview

Audit queries are in `scripts/sql/data_completeness_audit.sql`. Run them against `cbbd_silver` using the `cbbd` Athena workgroup.

## Known Gaps

### 1. fct_plays (Primary Concern)
- **Issue:** Many games lack play-by-play data across seasons 2010-2026
- **Root Cause:** Fan-out endpoints require per-game API calls; initial backfills may have been interrupted
- **Fix:** Use `gap_fill.py` or improved `backfill_missing_plays.py` with `--season-range`
- **Priority:** High — plays data drives the entire PBP analytics pipeline

### 2. fct_substitutions
- **Issue:** Same pattern as plays; game-level fan-out may have gaps
- **Fix:** Use `gap_fill.py` with `--endpoint substitutions_game`
- **Priority:** Medium

### 3. fct_lineups
- **Issue:** Lineup data may be sparse, especially for older seasons
- **Fix:** Use `gap_fill.py` with `--endpoint lineups_game`
- **Priority:** Medium

### 4. Partition Inconsistencies
- **Issue:** Some `fct_games` partitions used `season/date` instead of `season/asof`
- **Fix:** Run `scripts/normalize_fct_games_partitions.py` then `MSCK REPAIR TABLE`

## Expected Partition Layouts

| Layer  | Table Type    | Pattern                       |
|--------|--------------|-------------------------------|
| Bronze | snapshot      | `asof=YYYY-MM-DD`             |
| Bronze | season        | `season=YYYY/asof=YYYY-MM-DD` |
| Bronze | game_fanout   | `season=YYYY/date=YYYY-MM-DD` |
| Silver | dim_*         | `asof=YYYY-MM-DD`             |
| Silver | fct_* (date)  | `season=YYYY/date=YYYY-MM-DD` |
| Silver | fct_* (season)| `season=YYYY/asof=YYYY-MM-DD` |

## How to Run Audit

```bash
# Run queries via AWS CLI
aws athena start-query-execution \
    --work-group cbbd \
    --query-execution-context Database=cbbd_silver \
    --result-configuration OutputLocation=s3://hoops-edge/athena/ \
    --query-string "$(cat scripts/sql/data_completeness_audit.sql)"

# Or use gap_fill discovery mode
poetry run python -m cbbd_etl.gap_fill --endpoint plays_game --discover --season 2024
```
