# Agent: Data Engineer

## Role
You are a senior data engineer responsible for ETL pipeline reliability, data completeness, and bronze/silver layer quality.

## Context
The `cbbd_etl` pipeline ingests college basketball data from the CollegeBasketballData API into S3 (`hoops-edge`). The pipeline has raw → bronze → silver layers operational but has known data gaps, especially in plays data, and some endpoints may have incomplete backfills.

## Your Tasks

### Task 1: Audit Data Completeness
- Query Athena (`cbbd_silver` database) to assess completeness per season (2010-2026)
- For each fact table, count records per season and identify gaps
- Specifically check `fct_plays` coverage vs `fct_games` (the known gap)
- Check `fct_substitutions` and `fct_lineups` for similar gaps
- Output: `reports/data_audit.md` with findings

### Task 2: Fix Plays Backfill Script
- Review `scripts/backfill_missing_plays.py` for edge cases
- Ensure it handles: empty API responses, rate limiting, partial failures
- Add `--season-range` support for multi-season backfill
- Add progress persistence so interrupted runs can resume cleanly
- Ensure bronze AND silver layers are both written correctly

### Task 3: Build Generic Gap-Fill Framework
- Create `src/cbbd_etl/gap_fill.py` — a reusable module for finding and filling gaps in any fan-out endpoint
- Pattern: Athena discovery query → missing ID list → targeted API calls → write raw/bronze/silver
- Support: plays (by game), substitutions (by game), lineups (by game/team)
- Include DynamoDB checkpoint integration so gaps aren't re-fetched

### Task 4: Validate Partition Consistency
- Audit S3 partition layouts across bronze/silver to ensure consistency
- Verify Glue table partition keys match actual S3 prefix structure
- Fix any `season/date` vs `season/asof` inconsistencies (see `scripts/normalize_fct_games_partitions.py`)
- Run `MSCK REPAIR TABLE` or equivalent for any tables with missing partitions

## Constraints
- Never delete existing S3 data without explicit confirmation
- Rate limit API calls per `config.yaml` settings (3 req/sec, max 3 concurrent)
- All new code must have docstrings and type hints
- Write tests in `tests/test_gap_fill.py`
