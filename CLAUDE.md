# CollegeBasketballData ETL — Project Instructions

## Project Overview
Production ETL pipeline ingesting the CollegeBasketballData API into an S3 lakehouse (`hoops-edge` bucket) with raw → bronze → silver → gold layers. Stack: Python 3.11+, Poetry, httpx (async), PyArrow/Parquet, boto3, Terraform, Glue/Athena.

## Key Architecture
- **API Source**: `https://api.collegebasketballdata.com` (bearer token auth)
- **S3 Bucket**: `hoops-edge` (us-east-1) — never created by pipeline, must pre-exist
- **Layers**: raw (gzipped JSON) → bronze (typed Parquet) → silver (fact/dim star schema) → gold (analytics-ready)
- **Checkpoints**: DynamoDB table `cbbd_checkpoints` keyed by `(endpoint, parameter_hash)`
- **Catalog**: AWS Glue databases `cbbd_bronze`, `cbbd_silver` with Athena workgroup `cbbd`
- **IaC**: Terraform in `infra/terraform/`

## Codebase Layout
```
src/cbbd_etl/
  orchestrate.py    # Main pipeline orchestrator (backfill, incremental, single-endpoint)
  api_client.py     # Async HTTP client with rate limiting + retries
  checkpoint.py     # DynamoDB checkpoint read/write
  config.py         # YAML config loader
  extractors.py     # Endpoint registry and parameter builders
  glue_catalog.py   # Glue database/table management
  normalize.py      # Record normalization, TABLE_SPECS, dedup
  s3_io.py          # S3 read/write/list, Parquet I/O
  logging_utils.py  # Structured JSON logging
  utils.py          # stable_hash, helpers

scripts/
  backfill_missing_plays.py       # Targeted plays gap-fill via Athena discovery
  verify_missing_plays_api.py     # Verify plays availability against API
  rebuild_from_raw.py             # Rebuild bronze/silver from raw JSON
  normalize_fct_games_partitions.py  # Fix partition layout for fct_games

infra/terraform/
  main.tf           # DynamoDB, Glue, IAM, Athena workgroup
  variables.tf      # region, bucket_name, checkpoint_table_name
  outputs.tf        # ARNs and resource names

config.yaml         # Seasons 2010-2026, endpoints, S3 layout, rate limits
```

## Silver Layer Tables (Star Schema)
**Dimensions**: dim_teams, dim_conferences, dim_venues, dim_lines_providers, dim_play_types
**Facts**: fct_games, fct_game_media, fct_lines, fct_game_teams, fct_game_players, fct_plays, fct_substitutions, fct_lineups, fct_rankings, fct_ratings_adjusted, fct_ratings_srs, fct_recruiting_players, fct_stats_player_season, fct_stats_team_season, fct_stats_player_shooting, fct_stats_team_shooting

## Gold Layer (Current)
- `team_quality_daily` — basic team quality snapshot

## Conventions
- Use `poetry run` for all Python commands
- All S3 keys use Hive-style partitioning: `season=YYYY/asof=YYYY-MM-DD/`
- Parquet files named `part-{hash8}.parquet`
- Config changes go in `config.yaml`, not hardcoded
- Terraform changes require `terraform plan` before `terraform apply`
- Tests in `tests/` using pytest + pytest-asyncio
- Keep structured JSON logging via `log_json()`

## Environment Variables
- `CBBD_API_KEY` or `BEARER_TOKEN` — API authentication
- `CBBD_CHECKPOINT_TABLE` — DynamoDB table name (default: `cbbd_checkpoints`)
- `AWS_PROFILE` / `AWS_REGION` — AWS credentials

## When Making Changes
1. Read the relevant source files before modifying
2. Maintain backward compatibility with existing S3 data
3. Update Glue schemas when adding columns
4. Add/update tests for new functionality
5. Run `make validate` after pipeline changes
