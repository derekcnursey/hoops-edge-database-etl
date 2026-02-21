# CollegeBasketballData ETL (S3 Lakehouse)

Production-ready ETL pipeline to ingest the full CollegeBasketballData API into S3 (`hoops-edge`) with raw/bronze/silver/gold layers, incremental checkpoints, and Glue/Athena catalog support.

## Features
- Async ingestion with rate limiting + backoff (httpx)
- Raw gzipped JSON + typed Parquet bronze/silver
- Incremental strategy via DynamoDB checkpoints
- Fan-out discovery for game- and player-scoped endpoints
- Gold analytics layer with 5 ML-ready tables
- Data quality framework with TABLE_SPECS contract validation
- Gap-fill framework for discovering and fixing data gaps
- Glue catalog + Athena results location (`s3://hoops-edge/athena/`)
- Structured JSON logs + run summaries in `s3://hoops-edge/meta/`
- Docker containerization with ECS Fargate deployment
- CI/CD via GitHub Actions

## Setup

### 1) Python deps
```bash
poetry install
```

### 2) Env vars
Required:
- `CBBD_API_KEY` (or `BEARER_TOKEN`)

Optional:
- `CBBD_CHECKPOINT_TABLE` (default: `cbbd_checkpoints`)
- `AWS_PROFILE` / `AWS_REGION`

### 3) Config
Edit `config.yaml` for seasons, concurrency, and rolling window. Bucket is fixed to `hoops-edge`.

## Run

Backfill all seasons:
```bash
make backfill
```

Incremental (rolling window + checkpoints):
```bash
make incremental
```

Single endpoint for debugging:
```bash
poetry run python -m cbbd_etl one --endpoint games --params '{"season": 2024}'
```

Validate last run summary:
```bash
make validate
```

## S3 Layout
Top-level prefixes created automatically by writing objects:
- `s3://hoops-edge/raw/` — gzipped JSON from API
- `s3://hoops-edge/bronze/` — typed Parquet (1:1 with API)
- `s3://hoops-edge/silver/` — star-schema fact/dimension tables
- `s3://hoops-edge/gold/` — analytics-ready aggregates
- `s3://hoops-edge/ref/` — reference data
- `s3://hoops-edge/meta/` — run summaries
- `s3://hoops-edge/deadletter/` — failed/empty responses
- `s3://hoops-edge/tmp/` — temporary files
- `s3://hoops-edge/athena/` — Athena query results

Partitioning uses Hive-style keys:
```
bronze:  bronze/{table}/season=YYYY/asof=YYYY-MM-DD/part-*.parquet
silver:  silver/{table}/season=YYYY/date=YYYY-MM-DD/part-*.parquet
gold:    gold/{table}/season=YYYY/asof=YYYY-MM-DD/part-*.parquet
```

## Gold Layer

The gold layer transforms silver-layer star-schema tables into analytics-ready datasets. Run with:

```bash
make gold SEASON=2024                              # Build all gold tables
make gold-table SEASON=2024 TABLE=team_power_rankings  # Build one table
make gold-dry-run SEASON=2024                      # Preview without writing
```

### Tables

| Table | Description | Primary Keys | Key Metrics |
|-------|-------------|-------------|-------------|
| `team_power_rankings` | Composite team ranking combining API ratings, SRS, polls, and PBP efficiencies | teamId, season | adj_net_rating, srs_rating, composite_rank, pbp_adj_net_eff, ap_rank |
| `game_predictions_features` | Pre-game ML feature vectors (2 rows per game: home + away) | gameId, teamId | adj ratings, SRS, pace, Four Factors, spread, moneyline, game outcome labels |
| `player_season_impact` | Per-player efficiency and impact metrics | playerId, season | ppg, efg_pct, true_shooting, usage_rate, per_40 stats, recruiting context |
| `market_lines_analysis` | Lines/spreads merged with outcomes for ATS analysis | gameId, provider | spread, home_covered, over_hit, ats_margin, spread_error |
| `team_season_summary` | Comprehensive team season profiles | teamId, season | W/L, conf record, ratings, Four Factors, pace, recruiting class quality |

Gold tables are registered in the `cbbd_gold` Glue database and queryable via Athena.

## Data Quality

Data quality validation ensures TABLE_SPECS consistency across the pipeline:

```bash
make quality-check        # Run data quality contract tests
make test-quality         # Alias for quality-check
```

### What it validates
- Every TABLE_SPEC has non-empty primary keys
- All primary keys exist in the type hints
- No duplicate table specs
- All COMMON_TYPE_HINTS map to valid PyArrow types
- Every gold transform has a corresponding TABLE_SPEC
- Normalize round-trip preserves primary key columns
- Key silver tables have required fields (gameId, teamId, etc.)

## Gap-Fill Framework

The gap-fill framework discovers and fixes missing data for fan-out endpoints (plays, substitutions, lineups):

```bash
# Discover missing game IDs via Athena and fill them
make gap-fill ENDPOINT=plays_game SEASON=2024

# Discover via S3 listing (no Athena required)
make gap-fill-s3 ENDPOINT=plays_game SEASON=2024

# Validate partition consistency
make validate-partitions SEASON=2024
```

### How it works
1. **Discovery**: Queries Athena or scans S3 to find games in `fct_games` that lack corresponding plays/subs/lineups
2. **Fetch**: Calls the API for each missing game ID with concurrency control and retries
3. **Write**: Writes to all three layers (raw/bronze/silver) with checkpoint tracking
4. **Resume**: Supports resume files so interrupted runs can continue

Supported endpoints: `plays_game`, `substitutions_game`, `lineups_game`

## Docker

### Build locally
```bash
make docker-build
```

### Run locally
```bash
# Using docker compose
docker compose run etl incremental
docker compose run etl backfill
docker compose run etl validate

# Or directly
docker run --env-file .env cbbd-etl:latest incremental
```

The image uses a multi-stage build with a non-root user. Config is baked in; override by mounting:
```bash
docker run -v $(pwd)/config.yaml:/app/config.yaml:ro cbbd-etl:latest incremental
```

### Push to ECR
```bash
make docker-push   # Builds, tags with git SHA + latest, pushes to ECR
make deploy        # docker-push + instructions for ECS run
```

## Deployment (ECS)

The pipeline runs on ECS Fargate, triggered by EventBridge schedules.

### Architecture
- **ECR**: `cbbd-etl` repository (images tagged by git SHA + latest)
- **ECS Cluster**: `cbbd-etl` (Fargate, 0.5 vCPU, 1 GB memory)
- **Schedules**: Daily incremental at 08:00 UTC, weekly validate on Sundays at 12:00 UTC
- **API Key**: Stored in SSM Parameter Store (`/cbbd/api_key`)
- **Logs**: CloudWatch log group `/ecs/cbbd-etl` (30-day retention)

### Initial setup
```bash
# 1. Provision infrastructure
cd infra/terraform
terraform init
terraform apply

# 2. Store API key in SSM
aws ssm put-parameter --name /cbbd/api_key --value "YOUR_TOKEN" --type SecureString --overwrite

# 3. (Optional) Set alert email
terraform apply -var 'alert_email=you@example.com'

# 4. Build and push first image
make deploy
```

### Monitoring
- **CloudWatch Alarms**: Error log detection (immediate), stale pipeline (36 hours no logs), ECS task failures
- **SNS Alerts**: `cbbd-etl-alerts` topic — subscribe via email or Lambda
- **Logs**: Structured JSON via `log_json()`, searchable in CloudWatch Logs Insights

## CI/CD

### GitHub Actions Workflows

**CI (`ci.yml`)** — Runs on PRs and pushes to main:
- Python 3.11 + Poetry: runs `pytest` (excluding integration tests)
- Terraform 1.7: format check + validate

**Deploy (`deploy.yml`)** — Runs on pushes to main (after CI passes):
- Builds Docker image with git SHA labels
- Pushes to ECR (SHA + latest tags)
- Registers new ECS task definition revision

### Required GitHub Secrets
- `AWS_DEPLOY_ROLE_ARN` — IAM role ARN for OIDC-based AWS authentication

## Testing

```bash
make test              # Run all tests
make test-unit         # Unit tests only (excludes integration + quality)
make test-integration  # Integration tests (S3, DynamoDB, Glue roundtrips with moto)
make test-quality      # Data quality contract tests
```

### Test structure
Tests live in `src/cbbd_etl/tests/`:

| File | Tests | What it covers |
|------|-------|----------------|
| `test_config.py` | 7 | YAML loading, validation, token from env |
| `test_s3_io.py` | 11 | S3 put/get JSON, Parquet, list ops |
| `test_normalize.py` | 14 | Type casting, schema normalization |
| `test_normalize_extended.py` | 18 | Edge cases, mixed types |
| `test_checkpoint.py` | 4 | DynamoDB checkpoint CRUD |
| `test_glue_catalog.py` | 14 | Glue type mapping, table management |
| `test_utils.py` | 4 | stable_hash determinism |
| `test_api_client.py` | 7 | Retries, rate limiting |
| `test_gold.py` | 29 | All 5 gold transforms + helpers |
| `test_data_quality.py` | 7 | TABLE_SPECS contracts |
| `test_gap_fill.py` | 7 | Parsing, file loading, partition validation |
| `test_integration.py` | 5 | End-to-end roundtrips with moto |
| `test_dedupe.py` | 3 | Record deduplication |
| `test_partitions.py` | 3 | Partition key generation |

All tests use moto for AWS mocking — no real AWS calls needed.

## Incremental strategy
- Checkpoints in DynamoDB keyed by `(endpoint, parameter_hash)`
- Date endpoints use rolling windows (`api.rolling_window_days`)
- Season endpoints store `last_completed_season` + `last_ingested_date`
- Fan-out endpoints (game/player) use discovery from `/games` and `/games/players`

## IaC (Terraform)
Terraform provisions **everything except the S3 bucket**:
- DynamoDB checkpoint table
- Glue databases and placeholder tables
- IAM roles/policies (least-privilege)
- Athena workgroup
- ECR repository
- ECS cluster + task definition
- EventBridge scheduling rules
- CloudWatch monitoring + SNS alerts

```bash
make tf-plan    # Preview changes
make tf-apply   # Apply changes
make tf-fmt     # Format .tf files
```

## IAM permissions needed (least privilege)
The pipeline role/user needs:
- S3 read/write/list to `hoops-edge` bucket
- DynamoDB read/write to checkpoint table
- Glue get/create/update database and table
- Athena workgroup access for query execution
- SSM read for API key parameter

See `infra/terraform/main.tf` for the full policy document.
