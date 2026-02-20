# CollegeBasketballData ETL (S3 Lakehouse)

Production-ready ETL pipeline to ingest the full CollegeBasketballData API into S3 (`hoops-edge`) with raw/bronze/silver/gold layers, incremental checkpoints, and Glue/Athena catalog support.

## Features
- Async ingestion with rate limiting + backoff (httpx)
- Raw gzipped JSON + typed Parquet bronze/silver
- Incremental strategy via DynamoDB checkpoints
- Fan-out discovery for game- and player-scoped endpoints
- Glue catalog + Athena results location (`s3://hoops-edge/athena/`)
- Structured JSON logs + run summaries in `s3://hoops-edge/meta/`

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
- `s3://hoops-edge/raw/`
- `s3://hoops-edge/bronze/`
- `s3://hoops-edge/silver/`
- `s3://hoops-edge/gold/`
- `s3://hoops-edge/ref/`
- `s3://hoops-edge/meta/`
- `s3://hoops-edge/deadletter/`
- `s3://hoops-edge/tmp/`
- `s3://hoops-edge/athena/`

Raw example:
```
s3://hoops-edge/raw/plays/game/ingested_at=YYYY-MM-DD/part-*.json.gz
```

Bronze example:
```
s3://hoops-edge/bronze/games/season=YYYY/asof=YYYY-MM-DD/part-*.parquet
```

Silver example:
```
s3://hoops-edge/silver/fct_games/season=YYYY/date=YYYY-MM-DD/part-*.parquet
```

Gold example:
```
s3://hoops-edge/gold/team_quality_daily/asof=YYYY-MM-DD/part-*.parquet
```

## Incremental strategy
- Checkpoints in DynamoDB keyed by `(endpoint, parameter_hash)`
- Date endpoints use rolling windows (`api.rolling_window_days`)
- Season endpoints store `last_completed_season` + `last_ingested_date`
- Fan-out endpoints (game/player) use discovery from `/games` and `/games/players`

## IaC (Terraform)
Terraform provisions **everything except the S3 bucket**:
- DynamoDB checkpoint table
- Glue databases and placeholder tables (schemas updated by runtime)
- IAM role/policy for pipeline
- Athena workgroup with output `s3://hoops-edge/athena/`

```bash
cd infra/terraform
terraform init
terraform apply
```

## IAM permissions needed (least privilege)
The pipeline role/user needs:
- S3 read/write/list to `hoops-edge` bucket
- DynamoDB read/write to checkpoint table
- Glue get/create/update database and table
- (Optional) Athena workgroup access for query execution

See `infra/terraform/main.tf` for the policy document.

## Notes
- Glue tables created by Terraform use placeholder columns; the runtime updates schemas based on Parquet writes.
- The pipeline never creates the `hoops-edge` bucket.
