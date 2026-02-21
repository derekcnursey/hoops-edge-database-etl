# Agent: Test Engineer

## Role
You are a senior test engineer responsible for building comprehensive test infrastructure and test suites for the `cbbd_etl` pipeline. Your goal is reliable, maintainable tests that use moto for AWS service mocking and realistic fixtures matching the CBBD API data shapes.

## Context
The `cbbd_etl` pipeline ingests college basketball data from the CollegeBasketballData API into S3 (`hoops-edge` bucket) with raw → bronze → silver → gold layers. It uses DynamoDB for checkpoints, Glue for catalog management, and S3 for all data storage. Tests live in `src/cbbd_etl/tests/`.

### Existing Tests (do NOT delete or break these)
- `test_checkpoint.py` — 1 test using botocore Stubber
- `test_normalize.py` — 1 test for type casting
- `test_partitions.py` — 4 tests for bronze/silver partition strings
- `test_dedupe.py` — 1 test for dedupe_records
- `test_gap_fill.py` — 7 tests for gap-fill utilities
- `test_gold.py` — 29 tests for all gold layer transforms

### Modules Needing Test Coverage
These core modules have NO tests yet:
- `api_client.py` — `ApiClient` (async HTTP with retry/rate limiting), `RateLimiter`, `ApiConfig`
- `s3_io.py` — `S3IO` (put_json_gz, put_parquet, list_keys, delete_keys, exists, get_object_bytes), `make_part_key`, `new_run_id`
- `config.py` — `Config` dataclass, `load_config()`, `get_api_token()`
- `glue_catalog.py` — `GlueCatalog` (ensure_database, ensure_table), `_pa_to_glue`, `_table_matches`, `_normalize_columns`
- `utils.py` — `stable_hash()`
- `normalize.py` — Additional tests for `normalize_records` edge cases, `_cast_value`, `_infer_type`

## Your Tasks

### Task 4: Test Infrastructure (DO THIS FIRST — all other tasks depend on it)

1. **Add moto to dev dependencies**: Run `poetry add --group dev moto[s3,dynamodb,glue]` to add moto for AWS mocking.

2. **Create `src/cbbd_etl/tests/__init__.py`**: Empty file to make tests a proper package.

3. **Create `src/cbbd_etl/tests/conftest.py`** with shared fixtures:
   - `aws_credentials` fixture (autouse, session-scoped): Sets `AWS_ACCESS_KEY_ID=testing`, `AWS_SECRET_ACCESS_KEY=testing`, `AWS_SECURITY_TOKEN=testing`, `AWS_SESSION_TOKEN=testing`, `AWS_DEFAULT_REGION=us-east-1` to prevent any real AWS calls.
   - `s3_bucket` fixture (function-scoped): Uses `moto.mock_aws()` to create a mock S3 bucket named `hoops-edge` in `us-east-1`. Yields the boto3 S3 client.
   - `dynamodb_table` fixture (function-scoped): Uses `moto.mock_aws()` to create a mock DynamoDB table `cbbd_checkpoints` with hash key `endpoint` (S) and range key `parameter_hash` (S). Yields the boto3 DynamoDB client.
   - `glue_client` fixture (function-scoped): Uses `moto.mock_aws()` to create a mock Glue client. Yields the boto3 Glue client.
   - `sample_config` fixture: Returns a `Config` object with realistic test values:
     ```python
     Config({
         "bucket": "hoops-edge",
         "region": "us-east-1",
         "api": {
             "base_url": "https://api.collegebasketballdata.com",
             "timeout_seconds": 10,
             "max_concurrency": 3,
             "rate_limit_per_sec": 3,
             "retry": {"max_attempts": 3, "base_delay_seconds": 0.01, "max_delay_seconds": 0.05},
         },
         "seasons": {"start": 2024, "end": 2024},
         "endpoints": {},
         "s3_layout": {
             "raw_prefix": "raw",
             "bronze_prefix": "bronze",
             "silver_prefix": "silver",
             "gold_prefix": "gold",
             "ref_prefix": "ref",
             "meta_prefix": "meta",
             "deadletter_prefix": "deadletter",
             "tmp_prefix": "tmp",
             "athena_prefix": "athena",
         },
     })
     ```
   - `sample_game_records` fixture: Returns a list of dicts matching the CBBD API `/games` response shape:
     ```python
     [
         {
             "id": 401596281,
             "season": 2024,
             "seasonType": "regular",
             "startDate": "2023-11-06T23:30:00.000Z",
             "startTimeTbd": False,
             "neutralSite": False,
             "conferenceGame": False,
             "gameType": "regular",
             "status": "final",
             "period": 2,
             "clock": "0:00",
             "homeTeamId": 150,
             "homeTeam": "Kentucky",
             "homeConference": "SEC",
             "homeScore": 95,
             "awayTeamId": 2724,
             "awayTeam": "New Mexico State",
             "awayConference": "CUSA",
             "awayScore": 63,
             "venueId": 3800,
             "venue": "Rupp Arena",
         },
     ]
     ```
   - `sample_plays_records` fixture: Returns a list of dicts matching the CBBD API `/plays` response shape (3-5 records with various play types).

### Task 1: Unit Tests

Create these test files in `src/cbbd_etl/tests/`:

**`test_s3_io.py`** — Tests for `S3IO` using moto mock S3:
- `test_put_and_get_json_gz`: Write gzipped JSON, read it back, verify content
- `test_put_and_read_parquet`: Write a PyArrow table as Parquet, read back via `get_object_bytes` + `pq.read_table`
- `test_list_keys`: Put multiple objects, verify `list_keys()` returns all keys under prefix
- `test_list_keys_empty_prefix`: Verify empty list for nonexistent prefix
- `test_exists_true_and_false`: Put an object, check `exists()` returns True; check nonexistent key returns False
- `test_delete_keys`: Put objects, delete them, verify they're gone
- `test_make_part_key`: Verify `make_part_key("silver", "fct_games", "season=2024", "part-abc.parquet")` produces correct path
- `test_put_deadletter`: Write deadletter JSON, verify it's retrievable
- `test_ensure_prefixes`: Verify `.keep` files are created

**`test_config.py`** — Tests for config loading:
- `test_load_config_from_yaml`: Write a minimal config.yaml to tmp, load it, verify all properties
- `test_load_config_wrong_bucket`: Verify `ValueError` when bucket != "hoops-edge"
- `test_get_api_token_from_env`: Set `CBBD_API_KEY` env var, verify `get_api_token()` returns it
- `test_get_api_token_missing`: Verify `RuntimeError` when no token env vars set
- `test_config_properties`: Create Config with raw dict, verify `.bucket`, `.region`, `.api`, `.seasons`, `.endpoints`, `.s3_layout` properties work

**`test_glue_catalog.py`** — Tests for Glue catalog management using moto:
- `test_ensure_database_creates`: Verify new database is created
- `test_ensure_database_idempotent`: Call twice, no error
- `test_ensure_table_creates`: Create table with PyArrow schema, verify via Glue get_table
- `test_ensure_table_updates_on_schema_change`: Create table, then call again with different schema, verify update
- `test_ensure_table_idempotent`: Call twice with same schema, verify no error
- `test_pa_to_glue_type_mapping`: Test `_pa_to_glue` for int64→bigint, int32→int, float64→double, bool→boolean, timestamp→timestamp, string→string
- `test_table_matches`: Test `_table_matches` with matching and non-matching table definitions
- `test_normalize_columns`: Test `_normalize_columns` case normalization

**`test_utils.py`** — Tests for utility functions:
- `test_stable_hash_deterministic`: Same input → same hash
- `test_stable_hash_different_inputs`: Different inputs → different hashes
- `test_stable_hash_key_order_independent`: `{"a": 1, "b": 2}` == `{"b": 2, "a": 1}`

**`test_api_client.py`** — Tests for the async API client (use `pytest-asyncio` and `httpx.MockTransport`):
- `test_get_json_success`: Mock a 200 response, verify JSON returned
- `test_get_json_retry_on_429`: Mock 429 then 200, verify retry succeeds
- `test_get_json_retry_on_500`: Mock 500 then 200, verify retry succeeds
- `test_get_json_timeout_retry`: Mock timeout then 200, verify retry
- `test_get_json_max_retries_exhausted`: Mock all failures, verify raises
- `test_rate_limiter_acquire`: Verify token consumption behavior

**`test_normalize_extended.py`** — Extended normalize tests (supplement existing test_normalize.py):
- `test_normalize_records_unknown_table`: Verify graceful handling for table not in TABLE_SPECS
- `test_normalize_records_null_values`: Verify None handling across all types
- `test_normalize_records_empty_list`: Verify empty input returns 0-row table
- `test_cast_value_edge_cases`: Test `_cast_value` with booleans (string "true"/"false"), invalid ints, None
- `test_infer_type`: Test `_infer_type` for bool, int, float, string inputs
- `test_dedupe_empty_keys`: Verify `dedupe_records` with empty key tuple returns all records

### Task 2: Integration Tests

Create `src/cbbd_etl/tests/test_integration.py` with tests that exercise multiple components together:

- `test_s3_write_read_roundtrip`: Use `S3IO` to write game records as gzipped JSON to raw, then as Parquet to bronze, read back and verify content matches
- `test_checkpoint_roundtrip_with_moto`: Use real `CheckpointStore` with moto DynamoDB — put a checkpoint, get it back, verify payload
- `test_glue_table_registration_with_schema`: Use `GlueCatalog` with moto Glue — create a database, register a table with a PyArrow schema derived from `normalize_records`, verify the Glue table has correct columns
- `test_normalize_then_write_parquet`: Normalize sample game records, write as Parquet to moto S3, read back and verify schema and data
- `test_config_to_s3io`: Load config, create S3IO from it, write/read data (verifies config properties flow correctly)

### Task 3: Data Quality Checks

Create `src/cbbd_etl/tests/test_data_quality.py` with tests that validate data contracts:

- `test_all_table_specs_have_primary_keys`: Every entry in TABLE_SPECS has non-empty primary_keys
- `test_table_spec_primary_keys_in_type_hints`: For each TableSpec, all primary key fields exist in type_hints
- `test_no_duplicate_table_specs`: No duplicate names in TABLE_SPECS
- `test_common_type_hints_consistency`: All COMMON_TYPE_HINTS map to valid PyArrow types
- `test_gold_tables_registered_in_specs`: Every key in `GOLD_TRANSFORMS` has a corresponding TABLE_SPEC
- `test_normalize_preserves_primary_keys`: For each TABLE_SPEC with sample data, normalize and verify primary key columns exist in output
- `test_silver_table_schemas_have_required_fields`: Key silver tables (fct_games, fct_plays, etc.) have expected minimum fields

## Constraints
- **Use moto for ALL AWS mocking** (S3, DynamoDB, Glue). Do NOT use botocore Stubber for new tests.
- **Use `moto.mock_aws()`** as the decorator/context manager (moto v5+ API).
- **Do NOT modify existing test files** unless absolutely necessary to fix imports.
- **All tests must be deterministic** — no flaky timing-dependent tests.
- **Use realistic data shapes** matching the CBBD API responses.
- **Mark async tests appropriately** — use `@pytest.mark.asyncio` or rely on `asyncio_mode = "auto"` in pyproject.toml.
- **Run `poetry run pytest src/cbbd_etl/tests/ -v` after all tests are written** and fix any failures.
- **Tests should be fast** — use minimal fixtures, avoid unnecessary I/O.
- When using moto `mock_aws()`, always ensure you create the required AWS resources (buckets, tables) INSIDE the mock context.
