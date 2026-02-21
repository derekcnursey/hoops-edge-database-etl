.PHONY: backfill incremental validate one gap-fill gap-fill-s3 validate-partitions test test-unit test-integration test-quality gold gold-table gold-dry-run

backfill:
	poetry run python -m cbbd_etl backfill

incremental:
	poetry run python -m cbbd_etl incremental

one:
	poetry run python -m cbbd_etl one --endpoint $(endpoint) --params '$(params)'

validate:
	poetry run python -m cbbd_etl validate

gap-fill:
	poetry run python -m cbbd_etl.gap_fill --endpoint $(endpoint) --season $(season) --discover

gap-fill-s3:
	poetry run python -m cbbd_etl.gap_fill --endpoint $(endpoint) --season $(season) --discover-s3

validate-partitions:
	poetry run python -m cbbd_etl.gap_fill --endpoint plays_game --season $(season) --validate

test:
	poetry run pytest src/cbbd_etl/tests/ -v

test-unit:
	poetry run pytest src/cbbd_etl/tests/ -v --ignore=src/cbbd_etl/tests/test_integration.py --ignore=src/cbbd_etl/tests/test_data_quality.py

test-integration:
	poetry run pytest src/cbbd_etl/tests/test_integration.py -v

test-quality:
	poetry run pytest src/cbbd_etl/tests/test_data_quality.py -v

gold:
	poetry run python -m cbbd_etl.gold.runner --season $(SEASON)

gold-table:
	poetry run python -m cbbd_etl.gold.runner --season $(SEASON) --table $(TABLE)

gold-dry-run:
	poetry run python -m cbbd_etl.gold.runner --season $(SEASON) --dry-run
