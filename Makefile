.PHONY: backfill incremental validate one

backfill:
	poetry run python -m cbbd_etl backfill

incremental:
	poetry run python -m cbbd_etl incremental

one:
	poetry run python -m cbbd_etl one --endpoint $(endpoint) --params '$(params)'

validate:
	poetry run python -m cbbd_etl validate
