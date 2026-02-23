.PHONY: backfill incremental validate one gap-fill gap-fill-s3 validate-partitions \
       test test-unit test-integration test-quality quality-check \
       gold gold-table gold-dry-run \
       docker-build docker-push deploy \
       tf-plan tf-apply tf-fmt

# ---------------------------------------------------------------------------
# Pipeline targets
# ---------------------------------------------------------------------------

backfill:
	poetry run python -m cbbd_etl backfill

incremental:
	poetry run python -m cbbd_etl incremental

one:
	poetry run python -m cbbd_etl one --endpoint $(endpoint) --params '$(params)'

validate:
	poetry run python -m cbbd_etl validate

# ---------------------------------------------------------------------------
# Gap-fill targets
# ---------------------------------------------------------------------------

gap-fill:
	poetry run python -m cbbd_etl.gap_fill --endpoint $(ENDPOINT) --season $(SEASON) --discover

gap-fill-s3:
	poetry run python -m cbbd_etl.gap_fill --endpoint $(ENDPOINT) --season $(SEASON) --discover-s3

validate-partitions:
	poetry run python -m cbbd_etl.gap_fill --endpoint plays_game --season $(SEASON) --validate

# ---------------------------------------------------------------------------
# Gold layer targets
# ---------------------------------------------------------------------------

gold:
	poetry run python -m cbbd_etl.gold.runner --season $(SEASON)

gold-table:
	poetry run python -m cbbd_etl.gold.runner --season $(SEASON) --table $(TABLE)

gold-dry-run:
	poetry run python -m cbbd_etl.gold.runner --season $(SEASON) --dry-run

# ---------------------------------------------------------------------------
# Silver dedup targets
# ---------------------------------------------------------------------------

dedup-silver:
	poetry run python scripts/deduplicate_silver.py --table $(TABLE) --season $(SEASON)

dedup-silver-dry:
	poetry run python scripts/deduplicate_silver.py --table $(TABLE) --season $(SEASON) --dry-run

dedup-silver-all:
	poetry run python scripts/deduplicate_silver.py --all --season $(SEASON)

# ---------------------------------------------------------------------------
# Testing targets
# ---------------------------------------------------------------------------

test:
	poetry run pytest src/cbbd_etl/tests/ -v

test-unit:
	poetry run pytest src/cbbd_etl/tests/ -v --ignore=src/cbbd_etl/tests/test_integration.py --ignore=src/cbbd_etl/tests/test_data_quality.py

test-integration:
	poetry run pytest src/cbbd_etl/tests/test_integration.py -v

test-quality:
	poetry run pytest src/cbbd_etl/tests/test_data_quality.py -v

quality-check: test-quality

# ---------------------------------------------------------------------------
# Docker targets
# ---------------------------------------------------------------------------

ECR_URL := $(shell cd infra/terraform && terraform output -raw ecr_repository_url 2>/dev/null)

docker-build:
	docker build -t cbbd-etl:latest .

docker-push: docker-build
	aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin $(ECR_URL)
	docker tag cbbd-etl:latest $(ECR_URL):latest
	docker tag cbbd-etl:latest $(ECR_URL):$(shell git rev-parse --short HEAD)
	docker push $(ECR_URL):latest
	docker push $(ECR_URL):$(shell git rev-parse --short HEAD)

deploy: docker-push
	@echo "Image pushed. ECS will pick up :latest on next scheduled run."
	@echo "To force immediate run:"
	@echo "  aws ecs run-task --cluster cbbd-etl --task-definition cbbd-etl-incremental --launch-type FARGATE --network-configuration 'awsvpcConfiguration={subnets=[SUBNET_ID],assignPublicIp=ENABLED}'"

# ---------------------------------------------------------------------------
# Terraform targets
# ---------------------------------------------------------------------------

tf-plan:
	cd infra/terraform && terraform plan

tf-apply:
	cd infra/terraform && terraform apply

tf-fmt:
	cd infra/terraform && terraform fmt
