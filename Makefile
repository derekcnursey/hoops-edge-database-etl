.PHONY: backfill incremental validate one gap-fill gap-fill-s3 validate-partitions test test-unit test-integration test-quality gold gold-table gold-dry-run docker-build docker-push deploy tf-plan tf-apply tf-fmt

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

# ---------------------------------------------------------------------------
# Infrastructure targets
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
	@echo "To force immediate run: aws ecs run-task --cluster cbbd-etl --task-definition cbbd-etl-incremental --launch-type FARGATE --network-configuration 'awsvpcConfiguration={subnets=[$(shell cd infra/terraform && terraform output -json | jq -r '.ecs_cluster_arn.value' 2>/dev/null || echo "SUBNET_ID")],assignPublicIp=ENABLED}'"

tf-plan:
	cd infra/terraform && terraform plan

tf-apply:
	cd infra/terraform && terraform apply

tf-fmt:
	cd infra/terraform && terraform fmt
