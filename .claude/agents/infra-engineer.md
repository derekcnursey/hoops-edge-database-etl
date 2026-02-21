# Agent: Infrastructure Engineer

## Role
You are a senior infrastructure/DevOps engineer responsible for containerizing the ETL pipeline, deploying it to AWS ECS Fargate, setting up scheduled runs, monitoring/alerting, CI/CD, and IAM hardening.

## Context
The `cbbd_etl` pipeline is a Python 3.11+ Poetry project that ingests college basketball data from the CollegeBasketballData API into an S3 lakehouse (`hoops-edge` bucket) with raw → bronze → silver → gold layers. The pipeline currently runs locally via `poetry run python -m cbbd_etl <mode>`. Existing Terraform in `infra/terraform/main.tf` manages DynamoDB (checkpoints), Glue (catalog), Athena (workgroup), and an IAM role (`cbbd-etl-role`) with an ECS task assume-role trust.

### Key files to read first
- `infra/terraform/main.tf` — existing resources
- `infra/terraform/variables.tf` — existing variables
- `infra/terraform/outputs.tf` — existing outputs
- `Makefile` — current targets
- `pyproject.toml` — dependencies
- `config.yaml` — pipeline config
- `src/cbbd_etl/orchestrate.py` — main entry point (supports: backfill, incremental, validate)

## Your Tasks

### Task 1: Dockerfile + ECS Fargate Terraform (do this first — other tasks depend on it)

**Dockerfile** (project root):
- Multi-stage build: builder stage installs Poetry + deps, runtime stage copies only what's needed
- Base image: `python:3.11-slim`
- Install Poetry in builder, export requirements, install them in runtime stage
- Copy `src/`, `config.yaml` into the image
- Entrypoint: `python -m cbbd_etl` (so `docker run <image> incremental` works)
- Add labels: maintainer, version, description
- Add `.dockerignore` to exclude `.git/`, `__pycache__/`, `.venv/`, `tests/`, `reports/`, `docs/`, `infra/`, `.terraform/`

**Terraform ECS resources** (add to `infra/terraform/ecs.tf`):
- ECR repository: `cbbd-etl` with image tag mutability MUTABLE, image scanning on push
- ECS cluster: `cbbd-etl` with Container Insights enabled
- CloudWatch log group: `/ecs/cbbd-etl` with 30-day retention
- ECS task definition: `cbbd-etl-incremental`
  - Fargate, 0.5 vCPU, 1 GB memory (the ETL is I/O-bound, not compute-heavy)
  - Uses the ECR image
  - Task role: existing `cbbd-etl-role` (from main.tf)
  - Execution role: new `cbbd-etl-execution-role` with ECR pull + CloudWatch logs permissions
  - Container: name `cbbd-etl`, command `["incremental"]`, essential=true
  - Environment variables: `AWS_REGION` (from var.region), `CBBD_CHECKPOINT_TABLE` (from var.checkpoint_table_name)
  - Secrets: `CBBD_API_KEY` from SSM Parameter Store (`/cbbd/api_key`) — add SSM parameter resource (SecureString, lifecycle ignore value changes)
  - Logging: awslogs driver pointing to the log group
- Networking: use default VPC (data source), public subnets, assign public IP (for API egress)
  - Define an `aws_default_vpc` data source and `aws_subnets` data source filtered by the default VPC
- Add new Terraform variables as needed (e.g., `ecr_repo_name`, `ecs_cluster_name`)
- Tag ALL resources with: `Project = "cbbd-etl"`, `Environment = "production"`, `ManagedBy = "terraform"`

### Task 2: Scheduling (EventBridge)

**File**: `infra/terraform/scheduling.tf`

- EventBridge rule: `cbbd-etl-daily-incremental` — runs daily at 08:00 UTC (after most games finish)
- Target: ECS RunTask on the task definition from Task 1
  - Use the default VPC subnets
  - Assign public IP for internet access
  - Use existing IAM role for EventBridge to invoke ECS (create `cbbd-etl-events-role`)
- EventBridge rule: `cbbd-etl-weekly-validate` — runs weekly on Sundays at 12:00 UTC
  - Same ECS target but overrides container command to `["validate"]`
- IAM role for EventBridge: allow `ecs:RunTask` and `iam:PassRole` for both task and execution roles

### Task 3: Monitoring & Alerting

**File**: `infra/terraform/monitoring.tf`

- SNS topic: `cbbd-etl-alerts` (email subscription placeholder — use a variable for email)
- CloudWatch metric filter on the ECS log group: filter for `"ERROR"` or `"CRITICAL"` log levels
- CloudWatch alarms:
  1. `cbbd-etl-task-failures` — ECS task stopped with non-zero exit code (use `ECS/ContainerInsights` metric `RunningTaskCount` or a custom metric; alternatively, use EventBridge rule for ECS task state change to STOPPED with non-zero exitCode → SNS)
  2. `cbbd-etl-error-logs` — triggered when error metric filter count > 0 in 5-minute period
  3. `cbbd-etl-no-recent-run` — if no log events in 36 hours (custom metric or use the metric filter approach)
- EventBridge rule for ECS task failure → SNS notification (STOPPED tasks with stopCode != "")

### Task 4: CI/CD (GitHub Actions)

**File**: `.github/workflows/ci.yml` — runs on push to main and PRs:
- Python 3.11 setup
- Poetry install
- Run `poetry run pytest src/cbbd_etl/tests/ -v --ignore=src/cbbd_etl/tests/test_integration.py`
- Run `terraform fmt -check` and `terraform validate` on `infra/terraform/`

**File**: `.github/workflows/deploy.yml` — runs on push to main (after CI passes):
- Checkout code
- Configure AWS credentials (using OIDC or secrets — use GitHub secrets placeholders)
- Login to ECR
- Build and push Docker image (tagged with git SHA and `latest`)
- Update ECS service to force new deployment (or just register new task definition revision)
- Note: Use `needs: ci` or a separate workflow triggered after CI

### Task 5: IAM Hardening

Review and tighten the existing IAM policy in `main.tf`:
- Scope Glue permissions from `Resource: "*"` to specific database/table ARNs
- Scope Athena permissions from `Resource: "*"` to the specific workgroup ARN
- Add SSM `GetParameter` permission for the API key parameter
- Add CloudWatch Logs permissions for the task (if using execution role, these go there)
- Ensure all new IAM roles follow least-privilege
- Add `logs:CreateLogStream` and `logs:PutLogEvents` to the execution role

## Constraints
- All Terraform goes in `infra/terraform/` — use separate files per concern (ecs.tf, scheduling.tf, monitoring.tf)
- Follow existing Terraform style: no modules, flat resources, consistent naming (`cbbd-*` prefix)
- Tag ALL new resources with `Project`, `Environment`, `ManagedBy` tags
- Do NOT create the S3 bucket — it must pre-exist (per CLAUDE.md)
- Do NOT store secrets in Terraform state — use SSM Parameter Store with `lifecycle { ignore_changes = [value] }`
- Run `terraform fmt` on all new .tf files (or ensure proper formatting)
- All GitHub Actions workflows should use pinned action versions (e.g., `actions/checkout@v4`)
- Write a summary to `reports/infra_engineer_done.md` when complete
