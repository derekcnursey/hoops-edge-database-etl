# Infrastructure Engineer -- Completed Tasks

**Date**: 2026-02-21
**Author**: Infrastructure Engineer Agent

---

## Summary

Containerized the cbbd_etl pipeline for deployment on AWS ECS Fargate, added scheduled runs via EventBridge, configured monitoring/alerting with CloudWatch and SNS, created CI/CD pipelines with GitHub Actions, and hardened all IAM policies to follow least-privilege principles.

---

## Task 1: Dockerfile + ECS Fargate Terraform

### Files Created

- **`Dockerfile`** -- Multi-stage build using `python:3.11-slim`. Builder stage installs Poetry and exports pinned requirements. Runtime stage installs only production dependencies, copies `src/` and `config.yaml`, sets `PYTHONPATH`, creates a non-root `etl` user, and uses `ENTRYPOINT ["python", "-m", "cbbd_etl"]` so that `docker run <image> incremental` works.
- **`.dockerignore`** -- Excludes `.git/`, `__pycache__/`, `.venv/`, `tests/`, `reports/`, `docs/`, `infra/`, `.terraform/`, IDE files, and other non-runtime artifacts.
- **`infra/terraform/ecs.tf`** -- Contains:
  - `data.aws_vpc.default` and `data.aws_subnets.default` for default VPC networking
  - `data.aws_caller_identity.current` for account-scoped ARNs
  - `aws_ecr_repository.cbbd_etl` with image scanning on push and lifecycle policy (keep last 10 images)
  - `aws_ecs_cluster.cbbd_etl` with Container Insights enabled
  - `aws_cloudwatch_log_group.cbbd_etl` at `/ecs/cbbd-etl` with 30-day retention
  - `aws_ssm_parameter.cbbd_api_key` (SecureString, lifecycle ignores value changes)
  - `aws_iam_role.ecs_execution_role` + policy with ECR pull, CloudWatch logs, and SSM read permissions
  - `aws_ecs_task_definition.cbbd_etl_incremental` -- Fargate, 0.5 vCPU, 1 GB memory, container with `incremental` command, env vars for `AWS_REGION` and `CBBD_CHECKPOINT_TABLE`, secret `CBBD_API_KEY` from SSM, awslogs log driver
  - `local.common_tags` block used by all new resources: `Project = "cbbd-etl"`, `Environment = "production"`, `ManagedBy = "terraform"`

### Resource Sizing Rationale

0.5 vCPU / 1 GB memory was chosen because the ETL pipeline is I/O-bound (HTTP API calls + S3 writes), not compute-heavy. This keeps Fargate costs minimal while providing sufficient headroom.

---

## Task 2: EventBridge Scheduling

### File Created

- **`infra/terraform/scheduling.tf`** -- Contains:
  - `aws_iam_role.events_role` + policy allowing `ecs:RunTask` (scoped to the task definition) and `iam:PassRole` for both the task role and execution role
  - `aws_cloudwatch_event_rule.daily_incremental` -- `cron(0 8 * * ? *)` triggers the incremental ETL daily at 08:00 UTC
  - `aws_cloudwatch_event_target.daily_incremental` -- runs the task on Fargate in default VPC subnets with public IP
  - `aws_cloudwatch_event_rule.weekly_validate` -- `cron(0 12 ? * SUN *)` triggers validation every Sunday at 12:00 UTC
  - `aws_cloudwatch_event_target.weekly_validate` -- overrides the container command to `["validate"]`

---

## Task 3: Monitoring & Alerting

### File Created

- **`infra/terraform/monitoring.tf`** -- Contains:
  - `aws_sns_topic.cbbd_etl_alerts` with optional email subscription (controlled by `var.alert_email`)
  - `aws_cloudwatch_log_metric_filter.error_logs` -- filters for `ERROR` and `CRITICAL` log levels, emits `CBBD/ETL/ETLErrorCount` metric
  - `aws_cloudwatch_log_metric_filter.log_events` -- counts all log events for staleness detection, emits `CBBD/ETL/ETLLogEventCount` metric
  - `aws_cloudwatch_metric_alarm.error_logs` -- alarms when error count > 0 in any 5-minute period
  - `aws_cloudwatch_metric_alarm.no_recent_run` -- alarms when no log events for 36 hours (3 x 12-hour periods), treats missing data as breaching
  - `aws_cloudwatch_event_rule.task_failure` -- captures ECS task state changes to STOPPED with a non-empty `stoppedReason`
  - `aws_cloudwatch_event_target.task_failure_sns` -- sends formatted failure notification to SNS with task ARN, reason, and stop code
  - `aws_sns_topic_policy.allow_eventbridge` -- grants EventBridge permission to publish to the SNS topic

---

## Task 4: CI/CD (GitHub Actions)

### Files Created

- **`.github/workflows/ci.yml`** -- Runs on push to main and PRs:
  - `test` job: Python 3.11 setup, Poetry install with dependency caching, runs `pytest` (excluding integration tests)
  - `terraform` job: Terraform 1.7.0, runs `terraform fmt -check -recursive` and `terraform validate` with `init -backend=false`
  - All actions use pinned versions (`actions/checkout@v4`, `actions/setup-python@v5`, `snok/install-poetry@v1`, `hashicorp/setup-terraform@v3`, `actions/cache@v4`)

- **`.github/workflows/deploy.yml`** -- Runs on push to main after CI passes:
  - Uses `needs: ci` via reusable workflow reference
  - Configures AWS credentials via OIDC (`aws-actions/configure-aws-credentials@v4`)
  - Logs into ECR (`aws-actions/amazon-ecr-login@v2`)
  - Builds Docker image tagged with git SHA and `latest`
  - Pushes both tags to ECR
  - Registers a new ECS task definition revision with the updated image
  - Requires `AWS_DEPLOY_ROLE_ARN` secret to be configured in the repository

---

## Task 5: IAM Hardening

### File Modified

- **`infra/terraform/main.tf`** -- Updated `aws_iam_policy.etl_policy`:
  - **Glue**: Scoped from `Resource: "*"` to specific catalog, database, and table ARNs for `cbbd_bronze` and `cbbd_silver`. Added `glue:GetPartitions` and `glue:BatchCreatePartition` actions needed by the pipeline.
  - **Athena**: Scoped from `Resource: "*"` to `aws_athena_workgroup.cbbd.arn`. Added `athena:GetQueryResults` action.
  - **SSM**: Added new statement granting `ssm:GetParameter` and `ssm:GetParameters` scoped to `parameter/cbbd/api_key`.
  - **All statements**: Added `Sid` identifiers for auditability.

### Execution Role Permissions (in ecs.tf)

- `ecr:GetAuthorizationToken` (global, required by AWS)
- `ecr:BatchCheckLayerAvailability`, `ecr:GetDownloadUrlForLayer`, `ecr:BatchGetImage` (scoped to ECR repo ARN)
- `logs:CreateLogStream`, `logs:PutLogEvents` (scoped to log group ARN)
- `ssm:GetParameters`, `ssm:GetParameter` (scoped to SSM parameter ARN)

### EventBridge Role Permissions (in scheduling.tf)

- `ecs:RunTask` (scoped to task definition ARN with cluster condition)
- `iam:PassRole` (scoped to task role and execution role ARNs)

---

## Variables Added (`variables.tf`)

| Variable | Type | Default | Description |
|---|---|---|---|
| `ecr_repo_name` | string | `cbbd-etl` | ECR repository name |
| `ecs_cluster_name` | string | `cbbd-etl` | ECS cluster name |
| `alert_email` | string | `""` | Email for SNS alerts (empty = no subscription) |

## Outputs Added (`outputs.tf`)

| Output | Description |
|---|---|
| `ecr_repository_url` | ECR repository URL for docker push |
| `ecs_cluster_name` | ECS cluster name |
| `ecs_cluster_arn` | ECS cluster ARN |
| `ecs_task_definition_arn` | Task definition ARN |
| `execution_role_arn` | ECS execution role ARN |
| `sns_alerts_topic_arn` | SNS alerts topic ARN |
| `cloudwatch_log_group` | CloudWatch log group name |

---

## Validation

- `terraform fmt -check -recursive` passes with no changes needed
- `terraform validate` returns `Success! The configuration is valid.`

---

## Deployment Steps

1. **Set the API key in SSM** (one-time):
   ```bash
   aws ssm put-parameter --name /cbbd/api_key --value "YOUR_BEARER_TOKEN" --type SecureString --overwrite
   ```

2. **Apply Terraform**:
   ```bash
   cd infra/terraform
   terraform init
   terraform plan
   terraform apply
   ```

3. **Build and push Docker image**:
   ```bash
   ECR_URL=$(terraform output -raw ecr_repository_url)
   aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin $ECR_URL
   docker build -t $ECR_URL:latest .
   docker push $ECR_URL:latest
   ```

4. **Configure GitHub secrets** for CI/CD:
   - `AWS_DEPLOY_ROLE_ARN` -- IAM role ARN for GitHub OIDC federation

5. **Set alert email** (optional):
   ```bash
   terraform apply -var 'alert_email=your@email.com'
   ```

---

## Files Summary

| File | Action | Description |
|---|---|---|
| `Dockerfile` | Created | Multi-stage Docker build for the ETL pipeline |
| `.dockerignore` | Created | Excludes non-runtime files from Docker context |
| `infra/terraform/ecs.tf` | Created | ECR, ECS cluster, log group, SSM, execution role, task definition |
| `infra/terraform/scheduling.tf` | Created | EventBridge rules for daily incremental and weekly validate |
| `infra/terraform/monitoring.tf` | Created | SNS, metric filters, CloudWatch alarms, task failure detection |
| `infra/terraform/main.tf` | Modified | IAM policy hardened with specific resource ARNs |
| `infra/terraform/variables.tf` | Modified | Added ecr_repo_name, ecs_cluster_name, alert_email variables |
| `infra/terraform/outputs.tf` | Modified | Added outputs for ECR, ECS, SNS, CloudWatch resources |
| `.github/workflows/ci.yml` | Created | CI pipeline: tests + Terraform lint/validate |
| `.github/workflows/deploy.yml` | Created | CD pipeline: Docker build, ECR push, ECS task def update |
| `reports/infra_engineer_done.md` | Created | This summary report |
