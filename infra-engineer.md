# Agent: Infrastructure Engineer

## Role
You are a senior infrastructure/DevOps engineer responsible for making this pipeline production-grade: automated scheduling, monitoring, CI/CD, and deployment.

## Context
Current infra: Terraform provisions DynamoDB, Glue, IAM role, and Athena workgroup. The pipeline runs manually via `make backfill` or `make incremental`. IAM role assumes ECS task execution but no ECS infrastructure exists yet. Everything targets `us-east-1`.

## Your Tasks

### Task 1: ECS Fargate Deployment
- Add to `infra/terraform/`:
  - ECS cluster (Fargate)
  - Task definition for the ETL pipeline (Python container)
  - ECR repository for the Docker image
  - VPC/subnets/security groups (or reference existing default VPC)
  - CloudWatch log group for container logs
- Create `Dockerfile` at project root:
  - Base: `python:3.11-slim`
  - Poetry install (production deps only)
  - Entrypoint: configurable for backfill vs incremental mode
- Create `docker-compose.yml` for local testing

### Task 2: EventBridge Scheduling
- Add EventBridge rule for daily incremental runs (e.g., 6 AM UTC)
- Add EventBridge rule for weekly full validation
- Wire rules to ECS RunTask targets with the ETL IAM role
- Add Terraform variables for schedule expressions (so they're tunable)

### Task 3: Monitoring & Alerting
- Add CloudWatch alarms:
  - Pipeline run failure (ECS task exit code != 0)
  - No data ingested in 24 hours (custom metric from run summary)
  - DynamoDB throttling
  - S3 storage growth anomaly
- Add SNS topic for alerts with email subscription
- Create CloudWatch dashboard: `cbbd-pipeline-dashboard`
  - Panels: daily record counts, error rates, run duration, S3 object counts
- Integrate structured logs: update pipeline to emit CloudWatch Metrics from run summaries

### Task 4: CI/CD Pipeline
- Create `.github/workflows/ci.yml`:
  - On PR: lint (ruff), type check (mypy), unit tests (pytest)
  - On merge to main: build Docker image, push to ECR, update ECS task definition
- Create `.github/workflows/deploy.yml`:
  - Manual trigger for full backfill
  - Input: season range, specific endpoints
- Add `Makefile` targets: `make docker-build`, `make docker-push`, `make deploy`

### Task 5: IAM Hardening
- Review current IAM policy in `main.tf` — tighten `Resource: "*"` on Glue and Athena
- Add resource-level ARNs where possible
- Add least-privilege policy for CI/CD (separate from pipeline role)
- Add S3 lifecycle rules for cost optimization:
  - raw/ → Glacier after 90 days
  - tmp/ → expire after 7 days
  - deadletter/ → expire after 30 days

## Constraints
- All infra changes in Terraform (no ClickOps)
- Terraform state should use S3 backend (add backend config)
- Tag all resources: `project=cbbd`, `environment=production`
- Docker image must be < 500MB
- Keep costs minimal — Fargate Spot where appropriate
