# =============================================================================
# EventBridge scheduling for cbbd-etl pipeline
# =============================================================================

# -----------------------------------------------------------------------------
# IAM Role for EventBridge to invoke ECS RunTask
# -----------------------------------------------------------------------------

resource "aws_iam_role" "events_role" {
  name = "cbbd-etl-events-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "events.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })

  tags = local.common_tags
}

resource "aws_iam_policy" "events_policy" {
  name        = "cbbd-etl-events-policy"
  description = "Allows EventBridge to run ECS tasks and pass required IAM roles"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "ECSRunTask"
        Effect   = "Allow"
        Action   = "ecs:RunTask"
        Resource = aws_ecs_task_definition.cbbd_etl_incremental.arn_without_revision
        Condition = {
          ArnLike = {
            "ecs:cluster" = aws_ecs_cluster.cbbd_etl.arn
          }
        }
      },
      {
        Sid    = "PassRoles"
        Effect = "Allow"
        Action = "iam:PassRole"
        Resource = [
          aws_iam_role.etl_role.arn,
          aws_iam_role.ecs_execution_role.arn
        ]
      }
    ]
  })

  tags = local.common_tags
}

resource "aws_iam_role_policy_attachment" "events_attach" {
  role       = aws_iam_role.events_role.name
  policy_arn = aws_iam_policy.events_policy.arn
}

# -----------------------------------------------------------------------------
# Daily Incremental Run — 08:00 UTC (after most games finish)
# -----------------------------------------------------------------------------

resource "aws_cloudwatch_event_rule" "daily_incremental" {
  name                = "cbbd-etl-daily-incremental"
  description         = "Trigger daily incremental ETL at 08:00 UTC"
  schedule_expression = "cron(0 8 * * ? *)"

  tags = local.common_tags
}

resource "aws_cloudwatch_event_target" "daily_incremental" {
  rule     = aws_cloudwatch_event_rule.daily_incremental.name
  arn      = aws_ecs_cluster.cbbd_etl.arn
  role_arn = aws_iam_role.events_role.arn

  ecs_target {
    task_definition_arn = aws_ecs_task_definition.cbbd_etl_incremental.arn
    task_count          = 1
    launch_type         = "FARGATE"
    platform_version    = "LATEST"

    network_configuration {
      subnets          = data.aws_subnets.default.ids
      assign_public_ip = true
    }
  }
}

# -----------------------------------------------------------------------------
# Weekly Validate Run — Sundays at 12:00 UTC
# -----------------------------------------------------------------------------

resource "aws_cloudwatch_event_rule" "weekly_validate" {
  name                = "cbbd-etl-weekly-validate"
  description         = "Trigger weekly validation run on Sundays at 12:00 UTC"
  schedule_expression = "cron(0 12 ? * SUN *)"

  tags = local.common_tags
}

resource "aws_cloudwatch_event_target" "weekly_validate" {
  rule     = aws_cloudwatch_event_rule.weekly_validate.name
  arn      = aws_ecs_cluster.cbbd_etl.arn
  role_arn = aws_iam_role.events_role.arn

  ecs_target {
    task_definition_arn = aws_ecs_task_definition.cbbd_etl_incremental.arn
    task_count          = 1
    launch_type         = "FARGATE"
    platform_version    = "LATEST"

    network_configuration {
      subnets          = data.aws_subnets.default.ids
      assign_public_ip = true
    }
  }

  input = jsonencode({
    containerOverrides = [
      {
        name    = "cbbd-etl"
        command = ["validate"]
      }
    ]
  })
}
