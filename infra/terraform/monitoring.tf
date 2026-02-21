# =============================================================================
# Monitoring & alerting for cbbd-etl pipeline
# =============================================================================

# -----------------------------------------------------------------------------
# SNS Topic for alerts
# -----------------------------------------------------------------------------

resource "aws_sns_topic" "cbbd_etl_alerts" {
  name = "cbbd-etl-alerts"

  tags = local.common_tags
}

resource "aws_sns_topic_subscription" "email_alert" {
  count     = var.alert_email != "" ? 1 : 0
  topic_arn = aws_sns_topic.cbbd_etl_alerts.arn
  protocol  = "email"
  endpoint  = var.alert_email
}

# -----------------------------------------------------------------------------
# CloudWatch Metric Filter — count ERROR and CRITICAL log events
# -----------------------------------------------------------------------------

resource "aws_cloudwatch_log_metric_filter" "error_logs" {
  name           = "cbbd-etl-error-logs"
  log_group_name = aws_cloudwatch_log_group.cbbd_etl.name
  pattern        = "?ERROR ?CRITICAL ?\"level\":\"error\" ?\"level\":\"critical\""

  metric_transformation {
    name          = "ETLErrorCount"
    namespace     = "CBBD/ETL"
    value         = "1"
    default_value = "0"
  }
}

resource "aws_cloudwatch_log_metric_filter" "log_events" {
  name           = "cbbd-etl-log-events"
  log_group_name = aws_cloudwatch_log_group.cbbd_etl.name
  pattern        = ""

  metric_transformation {
    name          = "ETLLogEventCount"
    namespace     = "CBBD/ETL"
    value         = "1"
    default_value = "0"
  }
}

# -----------------------------------------------------------------------------
# CloudWatch Alarm — Error logs detected
# -----------------------------------------------------------------------------

resource "aws_cloudwatch_metric_alarm" "error_logs" {
  alarm_name          = "cbbd-etl-error-logs"
  alarm_description   = "Triggered when ERROR or CRITICAL log messages are detected in the ETL pipeline"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "ETLErrorCount"
  namespace           = "CBBD/ETL"
  period              = 300
  statistic           = "Sum"
  threshold           = 0
  treat_missing_data  = "notBreaching"

  alarm_actions = [aws_sns_topic.cbbd_etl_alerts.arn]
  ok_actions    = [aws_sns_topic.cbbd_etl_alerts.arn]

  tags = local.common_tags
}

# -----------------------------------------------------------------------------
# CloudWatch Alarm — No recent log activity (stale pipeline, 36 hours)
# -----------------------------------------------------------------------------

resource "aws_cloudwatch_metric_alarm" "no_recent_run" {
  alarm_name          = "cbbd-etl-no-recent-run"
  alarm_description   = "Triggered when no log events are received for 36 hours, indicating the pipeline may have stopped running"
  comparison_operator = "LessThanOrEqualToThreshold"
  evaluation_periods  = 3
  metric_name         = "ETLLogEventCount"
  namespace           = "CBBD/ETL"
  period              = 43200 # 12 hours; 3 evaluation periods = 36 hours
  statistic           = "Sum"
  threshold           = 0
  treat_missing_data  = "breaching"

  alarm_actions = [aws_sns_topic.cbbd_etl_alerts.arn]
  ok_actions    = [aws_sns_topic.cbbd_etl_alerts.arn]

  tags = local.common_tags
}

# -----------------------------------------------------------------------------
# EventBridge Rule — ECS task failure notification
# Fires when any cbbd-etl task stops with a non-zero exit or error stopCode
# -----------------------------------------------------------------------------

resource "aws_cloudwatch_event_rule" "task_failure" {
  name        = "cbbd-etl-task-failures"
  description = "Detect ECS task failures for cbbd-etl pipeline"

  event_pattern = jsonencode({
    source      = ["aws.ecs"]
    detail-type = ["ECS Task State Change"]
    detail = {
      clusterArn    = [aws_ecs_cluster.cbbd_etl.arn]
      lastStatus    = ["STOPPED"]
      stoppedReason = [{ "anything-but" : "" }]
    }
  })

  tags = local.common_tags
}

resource "aws_cloudwatch_event_target" "task_failure_sns" {
  rule      = aws_cloudwatch_event_rule.task_failure.name
  target_id = "cbbd-etl-task-failure-sns"
  arn       = aws_sns_topic.cbbd_etl_alerts.arn

  input_transformer {
    input_paths = {
      taskArn       = "$.detail.taskArn"
      stoppedReason = "$.detail.stoppedReason"
      stopCode      = "$.detail.stopCode"
      lastStatus    = "$.detail.lastStatus"
    }
    input_template = "\"CBBD-ETL Task Failed: <taskArn>. Reason: <stoppedReason>. StopCode: <stopCode>. Status: <lastStatus>.\""
  }
}

# Allow EventBridge to publish to the SNS topic
resource "aws_sns_topic_policy" "allow_eventbridge" {
  arn = aws_sns_topic.cbbd_etl_alerts.arn

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "AllowEventBridgePublish"
        Effect = "Allow"
        Principal = {
          Service = "events.amazonaws.com"
        }
        Action   = "SNS:Publish"
        Resource = aws_sns_topic.cbbd_etl_alerts.arn
      }
    ]
  })
}
