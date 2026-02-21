output "checkpoint_table_name" {
  value = aws_dynamodb_table.checkpoints.name
}

output "glue_bronze_db" {
  value = aws_glue_catalog_database.bronze.name
}

output "glue_silver_db" {
  value = aws_glue_catalog_database.silver.name
}

output "iam_role_arn" {
  value = aws_iam_role.etl_role.arn
}

output "ecr_repository_url" {
  description = "URL of the ECR repository for docker push"
  value       = aws_ecr_repository.cbbd_etl.repository_url
}

output "ecs_cluster_name" {
  description = "Name of the ECS cluster"
  value       = aws_ecs_cluster.cbbd_etl.name
}

output "ecs_cluster_arn" {
  description = "ARN of the ECS cluster"
  value       = aws_ecs_cluster.cbbd_etl.arn
}

output "ecs_task_definition_arn" {
  description = "ARN of the ECS task definition (incremental)"
  value       = aws_ecs_task_definition.cbbd_etl_incremental.arn
}

output "execution_role_arn" {
  description = "ARN of the ECS execution role"
  value       = aws_iam_role.ecs_execution_role.arn
}

output "sns_alerts_topic_arn" {
  description = "ARN of the SNS alerts topic"
  value       = aws_sns_topic.cbbd_etl_alerts.arn
}

output "cloudwatch_log_group" {
  description = "Name of the CloudWatch log group for ECS tasks"
  value       = aws_cloudwatch_log_group.cbbd_etl.name
}
