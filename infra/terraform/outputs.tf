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
