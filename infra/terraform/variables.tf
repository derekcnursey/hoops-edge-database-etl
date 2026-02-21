variable "region" {
  type    = string
  default = "us-east-1"
}

variable "bucket_name" {
  type    = string
  default = "hoops-edge"
}

variable "checkpoint_table_name" {
  type    = string
  default = "cbbd_checkpoints"
}

variable "ecr_repo_name" {
  description = "Name of the ECR repository for the ETL container image"
  type        = string
  default     = "cbbd-etl"
}

variable "ecs_cluster_name" {
  description = "Name of the ECS cluster for running ETL tasks"
  type        = string
  default     = "cbbd-etl"
}

variable "alert_email" {
  description = "Email address for SNS alert notifications (leave empty to skip subscription)"
  type        = string
  default     = ""
}
