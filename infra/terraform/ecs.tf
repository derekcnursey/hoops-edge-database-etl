# =============================================================================
# ECS Fargate infrastructure for cbbd-etl pipeline
# =============================================================================

locals {
  common_tags = {
    Project     = "cbbd-etl"
    Environment = "production"
    ManagedBy   = "terraform"
  }
}

# -----------------------------------------------------------------------------
# Data sources — default VPC and subnets for Fargate networking
# -----------------------------------------------------------------------------

data "aws_caller_identity" "current" {}

data "aws_vpc" "default" {
  default = true
}

data "aws_subnets" "default" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }

  filter {
    name   = "default-for-az"
    values = ["true"]
  }
}

# -----------------------------------------------------------------------------
# ECR Repository
# -----------------------------------------------------------------------------

resource "aws_ecr_repository" "cbbd_etl" {
  name                 = var.ecr_repo_name
  image_tag_mutability = "MUTABLE"

  image_scanning_configuration {
    scan_on_push = true
  }

  tags = local.common_tags
}

resource "aws_ecr_lifecycle_policy" "cbbd_etl" {
  repository = aws_ecr_repository.cbbd_etl.name

  policy = jsonencode({
    rules = [
      {
        rulePriority = 1
        description  = "Keep last 10 images"
        selection = {
          tagStatus   = "any"
          countType   = "imageCountMoreThan"
          countNumber = 10
        }
        action = {
          type = "expire"
        }
      }
    ]
  })
}

# -----------------------------------------------------------------------------
# ECS Cluster
# -----------------------------------------------------------------------------

resource "aws_ecs_cluster" "cbbd_etl" {
  name = var.ecs_cluster_name

  setting {
    name  = "containerInsights"
    value = "enabled"
  }

  tags = local.common_tags
}

# -----------------------------------------------------------------------------
# CloudWatch Log Group
# -----------------------------------------------------------------------------

resource "aws_cloudwatch_log_group" "cbbd_etl" {
  name              = "/ecs/cbbd-etl"
  retention_in_days = 30

  tags = local.common_tags
}

# -----------------------------------------------------------------------------
# SSM Parameter Store — API key (SecureString placeholder)
# -----------------------------------------------------------------------------

resource "aws_ssm_parameter" "cbbd_api_key" {
  name        = "/cbbd/api_key"
  description = "Bearer token for CollegeBasketballData API"
  type        = "SecureString"
  value       = "PLACEHOLDER"

  lifecycle {
    ignore_changes = [value]
  }

  tags = local.common_tags
}

# -----------------------------------------------------------------------------
# ECS Execution Role — ECR pull + CloudWatch logs + SSM read
# -----------------------------------------------------------------------------

resource "aws_iam_role" "ecs_execution_role" {
  name = "cbbd-etl-execution-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "ecs-tasks.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })

  tags = local.common_tags
}

resource "aws_iam_policy" "ecs_execution_policy" {
  name        = "cbbd-etl-execution-policy"
  description = "Allows ECS to pull images from ECR, write logs, and read SSM parameters"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "ECRAuth"
        Effect = "Allow"
        Action = [
          "ecr:GetAuthorizationToken"
        ]
        Resource = "*"
      },
      {
        Sid    = "ECRPull"
        Effect = "Allow"
        Action = [
          "ecr:BatchCheckLayerAvailability",
          "ecr:GetDownloadUrlForLayer",
          "ecr:BatchGetImage"
        ]
        Resource = aws_ecr_repository.cbbd_etl.arn
      },
      {
        Sid    = "CloudWatchLogs"
        Effect = "Allow"
        Action = [
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "${aws_cloudwatch_log_group.cbbd_etl.arn}:*"
      },
      {
        Sid    = "SSMGetParameter"
        Effect = "Allow"
        Action = [
          "ssm:GetParameters",
          "ssm:GetParameter"
        ]
        Resource = aws_ssm_parameter.cbbd_api_key.arn
      }
    ]
  })

  tags = local.common_tags
}

resource "aws_iam_role_policy_attachment" "ecs_execution_attach" {
  role       = aws_iam_role.ecs_execution_role.name
  policy_arn = aws_iam_policy.ecs_execution_policy.arn
}

# -----------------------------------------------------------------------------
# ECS Task Definition — incremental pipeline
# -----------------------------------------------------------------------------

resource "aws_ecs_task_definition" "cbbd_etl_incremental" {
  family                   = "cbbd-etl-incremental"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = "512"
  memory                   = "1024"

  task_role_arn      = aws_iam_role.etl_role.arn
  execution_role_arn = aws_iam_role.ecs_execution_role.arn

  container_definitions = jsonencode([
    {
      name      = "cbbd-etl"
      image     = "${aws_ecr_repository.cbbd_etl.repository_url}:latest"
      essential = true
      command   = ["incremental"]

      environment = [
        {
          name  = "AWS_REGION"
          value = var.region
        },
        {
          name  = "CBBD_CHECKPOINT_TABLE"
          value = var.checkpoint_table_name
        }
      ]

      secrets = [
        {
          name      = "CBBD_API_KEY"
          valueFrom = aws_ssm_parameter.cbbd_api_key.arn
        }
      ]

      logConfiguration = {
        logDriver = "awslogs"
        options = {
          "awslogs-group"         = aws_cloudwatch_log_group.cbbd_etl.name
          "awslogs-region"        = var.region
          "awslogs-stream-prefix" = "ecs"
        }
      }
    }
  ])

  tags = local.common_tags
}
