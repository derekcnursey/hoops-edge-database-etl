terraform {
  required_version = ">= 1.5.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">= 5.0"
    }
  }
}

provider "aws" {
  region = var.region
}

resource "aws_dynamodb_table" "checkpoints" {
  name         = var.checkpoint_table_name
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "endpoint"
  range_key    = "parameter_hash"

  attribute {
    name = "endpoint"
    type = "S"
  }

  attribute {
    name = "parameter_hash"
    type = "S"
  }
}

resource "aws_glue_catalog_database" "bronze" {
  name = "cbbd_bronze"
}

resource "aws_glue_catalog_database" "silver" {
  name = "cbbd_silver"
}

locals {
  bronze_tables = [
    "conferences",
    "conferences_history",
    "draft_picks",
    "draft_positions",
    "draft_teams",
    "games",
    "games_media",
    "games_players",
    "games_teams",
    "lines",
    "lines_providers",
    "lineups_game",
    "lineups_team",
    "plays_types",
    "plays_game",
    "plays_date",
    "plays_player",
    "plays_team",
    "plays_tournament",
    "substitutions_game",
    "substitutions_player",
    "substitutions_team",
    "rankings",
    "ratings_adjusted",
    "ratings_srs",
    "recruiting_players",
    "stats_player_shooting_season",
    "stats_player_season",
    "stats_team_shooting_season",
    "stats_team_season",
    "teams",
    "teams_roster",
    "venues",
  ]

  silver_tables = [
    "dim_teams",
    "dim_conferences",
    "dim_venues",
    "dim_lines_providers",
    "dim_play_types",
    "fct_games",
    "fct_game_media",
    "fct_lines",
    "fct_game_teams",
    "fct_game_players",
    "fct_plays",
    "fct_substitutions",
    "fct_lineups",
    "fct_rankings",
    "fct_ratings_adjusted",
    "fct_ratings_srs",
    "fct_team_season_stats",
    "fct_team_season_shooting",
    "fct_player_season_stats",
    "fct_player_season_shooting",
    "fct_recruiting_players",
    "fct_draft_picks",
  ]
}

resource "aws_glue_catalog_table" "bronze" {
  for_each      = toset(local.bronze_tables)
  name          = each.value
  database_name = aws_glue_catalog_database.bronze.name
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    classification = "parquet"
    EXTERNAL       = "TRUE"
  }

  storage_descriptor {
    location      = "s3://${var.bucket_name}/bronze/${each.value}/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    columns {
      name = "placeholder"
      type = "string"
    }
  }
}

resource "aws_glue_catalog_table" "silver" {
  for_each      = toset(local.silver_tables)
  name          = each.value
  database_name = aws_glue_catalog_database.silver.name
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    classification = "parquet"
    EXTERNAL       = "TRUE"
  }

  storage_descriptor {
    location      = "s3://${var.bucket_name}/silver/${each.value}/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    columns {
      name = "placeholder"
      type = "string"
    }
  }
}

resource "aws_athena_workgroup" "cbbd" {
  name = "cbbd"

  configuration {
    result_configuration {
      output_location = "s3://${var.bucket_name}/athena/"
    }
  }
}

resource "aws_iam_role" "etl_role" {
  name = "cbbd-etl-role"
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
}

resource "aws_iam_policy" "etl_policy" {
  name = "cbbd-etl-policy"
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "S3Access"
        Effect = "Allow"
        Action = [
          "s3:ListBucket",
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject"
        ]
        Resource = [
          "arn:aws:s3:::${var.bucket_name}",
          "arn:aws:s3:::${var.bucket_name}/*"
        ]
      },
      {
        Sid    = "DynamoDBCheckpoints"
        Effect = "Allow"
        Action = [
          "dynamodb:GetItem",
          "dynamodb:PutItem",
          "dynamodb:UpdateItem",
          "dynamodb:DescribeTable"
        ]
        Resource = aws_dynamodb_table.checkpoints.arn
      },
      {
        Sid    = "GlueCatalog"
        Effect = "Allow"
        Action = [
          "glue:GetDatabase",
          "glue:CreateDatabase",
          "glue:GetTable",
          "glue:CreateTable",
          "glue:UpdateTable",
          "glue:GetPartitions",
          "glue:BatchCreatePartition"
        ]
        Resource = [
          "arn:aws:glue:${var.region}:${data.aws_caller_identity.current.account_id}:catalog",
          aws_glue_catalog_database.bronze.arn,
          aws_glue_catalog_database.silver.arn,
          "arn:aws:glue:${var.region}:${data.aws_caller_identity.current.account_id}:table/${aws_glue_catalog_database.bronze.name}/*",
          "arn:aws:glue:${var.region}:${data.aws_caller_identity.current.account_id}:table/${aws_glue_catalog_database.silver.name}/*"
        ]
      },
      {
        Sid    = "AthenaWorkgroup"
        Effect = "Allow"
        Action = [
          "athena:StartQueryExecution",
          "athena:GetQueryExecution",
          "athena:GetQueryResults",
          "athena:GetWorkGroup"
        ]
        Resource = aws_athena_workgroup.cbbd.arn
      },
      {
        Sid    = "SSMGetApiKey"
        Effect = "Allow"
        Action = [
          "ssm:GetParameter",
          "ssm:GetParameters"
        ]
        Resource = "arn:aws:ssm:${var.region}:${data.aws_caller_identity.current.account_id}:parameter/cbbd/api_key"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "etl_attach" {
  role       = aws_iam_role.etl_role.name
  policy_arn = aws_iam_policy.etl_policy.arn
}
