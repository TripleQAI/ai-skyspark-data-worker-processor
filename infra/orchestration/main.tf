data "aws_partition" "current" {}
data "aws_region" "current" {}
data "aws_caller_identity" "current" {}

locals {
  resources      = yamldecode(file(var.resource_config_file))
  planner_name   = "${var.name_prefix}-planner"
  status_name    = "${var.name_prefix}-status"
  inventory_name = "${var.name_prefix}-inventory"
  workflow_name  = "${var.name_prefix}-ingestion"
  schedule_group = "${var.name_prefix}-schedules"
  workflow_limit = max(values(local.resources.workflow.max_run_seconds)...) + var.planning_allowance_seconds + var.inventory_timeout_seconds
  common_tags    = merge(var.tags, { Application = "skyspark-ingestion" })
}

resource "aws_cloudwatch_log_group" "planner" {
  name              = "/aws/lambda/${local.planner_name}"
  retention_in_days = var.log_retention_days
  tags              = local.common_tags
}

resource "aws_cloudwatch_log_group" "status" {
  name              = "/aws/lambda/${local.status_name}"
  retention_in_days = var.log_retention_days
  tags              = local.common_tags
}

resource "aws_cloudwatch_log_group" "inventory" {
  name              = "/aws/lambda/${local.inventory_name}"
  retention_in_days = var.log_retention_days
  tags              = local.common_tags
}

resource "aws_cloudwatch_log_group" "workflow" {
  name              = "/aws/vendedlogs/states/${local.workflow_name}"
  retention_in_days = var.log_retention_days
  tags              = local.common_tags
}

data "aws_iam_policy_document" "lambda_trust" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "planner" {
  name               = "${var.name_prefix}-planner"
  assume_role_policy = data.aws_iam_policy_document.lambda_trust.json
  tags               = local.common_tags
}

resource "aws_iam_role" "status" {
  name               = "${var.name_prefix}-status"
  assume_role_policy = data.aws_iam_policy_document.lambda_trust.json
  tags               = local.common_tags
}

resource "aws_iam_role" "inventory" {
  name               = "${var.name_prefix}-inventory"
  assume_role_policy = data.aws_iam_policy_document.lambda_trust.json
  tags               = local.common_tags
}

resource "aws_iam_role_policy_attachment" "planner_logs" {
  role       = aws_iam_role.planner.name
  policy_arn = "arn:${data.aws_partition.current.partition}:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_iam_role_policy_attachment" "planner_vpc" {
  role       = aws_iam_role.planner.name
  policy_arn = "arn:${data.aws_partition.current.partition}:iam::aws:policy/service-role/AWSLambdaVPCAccessExecutionRole"
}

resource "aws_iam_role_policy_attachment" "status_logs" {
  role       = aws_iam_role.status.name
  policy_arn = "arn:${data.aws_partition.current.partition}:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_iam_role_policy_attachment" "status_vpc" {
  role       = aws_iam_role.status.name
  policy_arn = "arn:${data.aws_partition.current.partition}:iam::aws:policy/service-role/AWSLambdaVPCAccessExecutionRole"
}

resource "aws_iam_role_policy_attachment" "inventory_logs" {
  role       = aws_iam_role.inventory.name
  policy_arn = "arn:${data.aws_partition.current.partition}:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_iam_role_policy_attachment" "inventory_vpc" {
  role       = aws_iam_role.inventory.name
  policy_arn = "arn:${data.aws_partition.current.partition}:iam::aws:policy/service-role/AWSLambdaVPCAccessExecutionRole"
}

resource "aws_iam_role_policy" "planner_data" {
  name = "read-pinned-config-inventory-and-control-secret"
  role = aws_iam_role.planner.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = concat([
      {
        Effect   = "Allow"
        Action   = ["secretsmanager:GetSecretValue"]
        Resource = [var.control_database_secret_arn]
      },
      {
        Effect   = "Allow"
        Action   = ["s3:GetObjectVersion"]
        Resource = concat(var.config_object_arns, var.inventory_object_arns)
      }
      ], var.secret_kms_key_arn == null ? [] : [{
        Effect   = "Allow"
        Action   = ["kms:Decrypt"]
        Resource = [var.secret_kms_key_arn]
        }], length(var.s3_kms_key_arns) == 0 ? [] : [{
        Effect   = "Allow"
        Action   = ["kms:Decrypt"]
        Resource = var.s3_kms_key_arns
    }])
  })
}

resource "aws_iam_role_policy" "status_data" {
  name = "read-control-secret"
  role = aws_iam_role.status.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = concat([{
      Effect   = "Allow"
      Action   = ["secretsmanager:GetSecretValue"]
      Resource = [var.control_database_secret_arn]
      }], var.secret_kms_key_arn == null ? [] : [{
      Effect   = "Allow"
      Action   = ["kms:Decrypt"]
      Resource = [var.secret_kms_key_arn]
    }])
  })
}

resource "aws_iam_role_policy" "inventory_data" {
  name = "read-certified-metadata-and-write-versioned-inventory"
  role = aws_iam_role.inventory.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = concat([
      {
        Effect   = "Allow"
        Action   = ["secretsmanager:GetSecretValue"]
        Resource = compact([var.control_database_secret_arn, var.target_database_secret_arn])
      },
      {
        Effect   = "Allow"
        Action   = ["s3:GetObject"]
        Resource = concat(var.metadata_evidence_object_arns, var.inventory_object_arns)
      },
      {
        Effect   = "Allow"
        Action   = ["s3:PutObject"]
        Resource = var.inventory_object_arns
      }
      ], var.secret_kms_key_arn == null && var.target_database_secret_kms_key_arn == null ? [] : [{
        Effect   = "Allow"
        Action   = ["kms:Decrypt"]
        Resource = compact([var.secret_kms_key_arn, var.target_database_secret_kms_key_arn])
        }], length(var.s3_kms_key_arns) == 0 ? [] : [{
        Effect   = "Allow"
        Action   = ["kms:Decrypt", "kms:GenerateDataKey"]
        Resource = var.s3_kms_key_arns
    }])
  })
}

resource "aws_lambda_function" "planner" {
  function_name                  = local.planner_name
  role                           = aws_iam_role.planner.arn
  package_type                   = "Image"
  image_uri                      = var.lambda_image_uri
  architectures                  = ["x86_64"]
  memory_size                    = var.planner_memory_mb
  timeout                        = var.planner_timeout_seconds
  reserved_concurrent_executions = var.planner_reserved_concurrency
  tags                           = local.common_tags

  image_config {
    command = ["ingestion.aws_handlers.plan_handler"]
  }

  environment {
    variables = {
      APP_ENVIRONMENT             = "aws"
      RESOURCE_CONFIG_PATH        = "/var/task/resources.yaml"
      CONTROL_DATABASE_SECRET_ARN = var.control_database_secret_arn
    }
  }

  vpc_config {
    subnet_ids         = var.private_subnet_ids
    security_group_ids = var.lambda_security_group_ids
  }

  depends_on = [
    aws_cloudwatch_log_group.planner,
    aws_iam_role_policy_attachment.planner_logs,
    aws_iam_role_policy_attachment.planner_vpc,
    aws_iam_role_policy.planner_data,
  ]
}

resource "aws_lambda_function" "status" {
  function_name                  = local.status_name
  role                           = aws_iam_role.status.arn
  package_type                   = "Image"
  image_uri                      = var.lambda_image_uri
  architectures                  = ["x86_64"]
  memory_size                    = var.status_memory_mb
  timeout                        = var.status_timeout_seconds
  reserved_concurrent_executions = var.status_reserved_concurrency
  tags                           = local.common_tags

  image_config {
    command = ["ingestion.aws_handlers.status_handler"]
  }

  environment {
    variables = {
      APP_ENVIRONMENT             = "aws"
      RESOURCE_CONFIG_PATH        = "/var/task/resources.yaml"
      CONTROL_DATABASE_SECRET_ARN = var.control_database_secret_arn
    }
  }

  vpc_config {
    subnet_ids         = var.private_subnet_ids
    security_group_ids = var.lambda_security_group_ids
  }

  depends_on = [
    aws_cloudwatch_log_group.status,
    aws_iam_role_policy_attachment.status_logs,
    aws_iam_role_policy_attachment.status_vpc,
    aws_iam_role_policy.status_data,
  ]
}

resource "aws_lambda_function" "inventory" {
  function_name                  = local.inventory_name
  role                           = aws_iam_role.inventory.arn
  package_type                   = "Image"
  image_uri                      = var.lambda_image_uri
  architectures                  = ["x86_64"]
  memory_size                    = var.inventory_memory_mb
  timeout                        = var.inventory_timeout_seconds
  reserved_concurrent_executions = var.inventory_reserved_concurrency
  tags                           = local.common_tags

  image_config {
    command = ["ingestion.aws_handlers.inventory_handler"]
  }

  environment {
    variables = merge({
      APP_ENVIRONMENT             = "aws"
      RESOURCE_CONFIG_PATH        = "/var/task/resources.yaml"
      CONTROL_DATABASE_SECRET_ARN = var.control_database_secret_arn
      }, var.target_database_secret_arn == null ? {} : {
      TARGET_DATABASE_SECRET_ARN = var.target_database_secret_arn
    })
  }

  vpc_config {
    subnet_ids         = var.private_subnet_ids
    security_group_ids = var.lambda_security_group_ids
  }

  depends_on = [
    aws_cloudwatch_log_group.inventory,
    aws_iam_role_policy_attachment.inventory_logs,
    aws_iam_role_policy_attachment.inventory_vpc,
    aws_iam_role_policy.inventory_data,
  ]
}

data "aws_iam_policy_document" "workflow_trust" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["states.amazonaws.com"]
    }
    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = [data.aws_caller_identity.current.account_id]
    }
    condition {
      test     = "ArnEquals"
      variable = "aws:SourceArn"
      values   = ["arn:${data.aws_partition.current.partition}:states:${data.aws_region.current.region}:${data.aws_caller_identity.current.account_id}:stateMachine:${local.workflow_name}"]
    }
  }
}

resource "aws_iam_role" "workflow" {
  name               = "${var.name_prefix}-workflow"
  assume_role_policy = data.aws_iam_policy_document.workflow_trust.json
  tags               = local.common_tags
}

resource "aws_iam_role_policy" "workflow" {
  name = "invoke-planner-status-inventory-and-write-logs"
  role = aws_iam_role.workflow.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["lambda:InvokeFunction"]
        Resource = [aws_lambda_function.planner.arn, aws_lambda_function.status.arn, aws_lambda_function.inventory.arn]
      },
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogDelivery", "logs:GetLogDelivery", "logs:UpdateLogDelivery",
          "logs:DeleteLogDelivery", "logs:ListLogDeliveries", "logs:PutResourcePolicy",
          "logs:DescribeResourcePolicies", "logs:DescribeLogGroups"
        ]
        Resource = "*"
      }
    ]
  })
}

resource "aws_sfn_state_machine" "ingestion" {
  name     = local.workflow_name
  type     = "STANDARD"
  role_arn = aws_iam_role.workflow.arn
  definition = templatefile("${path.module}/workflow.asl.json.tftpl", {
    planner_arn     = aws_lambda_function.planner.arn
    status_arn      = aws_lambda_function.status.arn
    inventory_arn   = aws_lambda_function.inventory.arn
    poll_seconds    = local.resources.workflow.poll_seconds
    timeout_seconds = local.workflow_limit
  })
  tags = local.common_tags

  logging_configuration {
    log_destination        = "${aws_cloudwatch_log_group.workflow.arn}:*"
    include_execution_data = false
    level                  = "ERROR"
  }

  lifecycle {
    precondition {
      condition = (
        toset(keys(local.resources.workflow.max_run_seconds)) == toset(["history", "rules", "metadata"])
        && alltrue([for seconds in values(local.resources.workflow.max_run_seconds) : seconds >= local.resources.workflow.poll_seconds])
      )
      error_message = "Reviewed workflow resources must define all three feed deadlines, each at least one poll interval."
    }
  }

  depends_on = [aws_iam_role_policy.workflow]
}

resource "aws_scheduler_schedule_group" "ingestion" {
  name = local.schedule_group
  tags = local.common_tags
}

resource "aws_sqs_queue" "scheduler_dlq" {
  name                      = "${var.name_prefix}-scheduler-dlq"
  message_retention_seconds = 1209600
  sqs_managed_sse_enabled   = true
  tags                      = local.common_tags
}

data "aws_iam_policy_document" "scheduler_trust" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["scheduler.amazonaws.com"]
    }
    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = [data.aws_caller_identity.current.account_id]
    }
    condition {
      test     = "ArnEquals"
      variable = "aws:SourceArn"
      values   = [aws_scheduler_schedule_group.ingestion.arn]
    }
  }
}

resource "aws_iam_role" "scheduler" {
  name               = "${var.name_prefix}-scheduler"
  assume_role_policy = data.aws_iam_policy_document.scheduler_trust.json
  tags               = local.common_tags
}

resource "aws_iam_role_policy" "scheduler" {
  name = "start-ingestion-and-write-delivery-failures"
  role = aws_iam_role.scheduler.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["states:StartExecution"]
        Resource = [aws_sfn_state_machine.ingestion.arn]
      },
      {
        Effect   = "Allow"
        Action   = ["sqs:SendMessage"]
        Resource = [aws_sqs_queue.scheduler_dlq.arn]
      }
    ]
  })
}
