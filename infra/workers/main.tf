data "aws_partition" "current" {}
data "aws_region" "current" {}
data "aws_caller_identity" "current" {}

locals {
  resources          = yamldecode(file(var.resource_config_file))
  queue_names        = toset(local.resources.work_queues)
  queues             = { for name in local.resources.work_queues : name => name }
  active             = var.enable_workers ? local.queues : {}
  protection_enabled = local.resources.worker.scale_in_protection.enabled
  control_commands = {
    dispatcher = "dispatch-service"
    publisher  = "publication-service"
    recovery   = "recovery-service"
  }
  active_control = var.enable_control_services ? local.control_commands : {}
  common_tags    = merge(var.tags, { Application = "skyspark-ingestion" })
  secret_arns = concat(
    [var.control_database_secret_arn, var.target_database_secret_arn],
    values(var.source_secret_arns),
  )
}

resource "aws_ecs_cluster" "workers" {
  name = "${var.name_prefix}-workers"
  tags = local.common_tags

  setting {
    name  = "containerInsights"
    value = "enabled"
  }

  lifecycle {
    precondition {
      condition = (
        length(local.queue_names) == 4 &&
        contains(local.queue_names, local.resources.backfill_queue) &&
        toset(keys(local.resources.feed_routes)) == toset(["history", "rules", "metadata"]) &&
        length(toset(values(local.resources.feed_routes))) == 3 &&
        alltrue([for route in values(local.resources.feed_routes) : contains(local.queue_names, route)]) &&
        toset(keys(var.capacity_by_queue)) == local.queue_names
      )
      error_message = "The reviewed resources need three feed routes, one backfill queue, and capacity for all four queues."
    }
    precondition {
      condition     = !var.enable_control_services || var.control_image_uri != ""
      error_message = "Enabling control services requires a digest-pinned control_image_uri."
    }
    precondition {
      condition = (
        alltrue([for arn in var.raw_object_arns :
          anytrue([for scope in var.approved_scopes : endswith(arn, "/raw/${scope}/*")])
        ]) &&
        alltrue([for arn in var.certified_object_arns :
          anytrue([for scope in var.approved_scopes : endswith(arn, "/certified/${scope}/*")])
        ])
      )
      error_message = "Every worker S3 object prefix must belong to an approved tenant/project scope."
    }
  }
}

resource "aws_sqs_queue" "dlq" {
  for_each                  = local.queues
  name                      = "${each.key}_dlq"
  message_retention_seconds = 1209600
  sqs_managed_sse_enabled   = true
  tags                      = local.common_tags
}

resource "aws_sqs_queue" "work" {
  for_each                   = local.queues
  name                       = each.key
  visibility_timeout_seconds = local.resources.worker.visibility_seconds
  receive_wait_time_seconds  = local.resources.worker.long_poll_seconds
  message_retention_seconds  = 1209600
  sqs_managed_sse_enabled    = true
  tags                       = local.common_tags
}

resource "aws_sqs_queue_redrive_allow_policy" "dlq" {
  for_each  = local.queues
  queue_url = aws_sqs_queue.dlq[each.key].id
  redrive_allow_policy = jsonencode({
    redrivePermission = "byQueue"
    sourceQueueArns   = [aws_sqs_queue.work[each.key].arn]
  })
}

resource "aws_sqs_queue_redrive_policy" "work" {
  for_each  = local.queues
  queue_url = aws_sqs_queue.work[each.key].id
  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.dlq[each.key].arn
    maxReceiveCount     = local.resources.max_receive_count
  })
  depends_on = [aws_sqs_queue_redrive_allow_policy.dlq]
}

resource "aws_cloudwatch_log_group" "worker" {
  for_each          = local.queues
  name              = "/ecs/${var.name_prefix}/${each.key}"
  retention_in_days = var.log_retention_days
  tags              = local.common_tags
}

data "aws_iam_policy_document" "ecs_trust" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ecs-tasks.amazonaws.com"]
    }
    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = [data.aws_caller_identity.current.account_id]
    }
    condition {
      test     = "ArnLike"
      variable = "aws:SourceArn"
      values   = ["arn:${data.aws_partition.current.partition}:ecs:${data.aws_region.current.region}:${data.aws_caller_identity.current.account_id}:*"]
    }
  }
}

resource "aws_iam_role" "execution" {
  for_each           = local.queues
  name               = "${var.name_prefix}-${each.key}-execution"
  assume_role_policy = data.aws_iam_policy_document.ecs_trust.json
  tags               = local.common_tags
}

resource "aws_iam_role_policy_attachment" "execution_base" {
  for_each   = local.queues
  role       = aws_iam_role.execution[each.key].name
  policy_arn = "arn:${data.aws_partition.current.partition}:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

resource "aws_iam_role_policy" "execution_secrets" {
  for_each = local.queues
  name     = "read-reviewed-worker-secrets"
  role     = aws_iam_role.execution[each.key].id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = concat([{
      Effect   = "Allow"
      Action   = ["secretsmanager:GetSecretValue"]
      Resource = local.secret_arns
      }], length(var.secret_kms_key_arns) == 0 ? [] : [{
      Effect   = "Allow"
      Action   = ["kms:Decrypt"]
      Resource = var.secret_kms_key_arns
    }])
  })
}

resource "aws_iam_role" "task" {
  for_each           = local.queues
  name               = "${var.name_prefix}-${each.key}-task"
  assume_role_policy = data.aws_iam_policy_document.ecs_trust.json
  tags               = local.common_tags
}

resource "aws_iam_role_policy" "task_data" {
  for_each = local.queues
  name     = "consume-own-queue-and-write-scoped-objects"
  role     = aws_iam_role.task[each.key].id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = concat([
      {
        Effect = "Allow"
        Action = [
          "sqs:GetQueueUrl", "sqs:GetQueueAttributes", "sqs:ReceiveMessage",
          "sqs:DeleteMessage", "sqs:ChangeMessageVisibility"
        ]
        Resource = [aws_sqs_queue.work[each.key].arn]
      },
      {
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:PutObject"]
        Resource = concat(var.raw_object_arns, var.certified_object_arns)
      }
      ], length(var.s3_kms_key_arns) == 0 ? [] : [{
        Effect   = "Allow"
        Action   = ["kms:Decrypt", "kms:Encrypt", "kms:GenerateDataKey"]
        Resource = var.s3_kms_key_arns
    }])
  })
}

resource "aws_iam_role_policy" "task_protection" {
  for_each = var.enable_workers && local.protection_enabled ? local.queues : {}
  name     = "protect-active-worker-task"
  role     = aws_iam_role.task[each.key].id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["ecs:GetTaskProtection", "ecs:UpdateTaskProtection"]
      Resource = ["arn:${data.aws_partition.current.partition}:ecs:${data.aws_region.current.region}:${data.aws_caller_identity.current.account_id}:task/${aws_ecs_cluster.workers.name}/*"]
    }]
  })
}

resource "aws_ecs_task_definition" "worker" {
  for_each                 = local.queues
  family                   = "${var.name_prefix}-${each.key}"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = var.capacity_by_queue[each.key].cpu_units
  memory                   = var.capacity_by_queue[each.key].memory_mb
  execution_role_arn       = aws_iam_role.execution[each.key].arn
  task_role_arn            = aws_iam_role.task[each.key].arn
  tags                     = local.common_tags

  runtime_platform {
    operating_system_family = "LINUX"
    cpu_architecture        = "X86_64"
  }

  container_definitions = jsonencode([{
    name       = "worker"
    image      = var.worker_image_uri
    essential  = true
    entryPoint = ["skyspark-control"]
    command = [
      "worker", "--environment", "aws", "--resources", "/app/resources.yaml",
      "--script-root", "/app/scripts", "--queue-class", each.key,
    ]
    stopTimeout = 120
    environment = [
      { name = "APP_ENVIRONMENT", value = "aws" },
      { name = "RESOURCE_CONFIG_PATH", value = "/app/resources.yaml" },
      { name = "AWS_REGION", value = data.aws_region.current.region },
    ]
    secrets = concat([
      {
        name      = "CONTROL_DATABASE_URL"
        valueFrom = "${var.control_database_secret_arn}:dsn::"
      },
      {
        name      = "TARGET_DATABASE_URL"
        valueFrom = "${var.target_database_secret_arn}:dsn::"
      }
      ], [for name, arn in var.source_secret_arns : {
        name      = name
        valueFrom = arn
    }])
    logConfiguration = {
      logDriver = "awslogs"
      options = {
        awslogs-group         = aws_cloudwatch_log_group.worker[each.key].name
        awslogs-region        = data.aws_region.current.region
        awslogs-stream-prefix = "worker"
      }
    }
  }])

  depends_on = [
    aws_iam_role_policy_attachment.execution_base,
    aws_iam_role_policy.execution_secrets,
    aws_iam_role_policy.task_data,
    aws_iam_role_policy.task_protection,
  ]
}

resource "aws_ecs_service" "worker" {
  for_each                           = local.queues
  name                               = each.key
  cluster                            = aws_ecs_cluster.workers.id
  task_definition                    = aws_ecs_task_definition.worker[each.key].arn
  desired_count                      = var.enable_workers ? var.capacity_by_queue[each.key].min_tasks : 0
  launch_type                        = "FARGATE"
  platform_version                   = "LATEST"
  deployment_minimum_healthy_percent = 100
  deployment_maximum_percent         = 200
  tags                               = local.common_tags

  deployment_circuit_breaker {
    enable   = true
    rollback = true
  }

  network_configuration {
    subnets          = var.private_subnet_ids
    security_groups  = var.worker_security_group_ids
    assign_public_ip = false
  }

  depends_on = [aws_sqs_queue_redrive_policy.work]
}

resource "aws_appautoscaling_target" "worker" {
  for_each           = local.active
  max_capacity       = var.capacity_by_queue[each.key].max_tasks
  min_capacity       = var.capacity_by_queue[each.key].min_tasks
  resource_id        = "service/${aws_ecs_cluster.workers.name}/${aws_ecs_service.worker[each.key].name}"
  scalable_dimension = "ecs:service:DesiredCount"
  service_namespace  = "ecs"
}

resource "aws_appautoscaling_policy" "backlog" {
  for_each           = local.active
  name               = "${var.name_prefix}-${each.key}-backlog-per-task"
  policy_type        = "TargetTrackingScaling"
  resource_id        = aws_appautoscaling_target.worker[each.key].resource_id
  scalable_dimension = aws_appautoscaling_target.worker[each.key].scalable_dimension
  service_namespace  = aws_appautoscaling_target.worker[each.key].service_namespace

  target_tracking_scaling_policy_configuration {
    target_value       = var.capacity_by_queue[each.key].backlog_target
    scale_out_cooldown = 60
    scale_in_cooldown  = 300
    disable_scale_in   = !local.protection_enabled

    customized_metric_specification {
      metrics {
        id          = "m1"
        return_data = false
        metric_stat {
          stat = "Sum"
          metric {
            metric_name = "ApproximateNumberOfMessagesVisible"
            namespace   = "AWS/SQS"
            dimensions {
              name  = "QueueName"
              value = aws_sqs_queue.work[each.key].name
            }
          }
        }
      }
      metrics {
        id          = "m2"
        return_data = false
        metric_stat {
          stat = "Average"
          metric {
            metric_name = "RunningTaskCount"
            namespace   = "ECS/ContainerInsights"
            dimensions {
              name  = "ClusterName"
              value = aws_ecs_cluster.workers.name
            }
            dimensions {
              name  = "ServiceName"
              value = aws_ecs_service.worker[each.key].name
            }
          }
        }
      }
      metrics {
        id          = "e1"
        expression  = "m1 / m2"
        return_data = true
      }
    }
  }
}

resource "aws_cloudwatch_metric_alarm" "queue_age" {
  for_each            = local.queues
  alarm_name          = "${var.name_prefix}-${each.key}-age"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 2
  metric_name         = "ApproximateAgeOfOldestMessage"
  namespace           = "AWS/SQS"
  period              = 60
  statistic           = "Maximum"
  threshold           = var.capacity_by_queue[each.key].queue_age_alarm_seconds
  treat_missing_data  = "notBreaching"
  alarm_actions       = var.alarm_sns_topic_arns
  tags                = local.common_tags
  dimensions = {
    QueueName = aws_sqs_queue.work[each.key].name
  }
}

resource "aws_cloudwatch_metric_alarm" "dlq_visible" {
  for_each            = local.queues
  alarm_name          = "${var.name_prefix}-${each.key}-dlq"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "ApproximateNumberOfMessagesVisible"
  namespace           = "AWS/SQS"
  period              = 60
  statistic           = "Maximum"
  threshold           = 0
  treat_missing_data  = "notBreaching"
  alarm_actions       = var.alarm_sns_topic_arns
  tags                = local.common_tags
  dimensions = {
    QueueName = aws_sqs_queue.dlq[each.key].name
  }
}
