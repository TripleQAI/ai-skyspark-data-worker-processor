resource "aws_cloudwatch_event_bus" "certified" {
  name = local.resources.publication.event_bus
  tags = local.common_tags
}

resource "aws_cloudwatch_log_group" "control" {
  for_each          = local.active_control
  name              = "/ecs/${var.name_prefix}/${each.key}"
  retention_in_days = var.log_retention_days
  tags              = local.common_tags
}

resource "aws_iam_role" "control_execution" {
  for_each           = local.active_control
  name               = "${var.name_prefix}-${each.key}-execution"
  assume_role_policy = data.aws_iam_policy_document.ecs_trust.json
  tags               = local.common_tags
}

resource "aws_iam_role_policy_attachment" "control_execution_base" {
  for_each   = local.active_control
  role       = aws_iam_role.control_execution[each.key].name
  policy_arn = "arn:${data.aws_partition.current.partition}:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

resource "aws_iam_role_policy" "control_execution_secret" {
  for_each = local.active_control
  name     = "read-control-database-secret"
  role     = aws_iam_role.control_execution[each.key].id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = concat([{
      Effect   = "Allow"
      Action   = ["secretsmanager:GetSecretValue"]
      Resource = [var.control_database_secret_arn]
      }], length(var.control_database_kms_key_arns) == 0 ? [] : [{
      Effect   = "Allow"
      Action   = ["kms:Decrypt"]
      Resource = var.control_database_kms_key_arns
    }])
  })
}

resource "aws_iam_role" "control_task" {
  for_each           = local.active_control
  name               = "${var.name_prefix}-${each.key}-task"
  assume_role_policy = data.aws_iam_policy_document.ecs_trust.json
  tags               = local.common_tags
}

resource "aws_iam_role_policy" "dispatcher" {
  for_each = var.enable_control_services ? { dispatcher = true } : {}
  name     = "send-reviewed-job-references"
  role     = aws_iam_role.control_task[each.key].id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["sqs:GetQueueUrl", "sqs:SendMessage"]
      Resource = [for queue in aws_sqs_queue.work : queue.arn]
    }]
  })
}

resource "aws_iam_role_policy" "publisher" {
  for_each = var.enable_control_services ? { publisher = true } : {}
  name     = "publish-certified-batch-references"
  role     = aws_iam_role.control_task[each.key].id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["events:DescribeEventBus", "events:PutEvents"]
      Resource = [aws_cloudwatch_event_bus.certified.arn]
    }]
  })
}

resource "aws_ecs_task_definition" "control" {
  for_each                 = local.active_control
  family                   = "${var.name_prefix}-${each.key}"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = var.control_capacity[each.key].cpu_units
  memory                   = var.control_capacity[each.key].memory_mb
  execution_role_arn       = aws_iam_role.control_execution[each.key].arn
  task_role_arn            = aws_iam_role.control_task[each.key].arn
  tags                     = local.common_tags

  runtime_platform {
    operating_system_family = "LINUX"
    cpu_architecture        = "X86_64"
  }

  container_definitions = jsonencode([{
    name        = each.key
    image       = var.control_image_uri
    essential   = true
    entryPoint  = ["skyspark-control"]
    command     = [each.value, "--environment", "aws", "--resources", "/app/resources.yaml"]
    stopTimeout = 120
    environment = [
      { name = "APP_ENVIRONMENT", value = "aws" },
      { name = "RESOURCE_CONFIG_PATH", value = "/app/resources.yaml" },
      { name = "AWS_REGION", value = data.aws_region.current.region },
    ]
    secrets = [{
      name      = "CONTROL_DATABASE_URL"
      valueFrom = "${var.control_database_secret_arn}:dsn::"
    }]
    logConfiguration = {
      logDriver = "awslogs"
      options = {
        awslogs-group         = aws_cloudwatch_log_group.control[each.key].name
        awslogs-region        = data.aws_region.current.region
        awslogs-stream-prefix = each.key
      }
    }
  }])

  depends_on = [
    aws_iam_role_policy_attachment.control_execution_base,
    aws_iam_role_policy.control_execution_secret,
    aws_iam_role_policy.dispatcher,
    aws_iam_role_policy.publisher,
  ]
}

resource "aws_ecs_service" "control" {
  for_each                           = local.active_control
  name                               = "${var.name_prefix}-${each.key}"
  cluster                            = aws_ecs_cluster.workers.id
  task_definition                    = aws_ecs_task_definition.control[each.key].arn
  desired_count                      = var.control_capacity[each.key].desired_tasks
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
}

resource "aws_cloudwatch_metric_alarm" "control_task_count" {
  for_each            = local.active_control
  alarm_name          = "${var.name_prefix}-${each.key}-running-tasks"
  comparison_operator = "LessThanThreshold"
  evaluation_periods  = 2
  metric_name         = "RunningTaskCount"
  namespace           = "ECS/ContainerInsights"
  period              = 60
  statistic           = "Minimum"
  threshold           = var.control_capacity[each.key].desired_tasks
  treat_missing_data  = "breaching"
  alarm_actions       = var.alarm_sns_topic_arns
  tags                = local.common_tags
  dimensions = {
    ClusterName = aws_ecs_cluster.workers.name
    ServiceName = aws_ecs_service.control[each.key].name
  }
}
