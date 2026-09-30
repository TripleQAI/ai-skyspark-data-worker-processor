mock_provider "aws" {
  mock_data "aws_partition" {
    defaults = { partition = "aws" }
  }
  mock_data "aws_region" {
    defaults = { region = "us-east-1" }
  }
  mock_data "aws_caller_identity" {
    defaults = { account_id = "123456789012" }
  }
  mock_data "aws_iam_policy_document" {
    defaults = {
      json = "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Action\":\"sts:AssumeRole\",\"Principal\":{\"Service\":\"ecs-tasks.amazonaws.com\"}}]}"
    }
  }
}

variables {
  name_prefix                 = "pilot-skyspark"
  resource_config_file        = "../../local/resources.yaml"
  worker_image_uri            = "123456789012.dkr.ecr.us-east-1.amazonaws.com/pilot/worker@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
  control_database_secret_arn = "arn:aws:secretsmanager:us-east-1:123456789012:secret:control"
  target_database_secret_arn  = "arn:aws:secretsmanager:us-east-1:123456789012:secret:target"
  raw_object_arns             = ["arn:aws:s3:::pilot-raw/raw/tenant-a/project-a/*"]
  certified_object_arns       = ["arn:aws:s3:::pilot-certified/certified/tenant-a/project-a/*"]
  approved_scopes             = ["tenant-a/project-a"]
  private_subnet_ids          = ["subnet-11111111", "subnet-22222222"]
  worker_security_group_ids   = ["sg-11111111"]
  capacity_by_queue = {
    history_live   = { cpu_units = 1024, memory_mb = 2048, min_tasks = 1, max_tasks = 4, backlog_target = 12, queue_age_alarm_seconds = 300 }
    rules_nightly  = { cpu_units = 512, memory_mb = 1024, min_tasks = 1, max_tasks = 2, backlog_target = 12, queue_age_alarm_seconds = 900 }
    metadata_sweep = { cpu_units = 512, memory_mb = 1024, min_tasks = 1, max_tasks = 2, backlog_target = 12, queue_age_alarm_seconds = 1800 }
    backfill       = { cpu_units = 512, memory_mb = 1024, min_tasks = 1, max_tasks = 1, backlog_target = 12, queue_age_alarm_seconds = 3600 }
  }
}

run "one_queue_and_service_per_class" {
  command = plan

  assert {
    condition = (
      length(aws_sqs_queue.work) == 4 && length(aws_sqs_queue.dlq) == 4 &&
      length(aws_ecs_service.worker) == 4
    )
    error_message = "Four independent work queues, DLQs, and services are required."
  }

  assert {
    condition     = alltrue([for service in values(aws_ecs_service.worker) : service.desired_count == 0])
    error_message = "Workers must remain disabled until AWS sandbox inputs are approved."
  }

  assert {
    condition = (
      length(jsondecode(aws_cloudwatch_dashboard.workers.dashboard_body).widgets) == 4 &&
      length(aws_cloudwatch_metric_alarm.queue_age) == 4 &&
      length(aws_cloudwatch_metric_alarm.dlq_visible) == 4
    )
    error_message = "Every worker queue needs backlog, age, DLQ, and task visibility."
  }

  assert {
    condition = contains(
      [for item in jsondecode(aws_ecs_task_definition.worker["metadata_sweep"].container_definitions)[0].environment : item.name],
      "RESOURCE_CONFIG_PATH"
    )
    error_message = "Reviewed worker scripts need the packaged resource file path."
  }
}

run "reject_broad_raw_object_permission" {
  command = plan
  variables {
    raw_object_arns = ["arn:aws:s3:::pilot-raw/raw/*"]
  }
  expect_failures = [var.raw_object_arns]
}

run "reject_unapproved_tenant_prefix" {
  command = plan
  variables {
    certified_object_arns = ["arn:aws:s3:::pilot-certified/certified/tenant-b/project-b/*"]
  }
  expect_failures = [aws_ecs_cluster.workers]
}
