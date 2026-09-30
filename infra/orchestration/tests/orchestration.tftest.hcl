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
      json = "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Action\":\"sts:AssumeRole\",\"Principal\":{\"Service\":\"lambda.amazonaws.com\"}}]}"
    }
  }
}

variables {
  name_prefix                   = "pilot-skyspark"
  resource_config_file          = "../../local/resources.yaml"
  lambda_image_uri              = "123456789012.dkr.ecr.us-east-1.amazonaws.com/pilot/lambda@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
  control_database_secret_arn   = "arn:aws:secretsmanager:us-east-1:123456789012:secret:control"
  target_database_secret_arn    = "arn:aws:secretsmanager:us-east-1:123456789012:secret:target"
  config_object_arns            = ["arn:aws:s3:::pilot-certified/config/tenant-a/project-a/*"]
  inventory_object_arns         = ["arn:aws:s3:::pilot-certified/inventory/tenant-a/project-a/*"]
  metadata_evidence_object_arns = ["arn:aws:s3:::pilot-certified/certified/tenant-a/project-a/*"]
  private_subnet_ids            = ["subnet-11111111", "subnet-22222222"]
  lambda_security_group_ids     = ["sg-11111111"]
}

run "timescale_inventory_secret_is_scoped" {
  command = plan

  assert {
    condition = (
      aws_lambda_function.inventory.environment[0].variables.TARGET_DATABASE_SECRET_ARN == var.target_database_secret_arn &&
      !contains(keys(aws_lambda_function.planner.environment[0].variables), "TARGET_DATABASE_SECRET_ARN")
    )
    error_message = "Only the inventory publisher should receive the optional target secret reference."
  }

  assert {
    condition = contains(
      jsondecode(aws_iam_role_policy.inventory_data.policy).Statement[0].Resource,
      var.target_database_secret_arn
    )
    error_message = "Inventory IAM must allow its configured TimescaleDB secret."
  }

  assert {
    condition = (
      length(aws_cloudwatch_metric_alarm.lambda_errors) == 3 &&
      length(aws_cloudwatch_metric_alarm.lambda_throttles) == 3 &&
      aws_cloudwatch_dashboard.orchestration.dashboard_name == "pilot-skyspark-orchestration"
    )
    error_message = "Scheduler, workflow, and Lambda failure visibility must be present."
  }
}

run "s3_inventory_omits_target_secret" {
  command = plan
  variables {
    target_database_secret_arn = null
  }

  assert {
    condition     = !contains(keys(aws_lambda_function.inventory.environment[0].variables), "TARGET_DATABASE_SECRET_ARN")
    error_message = "S3-only inventory must not receive an unused target secret."
  }
}
