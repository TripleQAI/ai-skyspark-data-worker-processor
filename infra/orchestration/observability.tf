locals {
  lambda_functions = {
    planner   = aws_lambda_function.planner
    status    = aws_lambda_function.status
    inventory = aws_lambda_function.inventory
  }
}

resource "aws_cloudwatch_metric_alarm" "scheduler_dlq" {
  alarm_name          = "${var.name_prefix}-scheduler-dlq"
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
    QueueName = aws_sqs_queue.scheduler_dlq.name
  }
}

resource "aws_cloudwatch_metric_alarm" "workflow_failed" {
  alarm_name          = "${var.name_prefix}-workflow-failed"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "ExecutionsFailed"
  namespace           = "AWS/States"
  period              = 60
  statistic           = "Sum"
  threshold           = 0
  treat_missing_data  = "notBreaching"
  alarm_actions       = var.alarm_sns_topic_arns
  tags                = local.common_tags
  dimensions = {
    StateMachineArn = aws_sfn_state_machine.ingestion.arn
  }
}

resource "aws_cloudwatch_metric_alarm" "lambda_errors" {
  for_each            = local.lambda_functions
  alarm_name          = "${var.name_prefix}-${each.key}-errors"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "Errors"
  namespace           = "AWS/Lambda"
  period              = 60
  statistic           = "Sum"
  threshold           = 0
  treat_missing_data  = "notBreaching"
  alarm_actions       = var.alarm_sns_topic_arns
  tags                = local.common_tags
  dimensions = {
    FunctionName = each.value.function_name
  }
}

resource "aws_cloudwatch_metric_alarm" "lambda_throttles" {
  for_each            = local.lambda_functions
  alarm_name          = "${var.name_prefix}-${each.key}-throttles"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "Throttles"
  namespace           = "AWS/Lambda"
  period              = 60
  statistic           = "Sum"
  threshold           = 0
  treat_missing_data  = "notBreaching"
  alarm_actions       = var.alarm_sns_topic_arns
  tags                = local.common_tags
  dimensions = {
    FunctionName = each.value.function_name
  }
}

resource "aws_cloudwatch_dashboard" "orchestration" {
  dashboard_name = "${var.name_prefix}-orchestration"
  dashboard_body = jsonencode({
    widgets = [
      {
        type = "metric"
        x    = 0, y = 0, width = 12, height = 6
        properties = {
          title  = "Workflow outcomes"
          region = data.aws_region.current.region
          period = 60
          stat   = "Sum"
          metrics = [
            ["AWS/States", "ExecutionsStarted", "StateMachineArn", aws_sfn_state_machine.ingestion.arn],
            ["AWS/States", "ExecutionsSucceeded", "StateMachineArn", aws_sfn_state_machine.ingestion.arn],
            ["AWS/States", "ExecutionsFailed", "StateMachineArn", aws_sfn_state_machine.ingestion.arn]
          ]
        }
      },
      {
        type = "metric"
        x    = 12, y = 0, width = 12, height = 6
        properties = {
          title  = "Planner and status function errors"
          region = data.aws_region.current.region
          period = 60
          stat   = "Sum"
          metrics = [for name, function in local.lambda_functions : [
            "AWS/Lambda", "Errors", "FunctionName", function.function_name
          ]]
        }
      },
      {
        type = "metric"
        x    = 0, y = 6, width = 12, height = 6
        properties = {
          title   = "Scheduler delivery failures"
          region  = data.aws_region.current.region
          period  = 60
          stat    = "Maximum"
          metrics = [["AWS/SQS", "ApproximateNumberOfMessagesVisible", "QueueName", aws_sqs_queue.scheduler_dlq.name]]
        }
      }
    ]
  })
}
