resource "aws_cloudwatch_dashboard" "workers" {
  dashboard_name = "${var.name_prefix}-workers"
  dashboard_body = jsonencode({
    widgets = [
      {
        type = "metric"
        x    = 0, y = 0, width = 12, height = 6
        properties = {
          title   = "Visible work by queue"
          region  = data.aws_region.current.region
          stat    = "Sum"
          period  = 60
          metrics = [for name in local.resources.work_queues : ["AWS/SQS", "ApproximateNumberOfMessagesVisible", "QueueName", aws_sqs_queue.work[name].name]]
        }
      },
      {
        type = "metric"
        x    = 12, y = 0, width = 12, height = 6
        properties = {
          title   = "Oldest work age (seconds)"
          region  = data.aws_region.current.region
          stat    = "Maximum"
          period  = 60
          metrics = [for name in local.resources.work_queues : ["AWS/SQS", "ApproximateAgeOfOldestMessage", "QueueName", aws_sqs_queue.work[name].name]]
        }
      },
      {
        type = "metric"
        x    = 0, y = 6, width = 12, height = 6
        properties = {
          title   = "Dead-letter queue depth"
          region  = data.aws_region.current.region
          stat    = "Sum"
          period  = 60
          metrics = [for name in local.resources.work_queues : ["AWS/SQS", "ApproximateNumberOfMessagesVisible", "QueueName", aws_sqs_queue.dlq[name].name]]
        }
      },
      {
        type = "metric"
        x    = 12, y = 6, width = 12, height = 6
        properties = {
          title  = "Running Fargate worker tasks"
          region = data.aws_region.current.region
          stat   = "Average"
          period = 60
          metrics = [for name in local.resources.work_queues : [
            "ECS/ContainerInsights", "RunningTaskCount",
            "ClusterName", aws_ecs_cluster.workers.name,
            "ServiceName", aws_ecs_service.worker[name].name
          ]]
        }
      }
    ]
  })
}
