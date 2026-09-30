output "ecs_cluster_name" {
  value = aws_ecs_cluster.workers.name
}

output "work_queue_urls" {
  value = { for name, queue in aws_sqs_queue.work : name => queue.id }
}

output "dlq_urls" {
  value = { for name, queue in aws_sqs_queue.dlq : name => queue.id }
}

output "service_names" {
  value = { for name, service in aws_ecs_service.worker : name => service.name }
}

output "publication_event_bus_arn" {
  value = aws_cloudwatch_event_bus.certified.arn
}

output "control_service_names" {
  value = { for name, service in aws_ecs_service.control : name => service.name }
}

output "worker_scale_in_protection_enabled" {
  value = local.protection_enabled
}

output "worker_task_role_arns" {
  value = { for name, role in aws_iam_role.task : name => role.arn }
}
