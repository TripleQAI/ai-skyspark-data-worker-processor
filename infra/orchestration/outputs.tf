output "state_machine_arn" {
  value = aws_sfn_state_machine.ingestion.arn
}

output "schedule_group_name" {
  value = aws_scheduler_schedule_group.ingestion.name
}

output "scheduler_role_arn" {
  value = aws_iam_role.scheduler.arn
}

output "scheduler_dlq_arn" {
  value = aws_sqs_queue.scheduler_dlq.arn
}

output "planner_function_arn" {
  value = aws_lambda_function.planner.arn
}

output "status_function_arn" {
  value = aws_lambda_function.status.arn
}

output "inventory_function_arn" {
  value = aws_lambda_function.inventory.arn
}
