# Drop the reserved-concurrency setting on all three Lambdas.
#
# WHY. This AWS account's total Lambda concurrency is 10 -- the unraised
# new-account default, not the usual 1000 (verified with
# `aws lambda get-account-settings`). AWS additionally requires 10 to remain
# UNRESERVED, so the account has zero reservable concurrency and ANY positive
# reserved_concurrent_executions fails:
#
#   InvalidParameterValueException: Specified ReservedConcurrentExecutions for
#   function decreases account's UnreservedConcurrentExecution below its
#   minimum value of [10].
#
# The module's variables validate >= 1, so the value cannot be lowered to zero
# through tfvars. Terraform merges *_override.tf over the base configuration,
# so this removes the attribute without editing reviewed code -- delete this
# file once the account quota is raised.
#
# WHAT IS LOST. Reserved concurrency is a per-function CEILING, protecting the
# control store from a planner storm opening more connections than Postgres
# allows. Without it the three functions share the account's pool of 10, which
# is itself a tighter bound than the 20/40/2 the module wanted -- so the
# protection still exists, just account-wide rather than per function.
#
# Request the quota increase in parallel: Service Quotas ->
# "Concurrent executions" (L-B99A9384) -> 1000. Until it is granted, a
# 1,000-site run is bounded by this 10, not by the worker fleet.
resource "aws_lambda_function" "planner" {
  reserved_concurrent_executions = -1
}

resource "aws_lambda_function" "status" {
  reserved_concurrent_executions = -1
}

resource "aws_lambda_function" "inventory" {
  reserved_concurrent_executions = -1
}
