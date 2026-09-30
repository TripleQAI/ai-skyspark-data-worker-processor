variable "name_prefix" {
  type        = string
  description = "Unique prefix for the shared worker deployment."

  validation {
    condition     = can(regex("^[a-z][a-z0-9-]{2,28}$", var.name_prefix))
    error_message = "name_prefix must be 3-29 lowercase letters, digits, or hyphens."
  }
}

variable "resource_config_file" {
  type        = string
  description = "Reviewed resources.yaml used to build the worker image."
}

variable "worker_image_uri" {
  type        = string
  description = "Approved ECR worker image URI pinned by sha256 digest."

  validation {
    condition     = can(regex("@sha256:[0-9a-f]{64}$", var.worker_image_uri))
    error_message = "worker_image_uri must be digest pinned."
  }
}

variable "control_image_uri" {
  type        = string
  default     = ""
  description = "Approved digest-pinned ECR image for dispatch, publication, and recovery services. Required when enabled."

  validation {
    condition     = var.control_image_uri == "" || can(regex("@sha256:[0-9a-f]{64}$", var.control_image_uri))
    error_message = "control_image_uri must be empty or digest pinned."
  }
}

variable "enable_control_services" {
  type        = bool
  default     = false
  description = "Explicit gate for dispatcher, publisher, and stale-recovery services."
}

variable "control_capacity" {
  type = map(object({
    cpu_units     = number
    memory_mb     = number
    desired_tasks = number
  }))
  default = {
    dispatcher = { cpu_units = 512, memory_mb = 1024, desired_tasks = 4 }
    publisher  = { cpu_units = 512, memory_mb = 1024, desired_tasks = 2 }
    recovery   = { cpu_units = 256, memory_mb = 512, desired_tasks = 1 }
  }
  description = "Pilot task counts for each control role; measure throughput before estate rollout."

  validation {
    condition = (
      toset(keys(var.control_capacity)) == toset(["dispatcher", "publisher", "recovery"]) &&
      alltrue([for c in values(var.control_capacity) :
        c.cpu_units >= 256 && c.memory_mb >= 512 && c.desired_tasks >= 1
      ])
    )
    error_message = "Provide positive Fargate capacity for dispatcher, publisher, and recovery."
  }
}

variable "control_database_secret_arn" {
  type        = string
  description = "Existing JSON secret with a dsn field for the control database."
}

variable "target_database_secret_arn" {
  type        = string
  description = "Existing JSON secret with a dsn field for the TimescaleDB target."
}

variable "source_secret_arns" {
  type        = map(string)
  default     = {}
  description = "Approved script environment variable names mapped to existing secret ARNs."

  validation {
    condition     = alltrue([for name in keys(var.source_secret_arns) : can(regex("^[A-Z][A-Z0-9_]*$", name))])
    error_message = "Source secret environment names must be uppercase identifiers."
  }
}

variable "secret_kms_key_arns" {
  type        = list(string)
  default     = []
  description = "Customer managed KMS keys for injected secrets, if applicable."
}

variable "control_database_kms_key_arns" {
  type        = list(string)
  default     = []
  description = "Only the customer managed KMS keys used for the control database secret."
}

variable "s3_kms_key_arns" {
  type        = list(string)
  default     = []
  description = "Customer managed KMS keys for raw and certified objects, if applicable."
}

variable "raw_object_arns" {
  type        = list(string)
  description = "Approved raw S3 object ARN patterns across the workers' authorized scope."

  validation {
    condition = length(var.raw_object_arns) > 0 && alltrue([
      for arn in var.raw_object_arns : can(regex("^arn:(aws|aws-us-gov|aws-cn):s3:::[a-z0-9.-]+/raw/[A-Za-z0-9_-]+/[A-Za-z0-9_-]+/\\*$", arn))
    ])
    error_message = "Raw object ARNs must name explicit raw/tenant/project/* prefixes."
  }
}

variable "certified_object_arns" {
  type        = list(string)
  description = "Approved certified S3 object ARN patterns across the workers' authorized scope."

  validation {
    condition = length(var.certified_object_arns) > 0 && alltrue([
      for arn in var.certified_object_arns : can(regex("^arn:(aws|aws-us-gov|aws-cn):s3:::[a-z0-9.-]+/certified/[A-Za-z0-9_-]+/[A-Za-z0-9_-]+/\\*$", arn))
    ])
    error_message = "Certified object ARNs must name explicit certified/tenant/project/* prefixes."
  }
}

variable "approved_scopes" {
  type        = set(string)
  description = "Explicit tenant/project pairs authorized for this shared worker deployment."

  validation {
    condition = length(var.approved_scopes) > 0 && alltrue([
      for scope in var.approved_scopes : can(regex("^[A-Za-z0-9_-]+/[A-Za-z0-9_-]+$", scope))
    ])
    error_message = "approved_scopes must contain explicit tenant/project pairs."
  }
}

variable "private_subnet_ids" {
  type        = list(string)
  description = "Existing private subnets with database and required AWS service connectivity."

  validation {
    condition     = length(var.private_subnet_ids) >= 2
    error_message = "At least two private subnets are required."
  }
}

variable "worker_security_group_ids" {
  type        = list(string)
  description = "Existing security groups permitting required source and database connections."

  validation {
    condition     = length(var.worker_security_group_ids) > 0
    error_message = "At least one worker security group is required."
  }
}

variable "capacity_by_queue" {
  type = map(object({
    cpu_units               = number
    memory_mb               = number
    min_tasks               = number
    max_tasks               = number
    backlog_target          = number
    queue_age_alarm_seconds = number
  }))
  description = "Pilot capacity and alarm settings for every configured work queue."

  validation {
    condition = alltrue([
      for c in values(var.capacity_by_queue) :
      c.min_tasks >= 1 && c.max_tasks >= c.min_tasks &&
      c.backlog_target > 0 && c.queue_age_alarm_seconds >= 60
    ])
    error_message = "Each queue needs positive min/max tasks, backlog target, and queue age threshold."
  }
}

variable "enable_workers" {
  type        = bool
  default     = false
  description = "Explicit pilot gate; services otherwise stay at zero tasks with no scaling target."
}

variable "log_retention_days" {
  type        = number
  default     = 30
  description = "Retention for worker CloudWatch log groups."
}

variable "alarm_sns_topic_arns" {
  type        = list(string)
  default     = []
  description = "Existing notification targets for queue age and DLQ alarms."
}

variable "tags" {
  type        = map(string)
  default     = {}
  description = "Organization tags to apply to created resources."
}
