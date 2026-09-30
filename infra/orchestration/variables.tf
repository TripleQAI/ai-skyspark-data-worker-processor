variable "name_prefix" {
  type        = string
  description = "Unique, approved prefix for this tenant's orchestration resources."

  validation {
    condition     = can(regex("^[a-z][a-z0-9-]{2,28}$", var.name_prefix))
    error_message = "name_prefix must be 3-29 lowercase letters, digits, or hyphens."
  }
}

variable "resource_config_file" {
  type        = string
  description = "Absolute path to the reviewed resources.yaml packaged in the Lambda image."
}

variable "lambda_image_uri" {
  type        = string
  description = "ECR image URI pinned by sha256 digest; image must contain the reviewed resource file."

  validation {
    condition     = can(regex("@sha256:[0-9a-f]{64}$", var.lambda_image_uri))
    error_message = "lambda_image_uri must be pinned by a sha256 digest."
  }
}

variable "control_database_secret_arn" {
  type        = string
  description = "Existing Secrets Manager secret containing a JSON dsn key."
}

variable "target_database_secret_arn" {
  type        = string
  default     = null
  description = "Existing JSON dsn secret for TimescaleDB metadata inventory publication, when that target is enabled."
}

variable "target_database_secret_kms_key_arn" {
  type        = string
  default     = null
  description = "Customer managed KMS key for the target database secret, when applicable."
}

variable "secret_kms_key_arn" {
  type        = string
  default     = null
  description = "Customer managed KMS key for the existing secret, if applicable."
}

variable "s3_kms_key_arns" {
  type        = list(string)
  default     = []
  description = "Customer managed KMS keys for pinned config and inventory objects, if applicable."
}

variable "config_object_arns" {
  type        = list(string)
  description = "Approved versioned configuration object ARN patterns for this deployment."

  validation {
    condition     = length(var.config_object_arns) > 0
    error_message = "At least one approved configuration object ARN is required."
  }
}

variable "inventory_object_arns" {
  type        = list(string)
  description = "Approved versioned inventory object ARN patterns for this deployment."

  validation {
    condition     = length(var.inventory_object_arns) > 0
    error_message = "At least one approved inventory object ARN is required."
  }
}

variable "metadata_evidence_object_arns" {
  type        = list(string)
  description = "Approved raw and certified metadata evidence object ARN patterns."

  validation {
    condition     = length(var.metadata_evidence_object_arns) > 0
    error_message = "At least one metadata evidence object ARN is required."
  }
}

variable "private_subnet_ids" {
  type        = list(string)
  description = "Existing private subnet IDs with database and AWS service connectivity."

  validation {
    condition     = length(var.private_subnet_ids) >= 2
    error_message = "At least two private subnets are required."
  }
}

variable "lambda_security_group_ids" {
  type        = list(string)
  description = "Existing security groups permitting database and required AWS service access."

  validation {
    condition     = length(var.lambda_security_group_ids) > 0
    error_message = "At least one Lambda security group is required."
  }
}

variable "planner_timeout_seconds" {
  type        = number
  default     = 180
  description = "Bounded pilot planner timeout; measure before increasing scope."

  validation {
    condition     = var.planner_timeout_seconds >= 1 && var.planner_timeout_seconds <= 900
    error_message = "Lambda timeout must be between 1 and 900 seconds."
  }
}

variable "planner_memory_mb" {
  type        = number
  default     = 1024
  description = "Pilot planner memory, subject to inventory size measurements."

  validation {
    condition     = var.planner_memory_mb >= 128 && var.planner_memory_mb <= 10240
    error_message = "Planner memory must be between 128 and 10240 MB."
  }
}

variable "planning_allowance_seconds" {
  type        = number
  default     = 600
  description = "Additional workflow time for planning and status retries."

  validation {
    condition     = var.planning_allowance_seconds >= 1 && var.planning_allowance_seconds <= 3600
    error_message = "Planning allowance must be between 1 and 3600 seconds."
  }
}

variable "planner_reserved_concurrency" {
  type        = number
  default     = 20
  description = "Upper bound on simultaneously running planners for this deployment."

  validation {
    condition     = var.planner_reserved_concurrency >= 1
    error_message = "Planner reserved concurrency must be positive."
  }
}

variable "status_timeout_seconds" {
  type        = number
  default     = 30
  description = "Timeout for one indexed run-status query."

  validation {
    condition     = var.status_timeout_seconds >= 1 && var.status_timeout_seconds <= 900
    error_message = "Lambda timeout must be between 1 and 900 seconds."
  }
}

variable "status_memory_mb" {
  type        = number
  default     = 256
  description = "Memory for the read-only status function."

  validation {
    condition     = var.status_memory_mb >= 128 && var.status_memory_mb <= 10240
    error_message = "Status memory must be between 128 and 10240 MB."
  }
}

variable "status_reserved_concurrency" {
  type        = number
  default     = 40
  description = "Upper bound on concurrent status database queries."

  validation {
    condition     = var.status_reserved_concurrency >= 1
    error_message = "Status reserved concurrency must be positive."
  }
}

variable "inventory_timeout_seconds" {
  type        = number
  default     = 900
  description = "Pilot metadata inventory publication timeout; measure before increasing project scope."

  validation {
    condition     = var.inventory_timeout_seconds >= 1 && var.inventory_timeout_seconds <= 900
    error_message = "Inventory Lambda timeout must be between 1 and 900 seconds."
  }
}

variable "inventory_memory_mb" {
  type        = number
  default     = 2048
  description = "Pilot metadata publisher memory; size from measured project inventory."

  validation {
    condition     = var.inventory_memory_mb >= 128 && var.inventory_memory_mb <= 10240
    error_message = "Inventory Lambda memory must be between 128 and 10240 MB."
  }
}

variable "inventory_reserved_concurrency" {
  type        = number
  default     = 2
  description = "Upper bound on concurrent metadata publisher invocations."

  validation {
    condition     = var.inventory_reserved_concurrency >= 1
    error_message = "Inventory reserved concurrency must be positive."
  }
}

variable "log_retention_days" {
  type        = number
  default     = 30
  description = "Retention for Lambda and workflow logs."

  validation {
    condition     = contains([1, 3, 5, 7, 14, 30, 60, 90, 120, 150, 180, 365, 400, 545, 731, 1096, 1827, 2192, 2557, 2922, 3288, 3653], var.log_retention_days)
    error_message = "log_retention_days must be a supported CloudWatch Logs retention period."
  }
}

variable "tags" {
  type        = map(string)
  default     = {}
  description = "Organization tags to apply to created resources."
}

variable "alarm_sns_topic_arns" {
  type        = list(string)
  default     = []
  description = "Existing SNS topics for workflow, Lambda, and Scheduler delivery alarms."
}
