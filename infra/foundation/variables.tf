variable "name_prefix" {
  type        = string
  description = "Unique lowercase deployment prefix."

  validation {
    condition     = can(regex("^[a-z][a-z0-9-]{2,28}$", var.name_prefix))
    error_message = "name_prefix must be 3-29 lowercase letters, digits, or hyphens."
  }
}

variable "resource_config_file" {
  type        = string
  description = "Reviewed AWS resources.yaml packaged unchanged into worker, control, and Lambda images."
}

variable "bucket_names" {
  type = object({
    raw        = string
    certified  = string
    quarantine = string
  })
  description = "Globally unique, reviewed S3 names; must exactly match the resources.yaml bucket list."
}

variable "create_private_endpoints" {
  type        = bool
  default     = false
  description = "Create AWS service endpoints in an existing approved VPC when NAT/equivalent access is unavailable."
}

variable "vpc_id" {
  type        = string
  default     = null
  description = "Existing VPC ID used only when creating private endpoints."
}

variable "private_subnet_ids" {
  type        = list(string)
  default     = []
  description = "Existing private subnets used for interface endpoints."
}

variable "endpoint_security_group_ids" {
  type        = list(string)
  default     = []
  description = "Existing security groups allowing HTTPS from approved worker and Lambda subnets."
}

variable "private_route_table_ids" {
  type        = list(string)
  default     = []
  description = "Existing private route tables for the S3 gateway endpoint."
}

variable "tags" {
  type        = map(string)
  default     = {}
  description = "Organization tags."
}
