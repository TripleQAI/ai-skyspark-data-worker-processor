data "aws_partition" "current" {}
data "aws_region" "current" {}
data "aws_caller_identity" "current" {}

locals {
  resources   = yamldecode(file(var.resource_config_file))
  buckets     = { for purpose, name in var.bucket_names : purpose => name }
  ecr_roles   = toset(["worker", "control", "lambda"])
  common_tags = merge(var.tags, { Application = "skyspark-ingestion" })
  endpoint_services = var.create_private_endpoints ? toset([
    "ecr.api", "ecr.dkr", "logs", "secretsmanager", "sqs", "events"
  ]) : toset([])
}

resource "aws_kms_key" "objects" {
  description             = "${var.name_prefix} raw, certified, and quarantine S3 objects"
  enable_key_rotation     = true
  deletion_window_in_days = 30
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "AccountRootDelegatesToIAM"
      Effect    = "Allow"
      Principal = { AWS = "arn:${data.aws_partition.current.partition}:iam::${data.aws_caller_identity.current.account_id}:root" }
      Action    = "kms:*"
      Resource  = "*"
    }]
  })
  tags = local.common_tags

  lifecycle {
    prevent_destroy = true
  }
}

resource "aws_kms_alias" "objects" {
  name          = "alias/${var.name_prefix}-objects"
  target_key_id = aws_kms_key.objects.key_id
}

resource "aws_s3_bucket" "data" {
  for_each = local.buckets
  bucket   = each.value
  tags     = merge(local.common_tags, { Purpose = each.key })

  lifecycle {
    prevent_destroy = true
    precondition {
      condition = (
        length(toset(values(local.buckets))) == 3 &&
        toset(values(local.buckets)) == toset(local.resources.buckets) &&
        local.buckets.raw == local.resources.storage.raw_bucket &&
        local.buckets.certified == local.resources.storage.certified_bucket
      )
      error_message = "Foundation bucket names must exactly match the reviewed resources.yaml."
    }
    precondition {
      condition = !var.create_private_endpoints || (
        var.vpc_id != null && length(var.private_subnet_ids) >= 2 &&
        length(var.endpoint_security_group_ids) > 0 &&
        length(var.private_route_table_ids) > 0
      )
      error_message = "Private endpoints need an existing VPC, two subnets, endpoint security groups, and S3 route tables."
    }
  }
}

resource "aws_s3_bucket_public_access_block" "data" {
  for_each                = local.buckets
  bucket                  = aws_s3_bucket.data[each.key].id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_ownership_controls" "data" {
  for_each = local.buckets
  bucket   = aws_s3_bucket.data[each.key].id
  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}

resource "aws_s3_bucket_versioning" "data" {
  for_each = local.buckets
  bucket   = aws_s3_bucket.data[each.key].id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "data" {
  for_each = local.buckets
  bucket   = aws_s3_bucket.data[each.key].id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = "aws:kms"
      kms_master_key_id = aws_kms_key.objects.arn
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_policy" "deny_http" {
  for_each = local.buckets
  bucket   = aws_s3_bucket.data[each.key].id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "DenyInsecureTransport"
      Effect    = "Deny"
      Principal = "*"
      Action    = "s3:*"
      Resource = [
        aws_s3_bucket.data[each.key].arn,
        "${aws_s3_bucket.data[each.key].arn}/*"
      ]
      Condition = { Bool = { "aws:SecureTransport" = "false" } }
    }]
  })
  depends_on = [aws_s3_bucket_public_access_block.data]
}

resource "aws_ecr_repository" "image" {
  for_each             = local.ecr_roles
  name                 = "${var.name_prefix}/${each.key}"
  image_tag_mutability = "IMMUTABLE"
  force_delete         = false
  image_scanning_configuration {
    scan_on_push = true
  }
  encryption_configuration {
    encryption_type = "AES256"
  }
  tags = merge(local.common_tags, { Purpose = each.key })

  lifecycle {
    prevent_destroy = true
  }
}

resource "aws_vpc_endpoint" "interface" {
  for_each            = local.endpoint_services
  vpc_id              = var.vpc_id
  service_name        = "com.amazonaws.${data.aws_region.current.region}.${each.key}"
  vpc_endpoint_type   = "Interface"
  subnet_ids          = var.private_subnet_ids
  security_group_ids  = var.endpoint_security_group_ids
  private_dns_enabled = true
  tags                = merge(local.common_tags, { Service = each.key })
}

resource "aws_vpc_endpoint" "s3" {
  count             = var.create_private_endpoints ? 1 : 0
  vpc_id            = var.vpc_id
  service_name      = "com.amazonaws.${data.aws_region.current.region}.s3"
  vpc_endpoint_type = "Gateway"
  route_table_ids   = var.private_route_table_ids
  tags              = merge(local.common_tags, { Service = "s3" })
}
