output "bucket_arns" {
  value = { for purpose, bucket in aws_s3_bucket.data : purpose => bucket.arn }
}

output "bucket_names" {
  value = { for purpose, bucket in aws_s3_bucket.data : purpose => bucket.id }
}

output "object_kms_key_arn" {
  value = aws_kms_key.objects.arn
}

output "ecr_repository_urls" {
  value = { for role, repo in aws_ecr_repository.image : role => repo.repository_url }
}

output "interface_endpoint_ids" {
  value = { for service, endpoint in aws_vpc_endpoint.interface : service => endpoint.id }
}

output "s3_endpoint_id" {
  value = try(aws_vpc_endpoint.s3[0].id, null)
}
