mock_provider "aws" {}

variables {
  name_prefix          = "pilot-skyspark"
  resource_config_file = "../../local/resources.yaml"
  bucket_names = {
    raw        = "skyspark-raw-local"
    certified  = "skyspark-certified-local"
    quarantine = "skyspark-quarantine-local"
  }
}

run "reviewed_buckets_and_repositories" {
  command = plan

  assert {
    condition     = length(aws_s3_bucket.data) == 3 && length(aws_ecr_repository.image) == 3
    error_message = "Foundation must plan three data buckets and three image repositories."
  }

  assert {
    condition = (
      aws_s3_bucket_versioning.data["certified"].versioning_configuration[0].status == "Enabled" &&
      aws_s3_bucket_public_access_block.data["raw"].block_public_policy &&
      aws_ecr_repository.image["worker"].image_tag_mutability == "IMMUTABLE"
    )
    error_message = "Versioning, public-access block, and image immutability are required."
  }

  assert {
    condition     = length(aws_vpc_endpoint.interface) == 0 && length(aws_vpc_endpoint.s3) == 0
    error_message = "Existing VPC connectivity must not be changed unless explicitly enabled."
  }
}

run "approved_private_endpoints" {
  command = plan
  variables {
    create_private_endpoints    = true
    vpc_id                      = "vpc-11111111"
    private_subnet_ids          = ["subnet-11111111", "subnet-22222222"]
    endpoint_security_group_ids = ["sg-11111111"]
    private_route_table_ids     = ["rtb-11111111", "rtb-22222222"]
  }

  assert {
    condition     = length(aws_vpc_endpoint.interface) == 6 && length(aws_vpc_endpoint.s3) == 1
    error_message = "Private mode needs six interface endpoints and an S3 gateway endpoint."
  }
}
