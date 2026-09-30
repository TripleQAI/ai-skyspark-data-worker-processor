# AWS data and image foundation

This Terraform root plans three private, versioned S3 buckets from the **reviewed AWS** `resources.yaml`: raw, certified, and quarantine. The names supplied through `bucket_names` must exactly match that file. The buckets block public access, deny plain HTTP, use a rotating customer-managed KMS key, and have no automatic expiration. Three immutable, scan-on-push ECR repositories hold worker, control, and Lambda images. Build and push reviewed images separately, then pass their **digest-pinned** URIs to the workers and orchestration roots.

The root accepts an existing VPC. Set `create_private_endpoints=true` only after reviewing VPC ID, two private subnets, endpoint security groups, and private route tables. It then adds ECR API/Docker, CloudWatch Logs, Secrets Manager, SQS, and EventBridge interface endpoints plus an S3 gateway endpoint. An approved route or proxy to the selected SkySpark endpoint and the databases is still required. Existing NAT or organization endpoints may be used instead; this root does not create a VPC, database, or internet egress.

## Local review

From this directory, use Terraform 1.13.5 or a compatible version:

```text
terraform init -backend=false
terraform fmt -check
terraform validate
terraform test
```

The mocked `terraform test` plans create **no AWS resources or credentials**. They check the three buckets, three image repositories, versioning/public-access settings, the default of no new VPC endpoints, and the seven-endpoint private option. Use a reviewed S3 remote backend, actual AWS resource file, globally unique bucket names, and an inspected plan before any AWS apply. `prevent_destroy` protects the KMS key, buckets, and ECR repositories; no retention lifecycle is enabled until measured policy values are approved.

Pass `object_kms_key_arn` to `s3_kms_key_arns` in the workers and orchestration roots. Scope their object ARNs to explicit tenant/project prefixes. The KMS key policy delegates use to account IAM; the task and Lambda roles still need their scoped KMS permissions, and any organization key/bucket policies must agree. The foundation root does not create database secrets or rotate them.
