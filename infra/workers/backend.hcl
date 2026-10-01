# Terraform state backend for infra/workers.
#
# The state bucket lives in the DEVELOPMENT account (001634074733), alongside
# the resources these roots create -- not in the shared tooling bucket.
#
# WHY NOT tripleq-tfstate-079247879111-us-east-1, the bucket every
# ZonicPlatformInfrastructure root uses. That bucket is in the tooling account,
# and reaching it from development needs three coordinated changes: a new
# prefix in bootstrap/iam.tf, a cross-account bucket policy (it currently has
# only DenyInsecureTransport), and attaching ZonicTerraformStateAccess-* to the
# development SSO permission set -- those policies are presently attached to
# nothing. Without all three, init fails with 403 Forbidden on HeadObject.
#
# Same-account state avoids that entirely and keeps this pipeline's blast
# radius inside one account. Moving to the shared bucket later, once those
# grants exist, is a terraform init -migrate-state with this file updated.
#
# Versioned deliberately: prevent_destroy guards the S3 buckets and the KMS
# key, so a lost state file leaves resources that cannot be recreated under the
# same names and cannot be destroyed by Terraform either.
#
# Init with the argument QUOTED -- PowerShell splits a native command's
# arguments on "=", which is why the unquoted form reports
# "Too many command line arguments":
#
#     terraform init "-backend-config=backend.hcl"
#
# Bucket, key and region are identifiers, not secrets. Encryption is SSE-S3
# (the bucket default); no kms_key_id here, unlike the tooling bucket, because
# this bucket uses no customer-managed key.

bucket       = "skyspark-tfstate-001634074733-us-east-1"
key          = "skyspark-ingestion/workers/terraform.tfstate"
region       = "us-east-1"
encrypt      = true
use_lockfile = true
