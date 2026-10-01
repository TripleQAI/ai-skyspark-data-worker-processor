# SkySpark ingestion worker processor — AWS deployment

**Status:** Deployed and ingesting in the development account, 2026-09-30/10-01.
**Source repository:** `D:\Zonic\ai-skyspark-data-worker-processor` (TripleQAI/ai-skyspark-data-worker-processor)
**Account:** development `001634074733`, `us-east-1`
**Scope:** The first AWS deployment of this pipeline. It records what was configured, why each
choice was made, and the three failures that shaped it. It is not a design document — the
repository's own `docs/aws-production-architecture.md` describes the intended design; this
describes what actually runs and the deviations forced by this account's real constraints.

This stack is **independent of EKS and Jenkins**. It is ECS Fargate + Lambda + Step Functions,
applied with Terraform from a workstation. It shares only the VPC, the NAT gateway and the
PostgreSQL instance with the existing platform.

---

## 1. Architecture as deployed

```
┌─ AWS development 001634074733 · us-east-1 ───────────────────────────┐
│                                                                      │
│  EventBridge Scheduler ──► Step Functions ──► Lambda planner ────┐   │
│  skyspark-dev-schedules    skyspark-dev-        (VPC, 2 subnets) │   │
│  (no schedules created)    ingestion   ▲                         │   │
│                               │        └── Lambda status ◄───────┼── │
│                               └─────────── Lambda inventory      │   │
│                                                                  ▼   │
│  ┌─ VPC vpc-01738eeb78ae67b39 (10.20.0.0/16) ───────────────────────┐│
│  │  private 1a subnet-0864b2e585f156e9a                             ││
│  │  private 1b subnet-06608ba8721b4cc00    sg-05a4928dccf405988     ││
│  │                                                                  ││
│  │  ECS skyspark-dev-workers (Fargate)                              ││
│  │    dispatcher ×4 ──► SQS history_live / rules_nightly /          ││
│  │                          metadata_sweep / backfill  (+4 DLQ)     ││
│  │                              │                                   ││
│  │                              ▼                                   ││
│  │    worker ×4 (1 per queue, max 4/2/2/1) ──┬──► S3 raw            ││
│  │       job_slots 4, fenced leases          └──► S3 certified      ││
│  │    publisher ×2 ──► EventBridge skyspark-certified-dev           ││
│  │    recovery ×1 ──► requeues stale leases                         ││
│  └───────────────────┬──────────────────────────┬──────────────────┘│
│                      │ NAT nat-118e8b83d8e7f8f19│                    │
│  S3 raw / certified / quarantine  (KMS 9bcfe39b)│                    │
│  ECR worker:v2 control:v1 lambda:v1             │                    │
└─────────────────────┬───────────────────────────┼────────────────────┘
                      ▼                           ▼
        SkySpark insiot.insiteintelligence.com    Tiger Cloud tsdb:32064
        /api/launchPad_ca141bd38c8fe5081          schema ingestion
```

**Shared with the existing platform:** the VPC, the NAT gateway, and the PostgreSQL instance.
Everything else is dedicated. The Terraform creates **no network resources at all** — it reads
the VPC and subnets as inputs — so EKS, the ALBs, `zonic-bff`, `zonic-authz` and pgfoundation
are untouched by any apply here, and a `terraform destroy` removes only what it created.

---

## 2. Step 1 — Control store (Tiger Cloud)

```
Tiger Cloud tsdb  ──┬── user_schema      (Authorization Server, pre-existing)
                    ├── gold, agent_builder, data_ingestion_workflow_config
                    └── ingestion        ← created here: 22 tables
                         runs, jobs, job_children, attempts,
                         dispatch_outbox, publication_outbox,
                         raw_artifacts, sink_receipts, certifications,
                         source_budgets, source_permits, checkpoints,
                         inventory_versions, inventory_entities, ...

       role: skyspark_control_app  (owns the schema, CONNECTION LIMIT 60)
```

### Why a schema and not a database

The plan was a separate `skyspark_control` database. Tiger Cloud refused it:

```
tsdb_admin: database skyspark_control is not an allowed database name
HINT: Contact your administrator to configure the "tsdb_admin.allowed_databases"
```

`SHOW tsdb_admin.allowed_databases` returns exactly `tsdb`. This service tier permits one
database, so isolation is by **schema plus a scoped role** instead of by database boundary.

This was checked before accepting it, not assumed safe:

| Measure | Value |
| --- | ---: |
| `max_connections` | 200 |
| In use at the time | 23 |
| `superuser_reserved_connections` | 12 |
| Headroom | 165 |
| Cap on `skyspark_control_app` | 60 |

165 free against a 60 cap means ingestion cannot starve the Authorization Server. Had the
ceiling been one of Tiger's smaller tiers (25–100), a 60-connection ingestion role would have
been actively dangerous and a second service would have been the correct purchase.

### Why only the control migrations

`migrations/control/` has ten files; `migrations/target/` has four. Only the control set was
applied. The target schema (`ingestion_target`) holds history observations, rule detections and
metadata entities — and all three feeds in `profile.yaml` write `target: s3`, so nothing would
ever write there. Running those migrations would have created four tables that stay empty
forever.

Ordering note: there are **two** `0007_` files. The runner sorts lexicographically, so
`0007_history_splits.sql` precedes `0007_metadata_inventory.sql`. Applying by hand in a console
must preserve that.

The runner (`skyspark-control migrate`) takes an advisory lock, records a SHA-256 per file in
`public.control_schema_migrations`, and refuses a file whose content changed after application.
Re-running applied 0 — idempotent as designed.

### Console access needed an explicit grant

`skyspark_control_app` created and owns schema `ingestion`. A new schema grants nothing to
anyone else, and `tsdbadmin` is **not** a superuser on Tiger Cloud (`rolsuper = False`), so the
web console could see the schema listed but got `permission denied for schema ingestion`.

Fixed by granting, **as the owner**, read-only access:

```sql
GRANT USAGE ON SCHEMA ingestion TO tsdbadmin;
GRANT SELECT ON ALL TABLES IN SCHEMA ingestion TO tsdbadmin;
ALTER DEFAULT PRIVILEGES IN SCHEMA ingestion GRANT SELECT ON TABLES TO tsdbadmin;
```

Read-only deliberately: the console is for inspection, not for mutating run state behind the
workers' backs.

---

## 3. Step 2 — Secrets

```
tripleq/development/skyspark/control-db     {"dsn": "postgresql://skyspark_control_app@..."}
tripleq/development/skyspark/credentials    {"username": "yzhao", "password": "..."}
```

JSON with a `dsn` key is the shape ECS and Lambda secret injection expect — the task definition
names the JSON field, and the container receives only that value as an environment variable.

Both were written through **BOM-less temporary files**, not inline `--secret-string`. Windows
PowerShell 5.1's `-Encoding utf8` emits a UTF-8 byte-order mark, and AWS's JSON parser rejects
those three leading bytes with a message naming the parameter rather than the cause. The same
trap is already documented in `scripts/rotate-pgfoundation-caller-key.ps1`; it recurred here,
and again later against Docker's config parser.

---

## 4. Step 3 — Terraform state

```
s3://skyspark-tfstate-001634074733-us-east-1   (versioned, development account)
   skyspark-ingestion/foundation/terraform.tfstate
   skyspark-ingestion/orchestration/terraform.tfstate
   skyspark-ingestion/workers/terraform.tfstate
```

### Why not the shared tooling bucket

The obvious choice was `tripleq-tfstate-079247879111-us-east-1`, which every
ZonicPlatformInfrastructure root uses. The first `terraform init` against it failed:

```
Error refreshing state: Unable to access object "skyspark-ingestion/foundation/terraform.tfstate"
in S3 bucket "tripleq-tfstate-079247879111-us-east-1": StatusCode: 403 ... Forbidden
```

Investigation found this needs **three** coordinated changes, not one:

1. A new prefix in `bootstrap/iam.tf` — `state_access_policies` scopes each principal to named
   key prefixes, and `skyspark-ingestion` is not among them.
2. A cross-account bucket policy. The bucket's resource policy currently contains **only**
   `DenyInsecureTransport` — there is no cross-account grant at all.
3. Attaching `ZonicTerraformStateAccess-Development` to the development SSO permission set.
   `aws iam list-entities-for-policy` shows that policy attached to **nothing**; the five
   `ZonicTerraformStateAccess-*` policies exist but are presently decorative.

That is surgery on shared infrastructure, requiring its own review and a tooling-account apply.
A same-account state bucket costs about $0.02/month, needs no cross-account IAM, and migrates
to the shared bucket later with `terraform init -migrate-state` when those grants exist.

Versioning is enabled deliberately: `prevent_destroy` guards the S3 buckets and the KMS key, so
a lost state file leaves resources that can neither be recreated under the same names nor
destroyed by Terraform.

---

## 5. Step 4 — Foundation root (23 resources)

```
S3  skyspark-dev-raw-001634074733         versioned, SSE-KMS, deny-HTTP,
    skyspark-dev-certified-001634074733   public access blocked, ownership enforced
    skyspark-dev-quarantine-001634074733
KMS alias/skyspark-dev-objects            customer-managed, rotating
ECR skyspark-dev/{worker,control,lambda}  IMMUTABLE tags, scan-on-push
```

### Why `create_private_endpoints = false`

The workers must reach `insiot.insiteintelligence.com` over HTTPS and Tiger Cloud on port
32064. Both are **public hosts**, which VPC interface endpoints cannot serve. The development
VPC's existing NAT gateway carries that traffic, so no endpoints were created and nothing was
added to the account's existing ~$140/month VPC endpoint bill.

### Bucket names appear in two places

`skyspark-dev-raw` and friends are globally unique names and had to be suffixed with the
account ID. The names occur **twice** in `config/environments/aws-dev/resources.yaml`:

* the `buckets:` list, and
* `storage.raw_bucket` / `storage.certified_bucket`.

`infra/foundation/main.tf`'s precondition compares all three conditions:

```hcl
toset(values(local.buckets)) == toset(local.resources.buckets) &&
local.buckets.raw == local.resources.storage.raw_bucket &&
local.buckets.certified == local.resources.storage.certified_bucket
```

Updating only the list produces a bare `Resource precondition failed` that does not say which
of the three checks broke. Both places must change together.

`storage` has no `quarantine_bucket` key, and no code path writes to that bucket — the
foundation creates it ahead of a future need.

### PowerShell argument splitting

Every Terraform invocation needs its arguments **quoted**:

```powershell
terraform init "-backend-config=backend.hcl"
terraform plan "-out=foundation.tfplan"
```

PowerShell splits a native command's arguments on `=`, so the unquoted form sends `-out` and
`foundation.tfplan` separately and Terraform reports *"Too many command line arguments. Did you
mean to use -chdir?"* — a message that points at the wrong thing entirely. This is the same
rule already recorded for `-target=aws_kms_key...`.

Variables were moved into `terraform.tfvars` files rather than `-var` flags for the same
reason: the nested `bucket_names` object would otherwise need backtick-escaped inner quotes.
`*.tfvars` is gitignored in this repository, so those files stay local.

---

## 6. Step 5 — Container images

```
Dockerfile.worker   PACKAGE_EXTRAS="[source]"  SCRIPT_ROOT_DIR=readers/skyspark  → worker:v2
Dockerfile.control                                                               → control:v1
Dockerfile.lambda   --platform linux/amd64 --provenance=false                    → lambda:v1
```

### Why images at all, and why built locally

ECS Fargate runs only container images; the Lambdas could have been zip-packaged but are
images so that all three deployment targets carry **the same reviewed `resources.yaml`** at a
known path (`/app/resources.yaml`, `/var/task/resources.yaml`). The orchestration README is
explicit that the Terraform `resource_config_file` must be the same file packaged into the
image. That is the integrity property: the YAML Terraform validates is byte-identical to the
one the container runs.

Building locally was expedient, not architecturally right. This platform already has a
`service-build-pipeline` module and eight CodeBuild projects that build images in AWS for the
other services. **Any permanent deployment of this pipeline should build through CodeBuild**,
which needs a `buildspec.yml` in the project root and a `local.services` entry in
`environments/tooling/network/services.tf`. Local Docker was accepted here only to avoid an
hour of CI plumbing during a first sandbox test.

### Digest pinning

Terraform receives `...@sha256:...`, never `:v1`. A tag can move; a digest cannot, so the
running container is provably the reviewed one. ECR repositories are also `IMMUTABLE`, which
blocks tag reuse — hence the rebuild below is `v2`, not a re-push of `v1`.

`--platform linux/amd64 --provenance=false` on the Lambda image because Lambda rejects
multi-architecture manifests.

### Docker Desktop on Windows: three dead ends

`docker login` against ECR failed with a bare `400 Bad Request`. Three hypotheses were tried
and two were wrong:

| Hypothesis | Verdict |
| --- | --- |
| `credsStore: desktop` mishandles the ECR token | wrong — removing it changed nothing |
| A UTF-8 BOM in `config.json` | real, but a separate bug; fixing it left the 400 |
| Docker Desktop's internal proxy (`http.docker.internal:3128`) | the actual cause |

`curl` with the same token against the same `/v2/` endpoint returned **200**, proving AWS, IAM
and the token were all fine and the fault was client-side. Adding `*.amazonaws.com` to the
proxy bypass saved to `settings-store.json` but the daemon never honoured it, even after a full
restart.

**What worked:** `--password` instead of `--password-stdin`.

```powershell
$pw = aws ecr get-login-password --region us-east-1
docker login --username AWS --password $pw $REG
```

---

## 7. Step 6 — Orchestration root (14 resources)

```
EventBridge Scheduler group  skyspark-dev-schedules   (no schedules created)
Step Functions (Standard)    skyspark-dev-ingestion
Lambda  skyspark-dev-planner / -status / -inventory   (three separate IAM roles)
SQS     skyspark-dev-scheduler-dlq
CloudWatch log groups, dashboard, 7 alarms
```

### The Lambda concurrency override

The first apply created all three functions and then failed setting concurrency:

```
InvalidParameterValueException: Specified ReservedConcurrentExecutions for function
decreases account's UnreservedConcurrentExecution below its minimum value of [10].
```

`aws lambda get-account-settings` shows this account's total concurrency is **10**, the
unraised new-account default, not the usual 1000. AWS requires 10 to remain unreserved, so the
account has **zero** reservable concurrency and any positive value fails. The module's
variables validate `>= 1`, so the value cannot be lowered to zero through tfvars.

`infra/orchestration/concurrency_override.tf` sets `reserved_concurrent_executions = -1`
(Terraform's "unset") on all three functions. Terraform merges `*_override.tf` over the base
configuration, so this avoided editing reviewed module code.

**What is lost:** reserved concurrency is a per-function ceiling protecting the control store
from a planner storm opening more connections than PostgreSQL allows. Without it the three
functions share the account's pool of 10 — which is *tighter* than the 20/40/2 the module
wanted, so the protection still exists, just account-wide.

**Delete `concurrency_override.tf` and re-apply** once a quota increase lands:

```powershell
aws service-quotas request-service-quota-increase `
  --service-code lambda --quota-code L-B99A9384 --desired-value 1000 `
  --profile tripleq-development --region us-east-1
```

Until then the pipeline is bounded by 10 concurrent Lambda executions, not by the worker fleet.

---

## 8. Step 7 — Workers root (~50 resources)

```
ECS cluster  skyspark-dev-workers           (Container Insights enabled)
  worker services   history_live, rules_nightly, metadata_sweep, backfill
                    min 1 task each, max 4 / 2 / 2 / 1
  control services  dispatcher ×4, publisher ×2, recovery ×1
SQS          4 work queues + 4 DLQs, max_receive_count 4
EventBridge  skyspark-certified-dev
IAM          one task role per service, scoped to its own queue
```

### S3 object ARNs must be exact prefixes

```hcl
raw_object_arns       = ["arn:aws:s3:::skyspark-dev-raw-001634074733/raw/dev-tenant/dev-scale/*"]
certified_object_arns = ["arn:aws:s3:::skyspark-dev-certified-001634074733/certified/dev-tenant/dev-scale/*"]
approved_scopes       = ["dev-tenant/dev-scale"]
```

The module's validation **rejects a bucket-wide `*`**, and its mocked tests assert that
rejection. This matters because a worker service consumes jobs across projects: the IAM
boundary is what stops a shared worker reading another tenant's objects, independently of the
job's own pinned tenant/project checks.

A quarantine ARN cannot be added to `certified_object_arns` — the regex requires a literal
`certified/` path segment. There is no `quarantine_object_arns` variable, and nothing writes
there.

### `target_database_secret_arn` points at the control secret

This variable is **required** in the workers root (unlike orchestration, where it defaults to
`null`), but all three feeds write `target: s3`, so no sink ever resolves to TimescaleDB and
the DSN is never opened. Pointing it at the control secret satisfies the type without inventing
a second database or editing reviewed code.

Note the task definition *does* inject `TARGET_DATABASE_URL`, and `control_cli.py` constructs a
Timescale verifier when that variable is set. It is harmless — no feed targets Timescale — but
an earlier claim in this deployment that the variable "is never set in AWS" was wrong.

### Why everything lands in S3, verified four ways

| Check | Result |
| --- | --- |
| Feed targets in the uploaded config bundle | all three `target: 's3'` |
| Reader manifests | each declares `targets=['s3']` — Timescale not permitted |
| Worker verifier map | `{TargetKind.S3: ...}` plus Timescale only if `TARGET_DATABASE_URL` set |
| Completion check | `"completion target differs from pinned profile"` |

The strongest guard is the manifest: the readers do not list `timescale` as a permitted target,
so config resolution fails before a run starts. `EvidenceVerifier` additionally re-reads and
re-checksums each S3 object before `certify_job` marks anything certified, so a job cannot
certify against evidence that did not land.

---

## 9. Step 8 — Triggering a run

The state machine input is a `ScheduledTrigger` — eight strictly validated fields:

```json
{
  "schema_version": 1,
  "tenant_id": "dev-tenant",
  "project_id": "dev-scale",
  "feed": "metadata",
  "profile_id": "aws-dev",
  "config_hash": "1a567aff21c03879ddca68ea9e186acfbb4b554600c7c2113bba92c4ee7ab0c8",
  "config_ref": "s3://skyspark-dev-raw-001634074733/config/dev-tenant/dev-scale/aws-dev.json?versionId=...",
  "scheduled_at": "2026-09-27T03:00:00Z"
}
```

It carries **no site list, endpoint or credentials** — everything else comes from the pinned
config bundle, a single JSON object with exactly the keys `schema_version`, `profile`,
`binding`, `manifest`. The planner downloads that exact object version, recomputes the hash and
refuses to plan if it differs from `config_hash`.

### `scheduled_at` must be a time the feed would really have fired

| Feed | Schedule | Valid example |
| --- | --- | --- |
| metadata | weekly, SUN 03:00 UTC | `2026-09-27T03:00:00Z` |
| rules | daily, 02:00 UTC | `2026-09-30T02:00:00Z` |
| history | every 5 minutes | last boundary minus the 5-minute source lag |

The planner derives window boundaries from this value, so an arbitrary minute is rejected with
`scheduled time does not match configured UTC time`. History additionally validates
`due.minute % 5 == 0` and respects `history_source_lag_minutes: 5`.

---

## 10. How 1,000 sites distribute across the pipeline

Two feeds split the same 1,000 sites very differently, and the difference is entirely a
`partition.max_ids` value in the config bundle — not an AWS setting.

### Metadata: one job per site

```
Step Functions ──► Lambda planner
     partition: by siteRef, max_ids 1   ──►  ONE JOB PER SITE
     1000 jobs + 1000 dispatch_outbox rows, ONE transaction
                       │
ECS dispatcher ×4      ▼   claim_dispatch: SELECT ... FOR UPDATE SKIP LOCKED
                           SendMessageBatch, <=10 refs per call  ──► 100 API calls
                       │
SQS metadata_sweep     ▼   1000 messages, long-poll, visibility 900s
                       │
ECS worker ×1-2        ▼   job_slots 4 each  =  4-8 concurrent jobs
                           fenced lease ──► SOURCE PERMIT ──► SkySpark
                       │
S3 raw + certified     ▼   EvidenceVerifier re-reads and re-checksums
                       │
certify_job            ▼   certification + site completion + publication_outbox
```

Dispatch is **shared, not sharded**: four dispatcher tasks compete over one outbox with
`FOR UPDATE SKIP LOCKED`. Workers self-balance through SQS with no assignment, which is why the
observed split across two worker tasks was 123 / 98 rather than exactly even.

### History: twenty jobs per site, every five minutes

```
EventBridge Scheduler (every 5 min) ──► Step Functions ──► Lambda planner
     pins the certified inventory version produced by the metadata run
     window = [T-10min, T-5min)          <- history_source_lag_minutes: 5
     for each of 1000 sites:
         ids    = site.historized_point_ids    <- FROM THE INVENTORY, not config
         groups = chunks(ids, max_ids=500)
                  10,000 points/site / 500 = 20 jobs per site
                       │
     1000 x 20 = 20,000 JOBS, one transaction
                       │
ECS dispatcher ×4      ▼   20,000 / 10 = 2,000 SendMessageBatch calls
SQS history_live       ▼   20,000 messages, visibility 900s
ECS worker ×1-4        ▼   4 tasks x 4 slots = up to 16 concurrent jobs
Source permit pool     ▼   max_concurrent_calls: 12          <- THE CEILING
SkySpark hisRead       ▼   <=500 point IDs per call, replicas deduped to real IDs
S3 + verify + certify  ▼   a site checkpoint advances only when ALL 20 partitions certify
```

### Which component owns which split

| Stage | Count | Responsible component |
| --- | ---: | --- |
| Sites | 1,000 | **Lambda planner** — reads `approved_sites` from the config bundle in S3 |
| Points per site | 10,000 | **Lambda inventory** — publishes the certified inventory the planner reads |
| Jobs per site | 20 | **Lambda planner** — `_chunks(ids, max_ids=500)`, `core/planner.py:104` |
| Jobs per 5-min cycle | 20,000 | **Lambda planner**, written to PostgreSQL `jobs` + `dispatch_outbox` |
| SQS batch calls | 2,000 | **ECS dispatcher** (4 tasks) — `SendMessageBatch`, <=10 per call |
| Concurrent source calls | 12 | **PostgreSQL** `ingestion.source_permits` — *not* an AWS service |

Two points worth stating plainly, because both are easy to assume wrong:

**The concurrency ceiling is a database table, not an AWS limit.** Workers acquire a fenced row
in `ingestion.source_permits` before calling SkySpark and renew it on a heartbeat; a dead task's
lease expires and returns the permit. ECS autoscaling therefore cannot raise throughput — the
ceiling lives in PostgreSQL.

**No AWS resource decides "20 jobs per site."** That falls out of `max_ids: 500` in the config
bundle divided into the point count the inventory reports. Changing the YAML changes the split;
no AWS resource is involved.

### Three nested ceilings

| Limit | Value | Effect |
| --- | ---: | --- |
| ECS tasks | 1–4 per queue | how many worker processes exist |
| `worker.job_slots` | 4 per task | 4–16 jobs in flight |
| **`source_policy.max_concurrent_calls`** | **12** | ceiling on simultaneous SkySpark calls |

All 1,000 replica sites map to **one** real site
(`p:launchPad_ca141bd38c8fe5081:r:2c2c8620-747a608c`). Without the permit pool, 16 concurrent
workers would hammer a single SkySpark server. Scaling ECS beyond three tasks achieves nothing
here.

### Why a 5-minute history run is expected to report `partial`

20,000 jobs per cycle against 12 concurrent source calls and a 900-second deadline. Even at one
second per call that is roughly 28 minutes of serialised source time for a window that must
finish in 15. The metadata run's observed rate — about 170 sites per 25 minutes — implies over
45 hours for 20,000 jobs, while the next cycle starts in five minutes.

That is the measurement, not a defect: 1,000 replica sites against one real SkySpark server is
deliberately a saturation test, and `partial` is the honest reported outcome.

**Prerequisite:** history cannot plan at all until a certified metadata run has published an
inventory. `inventory_entities` is empty until then, and the planner raises
`certified inventory is missing site`.

**Job splitting can inflate the count further.** If a `hisRead` response would exceed the safe
return size, `split_history_job` halves it into child jobs — bounded at depth 12, a 30-second
minimum window, and 8,192 descendants per job.

---

## 11. Three failures and what they actually were

| Failure | Cause | Fix |
| --- | --- | --- |
| Execution 1 `FAILED` instantly | `scheduled_at` taken from `Get-Date`; metadata runs weekly SUN 03:00 UTC | use a real due time |
| Execution 2 `BlockedCoverage`, 1000 attempts `retry`/`ValueError` in ~200 ms | secret key named `SKYSPARK_CREDENTIALS`; readers declare **`SKYSPARK_SOURCE_CREDENTIALS`** | renamed in tfvars |
| (same run) | worker image built without `PACKAGE_EXTRAS="[source]"`, so `phable` was absent | rebuilt as `worker:v2` |

All three live readers declare `env_names = ("RESOURCE_CONFIG_PATH",
"SKYSPARK_SOURCE_CREDENTIALS")`. The worker passes a script **only** the variables its reviewed
manifest names, so a differently spelled key is simply absent inside the subprocess: the script
raises `ValueError` before any network call and before writing a log line.

### Two diagnostic corrections worth recording

**Empty CloudWatch logs were never a symptom.** `src/ingestion/core/worker.py` contains no
logging at all — no `logging` import, no log calls. The worker log groups stay empty whether it
succeeds or fails. The real instrument is `ingestion.attempts`, specifically its `error_class`
column:

```sql
SELECT attempt_no, outcome, coalesce(error_class,'(none)') AS error, count(*)
FROM ingestion.attempts GROUP BY 1,2,3 ORDER BY 1;
```

**The `[source]` extra was not the cause** of the failures, though it is genuinely required —
`Dockerfile.worker`'s own comment says so, and the readers import
`ingestion.adapters.skyspark.live`, which needs `phable`. The v2 rebuild was necessary but did
not explain the `ValueError`.

### SQS visibility delays redelivery

After fixing the task definition, no new attempts appeared for roughly fifteen minutes. All
1,000 messages sat in `ApproximateNumberOfMessagesNotVisible` under the failed attempts'
900-second visibility timeout (`worker.visibility_seconds: 900`). Nothing was wrong; it was
waiting. Purging the queue and re-triggering is the faster path, and it also resets
`max_receive_count`, which the failed attempts had already consumed one of four.

---

## 12. Verified result

First metadata run, 1,000 replica sites:

```
jobs          certified 14, running 4, planned 982
attempts      attempt 1: retry/ValueError ×1000   (pre-fix)
              attempt 2: certified ×14, in flight ×4
raw_artifacts 14          certifications 14
S3 raw        30 objects, 227 MB      (~15 MB per site)
S3 certified  30 objects, 267 MB
```

Objects land under:

```
s3://skyspark-dev-raw-001634074733/raw/dev-tenant/dev-scale/metadata/<run>/<job>/<sha>.bin
                                                                             + .bin.manifest.json
s3://skyspark-dev-certified-001634074733/certified/dev-tenant/dev-scale/metadata/<run>/<job>/<sha>.jsonl
```

Only four jobs run concurrently. That is the per-project source permit pool working as
designed: all 1,000 replica sites dedupe to **one** real SkySpark site
(`p:launchPad_ca141bd38c8fe5081:r:2c2c8620-747a608c`, "One Hughes Landing"), so simultaneous
source calls are capped deliberately. At roughly 15 MB and a few seconds per site, a full
1,000-site metadata run is on the order of 1–2 hours and about 15 GB in S3.

---

## 13. Outstanding items

| Item | Action |
| --- | --- |
| **Seven Fargate tasks run continuously** | ≈ $60/month. Set `min_tasks: 0` or `terraform destroy` the workers root when testing stops. |
| **Lambda concurrency capped at 10** | Request quota `L-B99A9384`; then delete `infra/orchestration/concurrency_override.tf` and re-apply. |
| **Images built on a workstation** | Move to CodeBuild via the existing `service-build-pipeline` module before any non-sandbox use. |
| **State in a per-project bucket** | Migrate to the shared tooling bucket once `bootstrap/iam.tf`, the bucket policy and the SSO attachment are in place. |
| **Credentials exposed in conversation** | The SkySpark password, a live SkySpark session cookie and `attest-key`, and the Tiger Cloud `tsdbadmin` password were all pasted during this deployment. All should be rotated. |
| **`receipt_mode: observed`** | Live SkySpark returns no completeness receipt, so these readers accept observed receipts. Production keeps `provider` mode, where they refuse to run. Certified output here is not production-grade. |
| **No schedules created** | `skyspark-schedules reconcile --apply` creates them, disabled by default. Runs are manual until then. |

---

## 14. Command reference

```powershell
# Control store migrations (from the project root)
$env:CONTROL_DATABASE_URL = "postgresql://skyspark_control_app:<pw>@aqy26lurzx.lhy6ly0as0.tsdb.cloud.timescale.com:32064/tsdb?sslmode=require"
skyspark-control migrate --migrations migrations/control

# Terraform, in order. Arguments MUST be quoted for PowerShell.
cd infra\foundation    ; terraform init "-backend-config=backend.hcl" ; terraform plan "-out=foundation.tfplan"    ; terraform apply "foundation.tfplan"
cd ..\orchestration    ; terraform init "-backend-config=backend.hcl" ; terraform plan "-out=orchestration.tfplan" ; terraform apply "orchestration.tfplan"
cd ..\workers          ; terraform init "-backend-config=backend.hcl" ; terraform plan "-out=workers.tfplan"       ; terraform apply "workers.tfplan"

# ECR login (--password, not --password-stdin, on Docker Desktop for Windows)
$REG = "001634074733.dkr.ecr.us-east-1.amazonaws.com"
$pw = aws ecr get-login-password --region us-east-1
docker login --username AWS --password $pw $REG

# Status. CloudWatch worker logs are always empty -- use the control store.
SELECT attempt_no, outcome, coalesce(error_class,'(none)') AS error, count(*)
FROM ingestion.attempts GROUP BY 1,2,3 ORDER BY 1;
SELECT status, count(*) FROM ingestion.jobs GROUP BY status;
SELECT count(*) FROM ingestion.raw_artifacts;

# Stop a run
aws stepfunctions stop-execution --execution-arn <arn>
```

### Key identifiers

| Resource | Value |
| --- | --- |
| VPC | `vpc-01738eeb78ae67b39` (10.20.0.0/16) |
| Private subnets | `subnet-0864b2e585f156e9a` (1a), `subnet-06608ba8721b4cc00` (1b) |
| Security group | `sg-05a4928dccf405988` (`skyspark-dev-tasks`, egress only) |
| NAT gateway | `nat-118e8b83d8e7f8f19` |
| KMS key | `arn:aws:kms:us-east-1:001634074733:key/9bcfe39b-8489-4450-b13e-990f530c2dac` |
| State machine | `arn:aws:states:us-east-1:001634074733:stateMachine:skyspark-dev-ingestion` |
| ECS cluster | `skyspark-dev-workers` |
| Control secret | `tripleq/development/skyspark/control-db` |
| Source secret | `tripleq/development/skyspark/credentials` |
