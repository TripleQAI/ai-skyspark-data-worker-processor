# AWS development environment: live SkySpark readers at 1,000-site scale

This folder configures the worker pipeline to read the real SkySpark server
and write every result to S3. It also fans one real site out to a
configurable number of approved sites to load-test the pipeline at estate
scale. The default is 1,000 sites × 200 equipment × 50 points, which is
10,000,000 historized points.

| File | Purpose |
| --- | --- |
| `scope.yaml` | The only file to edit: endpoint, secret, source site, site count, and replica shape. |
| `profile.yaml` | Schedules and partitions. All three feeds target `s3`. |
| `plugins.yaml` | Pins the three reader scripts in [`readers/skyspark/`](../../../readers/skyspark/) by SHA-256. |
| `resources.yaml` | AWS names, read/storage caps, and the `source_client` policy. |
| `private/binding.yaml` | Generated from `scope.yaml`; git-ignored because it names the real site. |

## Reader scripts

| Script | Replaces | Source call per job |
| --- | --- | --- |
| `skyspark_metadata.py` | `get_skysparkentitydata.py` | `read (equip) and siteRef==@SITE`, then `read (point) and siteRef==@SITE` |
| `skyspark_rules.py` | `get_skysparkrulespark.py` | Equipment scope read, then `readAll(...).ruleSparks(day)` for up to 200 equipment |
| `skyspark_history.py` | `get_skysparktimeseriesdata.py` | One `hisRead` for up to 500 points over the five-minute window |

Each script writes raw evidence under `raw/<tenant>/<project>/<feed>/<run>/<job>/`
and certified-candidate JSONL under `certified/...` in the configured buckets.
It then returns a completion that the worker verifies against S3 before
certifying the job. Nothing is hard-coded:

- The endpoint, site, time zones and replica shape come from the binding.
- The filters, credential variable, caps and receipt mode come from `resources.yaml`.
- The credentials come from Secrets Manager through ECS.

## Two deliberate trust decisions

1. **Observed receipts (`source_client.receipt_mode: observed`).** SkySpark
   returns no completeness receipt, so a reader treats a response as complete
   only when all three hold:
   - there is no error grid;
   - there is no meta key containing `trunc` or `limit`;
   - every requested ID is answered. For history that means a column for
     every point; for rules, the equipment scope read returns exactly the
     requested equipment.

   This is weaker than a provider receipt, so raw evidence records
   `receipt_kind: observed`. Production keeps the default `provider` mode,
   where these readers refuse to run.
2. **Replica sites (`source_replica`).** Every generated site maps to the one
   real `source_site_ref`, and IDs are namespaced by site:
   - equipment: `<site>.e<i>.<real equipment id>`
   - points: `<site>.e<i>.p<j>.<real point id>`

   Equipment cycle through the real equipment, and points cycle through the
   real **historized** points. The values are real, but the topology is
   synthetic. One real rule detection appears on every virtual equipment that
   copies that equipment, and those rows are tagged `replicaSourceRef`. Before
   any source call, readers deduplicate virtual IDs back to real IDs, so a
   history job never requests more than 500 real points.

## Setup

1. **Fill in `scope.yaml`.** Supply the project API URL (the SkySpark root plus
   the project name), the Secrets Manager ARN, the real site's Haystack id,
   and its IANA zone and SkySpark `tz` tag. Keep `tenant_id`/`project_id`
   consistent with the Terraform `approved_scopes`.
2. **Generate the binding.** The command prints the planned jobs per run:
   ```powershell
   python scripts/generate_scale_binding.py --scope config/environments/aws-dev/scope.yaml
   ```
3. **Store the credential** as a JSON secret `{"username": "...", "password": "..."}`
   in Secrets Manager. Never put it in a file in this repo.
4. **Build the worker image:**
   ```powershell
   docker build -f Dockerfile.worker `
     --build-arg PYTHON_BASE_IMAGE=<digest-pinned python:3.12> `
     --build-arg RESOURCE_CONFIG_FILE=config/environments/aws-dev/resources.yaml `
     --build-arg SCRIPT_ROOT_DIR=readers/skyspark `
     --build-arg PACKAGE_EXTRAS="[source]" .
   ```
5. **Apply Terraform** with:
   - `resource_config_file = "config/environments/aws-dev/resources.yaml"`;
   - `bucket_names` matching `resources.yaml`;
   - `approved_scopes = ["dev-tenant/dev-scale"]`;
   - `source_secret_arns = { SKYSPARK_SOURCE_CREDENTIALS = "<secret ARN>" }`.
6. **Run in feed order.** Metadata first, then `publish-metadata-inventory`
   for that run, then `seed-checkpoint` per site for history and rules. After
   that, enable the history and rules schedules. All schedules default to
   `DISABLED`.

## Ramp up; do not start at 1,000

Set `sites.count` to 10, then 100, then 1,000, and regenerate the binding
between steps. The figures below come from the Phase 0 live probe
(`docs/phase0-live-source-evidence.md`) and the default partitions:

| Feed | Jobs per run at 1,000 sites | SkySpark calls | Observed call time |
| --- | ---: | ---: | --- |
| Metadata (weekly) | 1,000 | 2,000 | ~0.6 s equipment + ~6.8 s points |
| Rules (daily) | 1,000 | 2,000 | ~1 s each |
| History (every 5 min) | 20,000 | 20,000 per 5 min | ~0.9–1.3 s per 500-point read |

**History at 1,000 sites will not keep up with one SkySpark server.** With
`source_policy.max_concurrent_calls: 12` and about 1.1 s per call, the source
sustains roughly 11 calls/s. That is about 3,300 jobs per five minutes, or
around **165 replica sites**. Raising the cap moves all of that load onto the
SkySpark host. It is a real server, so agree the ramp with its owner first.

Other limits the scale test will hit, none of them fixed here:

- **Inventory publication.** The publisher holds every site's entities
  (10.2 million, with tags) in memory, which exceeds the 10 GB Lambda limit.
  Run `publish-metadata-inventory` on a large Fargate task or host for more
  than about 100 sites.
- **S3 volume.** Each job writes 4 objects. At 1,000 sites, history writes
  about 23 million objects a day: roughly $115/day in PUT requests before
  storage. A metadata run is about 15 MB raw plus 15 MB certified per site.
- **Control database.** It records about 5.8 million history jobs a day.
  Watch dispatcher throughput, `SKIP LOCKED` contention, and table growth.
