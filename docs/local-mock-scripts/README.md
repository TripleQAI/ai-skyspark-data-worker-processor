# HHC 5431 live-source / LocalStack scale exercise

The newer twelve-window, one-hour history test is documented in
[`../hhc5431-one-hour-local-test-results.md`](../hhc5431-one-hour-local-test-results.md).
Its output is in [`hhc5431-history-hour-20260928-2150-2250/`](hhc5431-history-hour-20260928-2150-2250/).
The exercise below describes an earlier single-window run.

Run output: [`run-20260928-site-scoped`](run-20260928-site-scoped). The input is `../HHC-5431.json`
(SHA-256 `570f83c9884aacb5fcec03009aee4c3b50be916127c091e1c8609e39ae3cf67f`).
The two scripts in this folder reproduce the read-only capture and local
synthetic workload. They make no changes to the worker implementation or
original scripts. Source credentials are required in process environment for
the live capture and are never saved in this folder.

## What the live source returned

At the user-selected SkySpark API root, 14 bounded read-only calls captured
250 equipment and 2,716 points for facility 5431's project. The JSON's one
facility maps to **two SkySpark `siteRef` values**. Of 2,716 points, 2,715
have the `his` tag; SkySpark rejected a history batch containing the remaining
point. That one point stays in `live_points.csv` and is recorded in
`live_history_exclusions.csv`. A single five-minute window,
**2026-09-28 20:25–20:30 UTC**, returned 1,824 point observations. Equipment
queries for **2026-09-27** returned 10 rule detections. This is an explicit UTC
test day; the source-local nightly cutover has not been validated. Scope checks matched
all 250 equipment IDs before calling the rule engine. The live files are
`live_equipment.csv`, `live_points.csv`, `live_history_exclusions.csv`,
`live_history_observations.csv`, `live_rule_scope.csv`,
`live_rule_detections.csv`, and `live_call_metrics.csv`.

## Synthetic workload and exact job counts

The 2,715 historized source points are duplicated into **10,000,000 distinct
synthetic point IDs**. This requires 3,684 logical facility copies: 3,683
complete copies and 655 points in the final copy. Each copy retains 250
equipment IDs and the source's two SkySpark site groups, giving **921,000
synthetic equipment IDs** and **7,368 source-site partitions**. Every
synthetic row carries its real source ID for provenance. Synthetic IDs are
never sent to SkySpark.

| Feed | Partition rule | Jobs | CSV result |
| --- | --- | ---: | --- |
| Metadata weekly | One source site per job | 7,368 | 20 point shards + 2 equipment shards |
| Rules nightly | At most 200 equipment, within a source site | 11,052 | 2 equipment-scope shards + 1 detection shard |
| History every 5 minutes | At most 500 points, within a source site | 25,783 | 20 point-coverage shards |

The 10,000,000 / 500 = 20,000 global division would cross facility
boundaries. The actual source-site plan is 3,683 × (6 + 1) + 2 = **25,783
history jobs**: 2,708 + 7 historized points per complete facility, with the
final partial copy in the larger source site. The 10,000,000 history-coverage
rows contain 6,718,243 replicated live values and 3,281,757 explicit
no-observation rows. The 10 source rule detections become 36,840 replicated
detections. They are marked synthetic.

Each feed used a separate LocalStack SQS queue and DLQ. The local worker used
four concurrent slots, consumed and acknowledged all **44,203 job references**,
and wrote plain CSV shards of at most 500,000 rows. LocalStack S3 bucket
`hhc5431-ac086c02ea-local` holds 57 gzip-compressed CSV objects for this run
(664,890,632 bytes). There are **58 plain CSV files** in the run directory,
about 6.14 GB total; `workflow_summary.csv` was written after the S3 upload
and is local only. `workflow_summary.json` records run IDs and counts.
The 58 CSVs are 45 synthetic data shards, 7 live-source files, 3 job
manifests, 1 schedule file, and 2 metric files. The 500,000-row shard limit
keeps the 10-million-row point and history outputs to 20 files each.

The three LocalStack Scheduler definitions match the project profile:
five-minute history, 02:00 UTC nightly rules, and 03:00 UTC Sunday metadata.
They are **disabled** because LocalStack stores schedules but does not fire
the full target chain in this setup. The runner invoked the three feeds once
to test a single history window, one rule day, and one metadata sweep.

## Verification and limits

An independent full-file scan confirmed 10,000,000 unique point metadata IDs,
the same 10,000,000 unique history-coverage IDs, 921,000 unique equipment
metadata IDs, 921,000 unique equipment rule-scope IDs, 6,718,243 rows with a
history value, and all three job-manifest counts. Every job and generated row
was checked against its source `siteRef`; no job crosses that boundary. The
three work queues and their DLQs were empty after acknowledgment. All three
schedules were present and disabled; S3 listed 57 uploaded objects. A history
shard downloaded from S3 matched its local CSV. The smoke runs and the superseded
facility-only partition run, including their isolated LocalStack resources,
were removed.

Four existing project integration tests also passed after running the reviewed
scripts in an isolated local Python environment: history to LocalStack S3 and
TimescaleDB, nightly rules to LocalStack S3, synthetic metadata S3 evidence,
and LocalStack schedule/Step Functions primitives. Those tests use the
project's separate contract fixture, not the HHC live rows.

This exercise tests local fan-out, partitioning, SQS transport, concurrency,
CSV writing, and S3 storage. It does **not** show that SkySpark can serve
10,000,000 distinct real point IDs every five minutes: SkySpark served the
2,715 real historized IDs once, then the local runner replicated the response.
The source grids did not provide completeness receipts, so these files are
not certified production ingestion. The local timing of 209.10 seconds is
not a SkySpark or AWS throughput benchmark.
