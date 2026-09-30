# SkySpark Phase 0 source-contract status

**Status: ACCEPTED by the user on 28 September 2026 for implementation
progression.** The local inspection, synthetic fixtures, secret preflight, and
read-only pilot probe are complete. A user-authorized, bounded live probe
verified the supplied project's metadata, 1/100/500-point history requests,
and equipment-filtered rule queries. This acceptance does not certify
production source completeness or estate-wide capacity. The decisions listed
below remain prerequisites for production rollout; illustrative partition
sizes are not production settings.

## Verified local evidence

| Source | Observation | Limit |
| --- | --- | --- |
| `orginal-scripts/get_skysparkentitydata.py` | Reads equipment and points and exports CSV. SHA-256 `56A653490E5003A6A3580C77326449E02A2F08FD1EAAEA7028583E41B4F82EBE`. | Export has no source paging/completeness receipt. |
| `orginal-scripts/get_skysparktimeseriesdata.py` | Selects random points from the newest CSV and queries one point at a time. SHA-256 `34DC44BF9257E700022400FB074897A3D01684DFD672ECC7F11CDFBC5E70C4EF`. | Failed reads are skipped; this is not a complete five-minute extraction. |
| `orginal-scripts/get_skysparkrulespark.py` | Uses project-wide `readAll(equip).ruleSparks(...)`. SHA-256 `D8EB608D0369A82B2CEAE495D3D4C19F01A90D46659711F971E4D13359F34D7D`. | Trigger rows alone cannot prove every equipment was queried. |
| External `C:\Projects\src\src\connectors\skyspark.py` | Exposes the selected **HTTP** API root and uses `phable`; SHA-256 `7C59C79762D67BA14096F4ACE45CB2C1EE393F949C8AC88089F503BE9F0CEF9A`. Live calls accepted 1, 100, and 500 history IDs over one five-minute UTC window. | This is one small historical window, not a sustained capacity measurement. The legacy file contains a literal credential fallback; its credential is not copied here. |

The three original scripts are unchanged. The source files above have filesystem
modification times on 24 September 2026; those times are **not** document
revisions or data-coverage timestamps. The export filenames likewise do not
establish the queried time window.

The supplied four `fsk=1001` CSV exports were inspected read-only. Their
SHA-256 fingerprints and synthetic replacements are in
`local/fixtures/phase0/README.md`. The reproducible
`scripts/inspect_phase0_exports.py` report gives:

| Export observation | Count / result |
| --- | ---: |
| Unique equipment | 250 of 250 rows |
| Unique points | 2,716 of 2,716 rows |
| Points accepted under the two observed site refs | 2,715 |
| Site-level points without `equipRef` | 5 |
| Point without either `siteRef` or `equipRef` | 1; unresolved |
| Rule detections | 37 rows, all targeting known equipment |
| Distinct equipment with a detection | 30 of the 250 exported equipment |
| History observations | 440 rows from only 2 point IDs |
| History typed values | 149 Boolean, 291 numeric |
| History timestamps | 24 September 2026 04:50Z to 25 September 2026 05:10Z |

The rules export has no explicit revision, closure, or detection-ID column.
Absence of a rule row cannot be treated as a closed detection. The history
export spans more than a day and cannot validate five-minute coverage. The
`fsk=1001` filename is an export label, not proof of the real tenant,
SkySpark project, authorized sites, or current source state.

## Live pilot source evidence (28 September 2026)

The user supplied the `--site-uri` used for the original `fsk=1001` exports and
authorized a bounded read-only check. Authentication through the external
connector succeeded. The calls and aggregate results are recorded in
[`phase0-live-source-evidence.md`](phase0-live-source-evidence.md). No source
rows, source IDs, URL, or credential were saved in the report. The original
scripts and connector remain unchanged.

The source returned 250 equipment and 2,716 points for project-wide reads,
matching every exported ID. The two site-filtered reads returned 249/1
equipment and 2,708/7 points. One project point has no `siteRef`; it is
ineligible for site-scoped ingestion until the data provider resolves it.

History requests for 1, 100, and 500 IDs all succeeded. The response is a
**wide grid**: one `ts` column and one value column per requested point, rather
than one row per point observation. The live grid names those columns `v0`,
`v1`, and so on; each column's `meta.id` carries the actual point Ref. A 500-ID
request over the tested five-minute
window returned one timestamp row with 478 non-null values. The history reader
must normalize non-null cells into per-point observations and retain the
requested-ID receipt; grid row count is not observation count.

The source accepted equipment-filtered `ruleSparks` for 1, 100, and all 249
equipment in the larger site. For the export's actual rule day, the 249-ID
query returned 37 detections across the same 30 equipment targets found in the
CSV. A one-equipment query with no detection returned a valid zero-row grid.
This shows bounded query syntax and empty-response shape, but a zero-row result
does not prove completed rule evaluation or closure semantics.

## Read-only probe delivered

`skyspark-source-probe` uses a reviewed `SourceBinding` and a private
`PilotScope`. Its only source operations are fixed, site-filtered `readAll`
queries, `phable.his_read_by_ids` with explicit UTC start/end bounds, and
equipment-filtered `ruleSparks` queries. Input IDs are restricted to plain
Haystack Refs; arbitrary Axon text is not accepted. The probe is separate from
workers and never certifies a job or inventory.

It caps one plan at 20 source calls, one-hour history windows, approved ID
counts, decoded rows, and estimated decoded bytes. The report stores counts,
non-history schema column names, meta key names, elapsed time,
requested-scope hashes, errors by class, and equipment-scope comparison.
History column identifiers, including `meta.id`, are redacted; the report
counts matched point columns and non-null value cells instead. It stores no URL, credential,
source ID, or source row. The estimated decoded byte count is **not** a
network-byte measurement;
`phable` materializes a grid before this cap is checked. Run only on a small
approved pilot and measure source response limits separately.

The example files under `local/fixtures/phase0/` contain invented scope IDs,
tenant names, and a placeholder secret reference; the binding now uses the
user-selected SkySpark HTTP root and project URI. The default dry run makes no
network calls. For a real pilot, place private copies under ignored
`local/pilot/` and write reports under ignored `local/probe-results/`.
The binding must name `http://44.221.91.30:8888/api/` and an AWS Secrets Manager
ARN containing a JSON secret with `username` and `password`. The executing
identity needs permission to read that secret. The probe accepts the selected
HTTP root and never imports the external connector or its embedded credential.

Run a network-free validation after installing the package:

```powershell
skyspark-source-probe --binding local/fixtures/phase0/pilot-binding.example.yaml --scope local/fixtures/phase0/pilot-scope.example.json
```

After the source owner approves the real scope, query templates, and rotated
secret, replace the placeholder tenant, site, and secret references in private
files and run:

```powershell
skyspark-source-probe --binding local/pilot/binding.yaml --scope local/pilot/scope.json --execute --report local/probe-results/pilot-001.json
```

The authorized legacy check used history batches of 1, 100, and 500 IDs and
verified that `ruleSparks(YYYY-MM-DD)` is accepted. The production probe still
requires a private approved binding and rotated secret. The equipment scope
check runs before each rule query; a successful zero-row rule result still
needs source-owner confirmation that the engine evaluated every requested
equipment. Probe errors and any truncation marker block source-contract
approval.

## Credential and repository boundary

`scripts/secret_preflight.py` scanned 124 files in the requested project with
zero findings on 28 September 2026. An explicit read-only scan of the external
legacy connector found a literal credential assignment on line 40; no value
was printed or copied. The three Dockerfiles run the preflight over packaged
code, scripts, and resource configuration before installing the package; the
build context excludes original exports and private pilot files. This
conservative preflight does not replace a
repository-wide secret scanner at commit time. The project is not yet a Git
repository, so its remote, branch, and tracked file set are unconfirmed.
The user authorized the one-off read-only legacy probe recorded above.
Rotate the old source credential before production use, a scheduled probe, or
any image use or commit that could expose the connector.

## Decisions carried forward after Phase 0 acceptance

1. **Approved scope:** the real tenant → user-supplied SkySpark project → site
   mapping, the two observed source `siteRef` values, and ongoing permission to
   read each feed. The user's approval covered this bounded verification; it
   does not establish a durable project registry or tenant/customer isolation.
2. **Credential path:** credential-owner confirmation of rotation and the
   Secrets Manager ARN/IAM access for the read-only identity. The selected
   SkySpark root is HTTP; credentials and source data are unencrypted in transit
   on that connection. This is an accepted endpoint decision, not a remaining
   HTTPS gate.
3. **Metadata contract:** pagination/partition or explicit truncation proof,
   full query and response receipts, and deletion/zero-result semantics. The
   one unscoped point needs a disposition. The matching CSV is a snapshot
   cross-check, not a source completeness token.
4. **History contract:** empty-window behavior, exact boundary semantics,
   late corrections, sustained request limits, and network-byte measurements.
   The live 1/100/500-ID calls prove one historical five-minute request shape
   and latency only.
5. **Rule contract:** source-owner confirmation of zero-detection equipment
   coverage, stable detection/revision identity, correction/closure behavior,
   and the nightly UTC interval. Live filtered reads do not supply a completion
   token.
6. **Joint approval:** the data provider (layer-0) and data consumer
   (ai-data-foundation) review the sanitized probe report, query templates,
   permissions, and measured limits. The intended Git remote and branch
   should be confirmed before source control is initialized.

Phase 0 is **accepted for implementation progression** on the pilot evidence
above. The listed decisions and source-owner receipts remain required before
production rollout. Production metadata, history, and rule readers must not
claim source completeness from the CSVs, synthetic fixture, or this bounded
probe.
