# Phase 0 live SkySpark source evidence

**Probe date:** 28 September 2026. **Scope:** the one project URI the user
identified for the `fsk=1001` exports. The two source site references and probe
IDs were selected in memory from those exports. The external connector's
existing credential handling was used only for user-authorized read-only
calls. The connector, original scripts, and source data were not changed.

## Request templates and measured responses

The templates below show the source operations without source IDs or the API
host. Elapsed time is client wall time for one run; bytes are estimated decoded
JSON size after `phable` materialization, **not** network bytes. No paging or
explicit completeness token was observed in the response metadata.

| Check | Request template | Aggregate response | Client time |
| --- | --- | --- | ---: |
| Auth | `about` | Succeeded; Haystack about fields returned. | Not timed |
| Larger-site equipment | `readAll(equip and siteRef==@SITE)` | 249 rows; all 249 exported IDs matched; ~141 KB decoded. | 631 ms |
| Larger-site points | `readAll(point and siteRef==@SITE)` | 2,708 rows; all 2,708 exported IDs matched; ~2.74 MB decoded. | 6,766 ms |
| Smaller-site equipment | Same template for second site | 1 row; exported ID matched. | 346 ms |
| Smaller-site points | Same template for second site | 7 rows; all exported IDs matched. | 311 ms |
| Project equipment cross-check | `readAll(equip)` | 250 rows; all 250 exported IDs matched. | 596 ms |
| Project points cross-check | `readAll(point)` | 2,716 rows; all 2,716 exported IDs matched; one has no `siteRef`. | 6,222 ms |
| History, 1 ID | `hisRead` typed request, start `2026-09-24 04:50Z`, end `04:55Z` | One timestamp row, 2 columns. | 364 ms |
| History, 100 IDs | Same bounds | One timestamp row, 101 columns. | 587 ms |
| History, 500 IDs | Same bounds | One timestamp row, 501 columns; a repeat with a different 500-ID subset had 478 non-null values (349 numeric, 117 Boolean, 12 strings). | 1,309 ms; repeat 863 ms |
| Rule scope, 100 IDs | `readAll(equip and siteRef==@SITE and (id==@ID or ...))` | 100 rows; all requested IDs matched. | 537 ms |
| Rules, 100 IDs | `readAll(FILTER).ruleSparks(2026-09-24)` | 34 detections across 25 requested equipment targets. | 570 ms |
| Rule scope, 249 IDs | Same scope template | 249 rows; all requested IDs matched. | 975 ms |
| Rules, 249 IDs, export day | `readAll(FILTER).ruleSparks(2026-09-23)` | 37 detections across the same 30 equipment targets as the earlier export. | 878 ms |
| Valid no-detection equipment | Scope read followed by `ruleSparks(2026-09-24)` | Scope read: 1 equipment; rule result: 0 rows. | 311 ms; 310 ms |

The earlier rules CSV was produced on 24 September with `yesterday`, and its
rows have the date 23 September. The 24 September live rule query represents a
different rule day; its 35 detections for all 249 equipment must not be
compared with the 37-row export as if they were the same day.

## Confirmed source shape and implementation consequence

- The pilot project's two source sites contain 249/1 equipment and 2,708/7
  site-scoped points. A project-wide query also returns one point without
  `siteRef`. Site-scoped ingestion needs an explicit quarantine/disposition for
  that record; dropping it silently would make the count look complete when it
  is not.
- `his_read_by_ids` accepts 1, 100, and 500 IDs for this historical five-minute
  request. Its grid has a `ts` column and one `vN` value column per point; each
  value column's `meta.id` carries the point Ref. Each non-null point cell is
  an observation; a grid with one row may contain hundreds of observations.
  The worker needs a deterministic wide-to-long normalizer using `meta.id`,
  plus a requested-ID receipt that includes IDs with null values. The revised
  Phase 0 probe was checked against a live two-point grid: both metadata Refs
  matched the requested IDs and neither appeared in the report.
- A filtered `ruleSparks` expression accepts equipment IDs and can return a
  valid zero-row result. The independent equipment scope read proves the IDs
  existed before the rule query. It does not prove that zero-detection
  equipment was fully evaluated by the rule engine or that older detections
  have closed.

## Limits of this evidence

These are read-only observations from one project and a small historical
window. The `readAll` metadata had only `ver`; the rule grids had `ver` and
`span`; the history grids had `ver`, `hisStart`, and `hisEnd`. No explicit
truncation, paging, completeness, or rule-run token was observed. Absence of a
marker does not prove completeness. The exact inclusion of the end boundary,
late corrections, deletion semantics, maximum request size, error/retry
behavior, and sustained rate limits remain unverified. The observed 500-ID
history result is not a 2,000-site capacity benchmark.

The user selected `http://44.221.91.30:8888/api/` as the SkySpark root for this
project. The external connector has a literal credential fallback. The system
proxy rejected this host, so the approved one-off probe connected directly.
The new probe accepts this HTTP root but requires a separate Secrets Manager
credential reference for execution. That reference and credential rotation
have not yet been established. No credential, source ID, or source row is
stored in this evidence file.
