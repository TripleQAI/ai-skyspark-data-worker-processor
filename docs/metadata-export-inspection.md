# Read-only metadata export inspection

The new `inspect-metadata-export` command was run against the supplied project `1001` equipment and point CSV files. The expected site refs for this assessment were the two distinct `siteRef` values observed in the equipment export; that choice inspects the files and does **not** approve those sites for ingestion.

| Source file | File timestamp in name | Size | SHA-256 |
| --- | --- | ---: | --- |
| `1001_equipment_20260924_150405.csv` | 2026-09-24 15:04:05 | 92,810 bytes | `FE2FFFD61B08085DD75D645D0C8FAAF54D0DD40F20F1944D6E8FBBC08A111D0D` |
| `1001_points_20260924_150405.csv` | 2026-09-24 15:04:05 | 1,841,324 bytes | `C7EC8E98814266DFE95E29CEB99DCBDFF1ED58B93A34D4B44DD29D9DACB7DD53` |

The filename timestamp is an export label, not a source revision or completeness guarantee. Both files have filesystem modification time 2026-09-24 19:04:05 UTC.

| Finding | Count |
| --- | ---: |
| Equipment rows / unique accepted equipment | 250 / 250 |
| Point rows / unique point IDs | 2,716 / 2,716 |
| Points with an accepted site ref | 2,715 |
| Accepted historized points (`his=True`) | 2,715 |
| Accepted site-level points with no `equipRef` | 5 |
| Points with no `siteRef` and no `equipRef` | 1 |
| Unknown or cross-site equipment references among accepted-site points | 0 |

There are two observed site refs: one has 249 equipment and 2,708 points; the other has one equipment and seven points. The remaining point has no site ref. The five site-level points remain in the candidate history scope because their `his` tag is present; the orphan is reported as `missing_site_ref` and excluded from site-scoped counts. No point is silently assigned to an equipment or site.

**Certification status:** blocked. CSV files do not contain per-site query-completeness receipts, and the orphan point needs a source or registry decision. The inspector emits counts and file hashes, never a planner-compatible `CertifiedInventory`. Live metadata extraction and approved site mapping remain required.
