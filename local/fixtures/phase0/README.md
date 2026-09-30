# Phase 0 synthetic source shapes

These small CSVs contain invented IDs, names, timestamps, and values. They
exercise the key columns and edge cases seen in the four supplied `fsk=1001`
exports without copying source records. They do not prove SkySpark query
completeness or authorize a pilot project.

| Source export | SHA-256 | Shape used here |
| --- | --- | --- |
| `1001_equipment_20260924_150405.csv` | `FE2FFFD61B08085DD75D645D0C8FAAF54D0DD40F20F1944D6E8FBBC08A111D0D` | Equipment ID, site reference, parent equipment, tags |
| `1001_points_20260924_150405.csv` | `C7EC8E98814266DFE95E29CEB99DCBDFF1ED58B93A34D4B44DD29D9DACB7DD53` | Point ID, site/equipment reference, `his` marker, site-level and unresolved rows |
| `1001_hisread_20260925_153139.csv` | `40394D0334C0E4AAE33F5BD46BDDA168116AFD5CDB861764B04EE9EB5FDDE2F3` | Point reference, timestamp, typed value columns |
| `1001_rulespark_20260924_150545.csv` | `384C20B58B42BAF6C5A9D6E309F1C0FF8ADE2AE7DE36AEBF8ED0EABED0CA0C1A` | Equipment target, rule reference, date/time zone, detection fields |

The original CSVs remain outside this project. The source export timestamps
are filenames, not proven data-coverage windows.

`pilot-binding.example.yaml` uses the user-selected HTTP API root and project
URI. Its tenant/site labels and secret ARN remain placeholders; the paired
pilot scope contains invented source IDs. The documented default probe run is
a network-free dry run. Do not use these placeholders for live execution.
