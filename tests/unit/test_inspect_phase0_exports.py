from __future__ import annotations

import importlib.util
import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
FIXTURES = ROOT / "local/fixtures/phase0"


def test_synthetic_exports_prove_cross_reference_report_without_source_values():
    script = ROOT / "scripts/inspect_phase0_exports.py"
    spec = importlib.util.spec_from_file_location("inspect_phase0_exports", script)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    report = module.inspect_exports(
        FIXTURES / "equipment.synthetic.csv",
        FIXTURES / "points.synthetic.csv",
        FIXTURES / "rules.synthetic.csv",
        FIXTURES / "history.synthetic.csv",
    )
    assert report["certified"] is False
    assert report["points"]["site_level_rows"] == 1
    assert report["points"]["missing_site_and_equipment_rows"] == 1
    assert report["rules"]["rows_with_known_equipment"] == 1
    assert report["history"]["rows_with_known_point"] == 2
    assert report["history"]["typed_value_column_counts"] == {
        "val_num": 1, "val_bool": 1,
    }
    output = json.dumps(report)
    assert "point-demo-1" not in output
    assert "equip-demo-1" not in output
