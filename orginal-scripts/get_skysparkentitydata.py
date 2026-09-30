"""
export_skyspark_data.py

Pulls equipment and point data from all configured SkySpark sites through
HaystackConnector and writes two CSV files.

Configuration precedence (highest first):
    1. Command-line arguments
    2. JSON config file (--config)
    3. Built-in defaults

Examples:
    python export_skyspark_data.py --sites-file sites.csv
    python export_skyspark_data.py --config export_config.json
    python export_skyspark_data.py --sites-file sites.csv --point-filter "point and sensor"

sites.csv format:
    fsk,uri
    1001,site_a
    1002,site_b
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from connectors.skyspark import HaystackConnector

try:
    from phable import Marker, Number, Ref
except ImportError:  # Allows the script to run with older phable versions
    Marker = Number = Ref = ()

logger = logging.getLogger("skyspark_export")

DEFAULT_CONFIG = {
    "root_url": None,               # None -> connector default or HAYSTACK_ROOT_URL
    "fsk": None,                    # Site key, added as a column and used in filenames
    "site_uri": None,               # Site URI appended to root_url
    "output_dir": "output",
    "equip_filter": "equip",
    "point_filter": "point",
    "equip_filename": "{fsk}_equipment_{timestamp}.csv",
    "point_filename": "{fsk}_points_{timestamp}.csv",
    "drop_columns": [],             # Columns to remove from both outputs
    "log_level": "INFO",
}


# ---------------------------------------------------------------------- #
# Configuration
# ---------------------------------------------------------------------- #
def load_config(argv: list[str] | None = None) -> dict:
    parser = argparse.ArgumentParser(description="Export SkySpark equip and point data to CSV.")
    parser.add_argument("--config", help="Path to a JSON config file.")
    parser.add_argument("--root-url", help="Haystack API root URL.")
    parser.add_argument("--fsk", help="Site key for the site to export.")
    parser.add_argument("--site-uri", help="Site URI appended to the root URL.")
    parser.add_argument("--output-dir", help="Directory for the CSV outputs.")
    parser.add_argument("--equip-filter", help="Haystack filter for equipment.")
    parser.add_argument("--point-filter", help="Haystack filter for points.")
    parser.add_argument("--log-level", help="DEBUG, INFO, WARNING, or ERROR.")
    args = parser.parse_args(argv)

    config = dict(DEFAULT_CONFIG)
    if args.config:
        with open(args.config, encoding="utf-8") as fh:
            config.update(json.load(fh))

    cli_values = {k: v for k, v in vars(args).items() if v is not None and k != "config"}
    config.update(cli_values)

    missing = [k for k in ("fsk", "site_uri") if not config.get(k)]
    if missing:
        parser.error(f"Missing required settings: {', '.join(missing)}")
    return config


# ---------------------------------------------------------------------- #
# Data retrieval
# ---------------------------------------------------------------------- #
def _read_site(client, filter_expr: str, fsk: str) -> pd.DataFrame:
    df = HaystackConnector._grid_to_df(client.read_all(filter_expr))
    if not df.empty:
        df.insert(0, "fsk", fsk)
    return df


def get_skyspark_equipment_data(client, config: dict) -> pd.DataFrame:
    logger.info("Reading equipment with filter: %s", config["equip_filter"])
    return _read_site(client, config["equip_filter"], config["fsk"])


def get_skyspark_point_data(client, config: dict) -> pd.DataFrame:
    logger.info("Reading points with filter: %s", config["point_filter"])
    return _read_site(client, config["point_filter"], config["fsk"])


# ---------------------------------------------------------------------- #
# Processing
# ---------------------------------------------------------------------- #
def _to_csv_value(value):
    """Convert Haystack kinds to CSV-friendly values."""
    # Haystack JSON v4 encoding, for example {"_kind": "ref", "val": "..."}
    if isinstance(value, dict) and "_kind" in value:
        kind = value["_kind"]
        if kind == "marker":
            return True
        if kind in ("ref", "number", "date", "time", "uri", "coord", "symbol"):
            return value.get("val", json.dumps(value))
        if kind == "dateTime":
            return value.get("val")
        return json.dumps(value)
    # Haystack JSON v3 encoding, for example "m:" or "r:abc123 Name"
    if isinstance(value, str) and len(value) > 1 and value[1] == ":":
        prefix, body = value[0], value[2:]
        if prefix == "m":
            return True
        if prefix == "r":
            return body.split(" ", 1)[0]
        if prefix == "n":
            return body.split(" ", 1)[0]
        return body
    if Marker and isinstance(value, Marker):
        return True
    if Ref and isinstance(value, Ref):
        return value.val
    if Number and isinstance(value, Number):
        return value.val
    if isinstance(value, (dict, list)):
        return json.dumps(value, default=str)
    return value


def process_data(df: pd.DataFrame, config: dict) -> pd.DataFrame:
    if df.empty:
        return df

    df = df.copy()
    for col in df.columns:
        if df[col].dtype == object:
            df[col] = df[col].map(_to_csv_value)

    drop = [c for c in config.get("drop_columns", []) if c in df.columns]
    if drop:
        df = df.drop(columns=drop)

    # Place identifying columns first when present
    lead = [c for c in ("fsk", "id", "dis", "navName", "siteRef", "equipRef") if c in df.columns]
    return df[lead + [c for c in df.columns if c not in lead]]


# ---------------------------------------------------------------------- #
# Output
# ---------------------------------------------------------------------- #
def write_to_csv(df: pd.DataFrame, filename_template: str, config: dict) -> Path | None:
    if df.empty:
        logger.warning("No data to write for %s", filename_template)
        return None

    output_dir = Path(config["output_dir"])
    output_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = output_dir / filename_template.format(fsk=config["fsk"], timestamp=timestamp)

    df.to_csv(path, index=False)
    logger.info("Wrote %d rows to %s", len(df), path)
    return path


# ---------------------------------------------------------------------- #
# Entry point
# ---------------------------------------------------------------------- #
def main(argv: list[str] | None = None) -> int:
    config = load_config(argv)
    logging.basicConfig(
        level=config["log_level"].upper(),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    try:
        conn = HaystackConnector(root_url=config["root_url"])
        logger.info("Connecting to site %s at %s", config["fsk"], conn.build_url(config["site_uri"]))

        # One connection serves both reads
        with conn.connect(config["site_uri"]) as client:
            equip_df = get_skyspark_equipment_data(client, config)
            point_df = get_skyspark_point_data(client, config)

        equip_df = process_data(equip_df, config)
        point_df = process_data(point_df, config)

        write_to_csv(equip_df, config["equip_filename"], config)
        write_to_csv(point_df, config["point_filename"], config)
    except Exception:
        logger.exception("Export failed.")
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())