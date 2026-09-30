"""
get_skysparkrulespark.py

Runs a SkySpark Axon expression (default: readAll(equip).ruleSpark(yesterday))
against a single site and writes the result grid to CSV.

Examples:
    python get_skysparkrulespark.py --fsk 1001 --site-uri demoSite
    python get_skysparkrulespark.py --fsk 1001 --site-uri demoSite --date-range "lastWeek"
    python get_skysparkrulespark.py --fsk 1001 --site-uri demoSite --expr "readAll(ahu).ruleSpark(today)"
    python get_skysparkrulespark.py --config rulespark_config.json
"""

from __future__ import annotations

import argparse
import json
import logging
import sys

import pandas as pd
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
# Match these imports to the ones used in get_skysparkentitydata.py
from connectors.skyspark import HaystackConnector
from get_skysparkentitydata import process_data, write_to_csv

logger = logging.getLogger("skyspark_rulespark")

DEFAULT_CONFIG = {
    "root_url": None,
    "fsk": None,
    "site_uri": None,
    "equip_filter": "equip",
    "date_range": "yesterday",
    "expr": None,                   # Full Axon override; ignores equip_filter and date_range
    "output_dir": "output",
    "output_filename": "{fsk}_rulespark_{timestamp}.csv",
    "drop_columns": [],
    "log_level": "INFO",
}


# ---------------------------------------------------------------------- #
# Configuration
# ---------------------------------------------------------------------- #
def load_config(argv: list[str] | None = None) -> dict:
    parser = argparse.ArgumentParser(description="Export SkySpark ruleSpark results to CSV.")
    parser.add_argument("--config", help="Path to a JSON config file.")
    parser.add_argument("--root-url", help="Haystack API root URL.")
    parser.add_argument("--fsk", help="Site key for the site to export.")
    parser.add_argument("--site-uri", help="Site URI appended to the root URL.")
    parser.add_argument("--equip-filter", help="Filter passed to readAll(). Default: equip")
    parser.add_argument("--date-range", help="Axon date range. Default: yesterday")
    parser.add_argument("--expr", help="Full Axon expression; overrides filter and date range.")
    parser.add_argument("--output-dir", help="Directory for the CSV output.")
    parser.add_argument("--log-level", help="DEBUG, INFO, WARNING, or ERROR.")
    args = parser.parse_args(argv)

    config = dict(DEFAULT_CONFIG)
    if args.config:
        with open(args.config, encoding="utf-8") as fh:
            config.update(json.load(fh))
    config.update({k: v for k, v in vars(args).items() if v is not None and k != "config"})

    missing = [k for k in ("fsk", "site_uri") if not config.get(k)]
    if missing:
        parser.error(f"Missing required settings: {', '.join(missing)}")
    return config


def build_expr(config: dict) -> str:
    if config.get("expr"):
        return config["expr"]
    return f"readAll({config['equip_filter']}).ruleSparks({config['date_range']})"


# ---------------------------------------------------------------------- #
# Data retrieval
# ---------------------------------------------------------------------- #
def eval_axon(client, expr: str):
    """Evaluate an Axon expression with whichever method this phable version supports."""
    if hasattr(client, "eval"):
        return client.eval(expr)

    request = {
        "_kind": "grid",
        "meta": {"ver": "3.0"},
        "cols": [{"name": "expr"}],
        "rows": [{"expr": expr}],
    }
    return client.call("eval", request)


def check_error(grid) -> None:
    """Raise if SkySpark returned an error grid."""
    meta = grid.get("meta", {}) if isinstance(grid, dict) else getattr(grid, "meta", {}) or {}
    if "err" in meta:
        raise RuntimeError(f"SkySpark error: {meta.get('dis', 'unknown error')}")


def get_skyspark_rulespark_data(client, config: dict) -> pd.DataFrame:
    expr = build_expr(config)
    logger.info("Evaluating: %s", expr)

    grid = eval_axon(client, expr)
    check_error(grid)

    df = HaystackConnector._grid_to_df(grid)
    if not df.empty:
        df.insert(0, "fsk", config["fsk"])
    return df


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

        with conn.connect(config["site_uri"]) as client:
            df = get_skyspark_rulespark_data(client, config)

        df = process_data(df, config)
        write_to_csv(df, config["output_filename"], config)
    except Exception:
        logger.exception("RuleSpark export failed.")
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())