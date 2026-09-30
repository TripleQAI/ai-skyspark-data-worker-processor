"""
get_skysparkhisread.py

Selects random points from a points CSV produced by get_skysparkentitydata.py,
runs read(id==@<id>).hisRead(<range>) for each one, and writes the combined
history to a single CSV.

Examples:
    python get_skysparkhisread.py --fsk 1001 --site-uri demoSite
    python get_skysparkhisread.py --fsk 1001 --site-uri demoSite --points-file output/1001_points_20260924_143000.csv
    python get_skysparkhisread.py --fsk 1001 --site-uri demoSite --sample-size 5 --date-range lastWeek --seed 42
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

import pandas as pd
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
# Match these imports to the ones used in your other scripts
from connectors.skyspark import HaystackConnector
from get_skysparkentitydata import process_data, write_to_csv
from get_skysparkrulespark import check_error, eval_axon

logger = logging.getLogger("skyspark_hisread")

DEFAULT_CONFIG = {
    "root_url": None,
    "fsk": None,
    "site_uri": None,
    "points_file": None,            # None -> newest {fsk}_points_*.csv in output_dir
    "id_column": "id",
    "sample_size": 2,
    "seed": None,                   # Set an integer for repeatable samples
    "his_only": True,               # Sample only points tagged "his"
    "date_range": "yesterday",
    "expr_template": "read(id==@{id}).hisRead({date_range})",
    "output_dir": "output",
    "output_filename": "{fsk}_hisread_{timestamp}.csv",
    "drop_columns": [],
    "log_level": "INFO",
}


# ---------------------------------------------------------------------- #
# Configuration
# ---------------------------------------------------------------------- #
def load_config(argv: list[str] | None = None) -> dict:
    parser = argparse.ArgumentParser(description="Export hisRead data for random points to CSV.")
    parser.add_argument("--config", help="Path to a JSON config file.")
    parser.add_argument("--root-url", help="Haystack API root URL.")
    parser.add_argument("--fsk", help="Site key for the site to export.")
    parser.add_argument("--site-uri", help="Site URI appended to the root URL.")
    parser.add_argument("--points-file", help="Points CSV. Default: newest file for this fsk.")
    parser.add_argument("--sample-size", type=int, help="Number of random points. Default: 2")
    parser.add_argument("--seed", type=int, help="Random seed for repeatable samples.")
    parser.add_argument("--date-range", help="Axon date range. Default: yesterday")
    parser.add_argument("--output-dir", help="Directory for input lookup and CSV output.")
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


# ---------------------------------------------------------------------- #
# Point selection
# ---------------------------------------------------------------------- #
def find_points_file(config: dict) -> Path:
    if config.get("points_file"):
        path = Path(config["points_file"])
    else:
        matches = sorted(
            Path(config["output_dir"]).glob(f"{config['fsk']}_points_*.csv"),
            key=lambda p: p.stat().st_mtime,
        )
        if not matches:
            raise FileNotFoundError(
                f"No points file found for fsk {config['fsk']} in {config['output_dir']}. "
                "Run get_skysparkentitydata.py first or pass --points-file."
            )
        path = matches[-1]

    if not path.exists():
        raise FileNotFoundError(f"Points file not found: {path}")
    return path


def select_random_points(config: dict) -> pd.DataFrame:
    path = find_points_file(config)
    logger.info("Reading points from %s", path)
    df = pd.read_csv(path, dtype=str)

    id_col = config["id_column"]
    if id_col not in df.columns:
        raise ValueError(f"Column '{id_col}' not found in {path}")

    df = df.dropna(subset=[id_col])
    if config["his_only"] and "his" in df.columns:
        df = df[df["his"].str.lower().isin(["true", "m:", "✓"])]

    if df.empty:
        raise ValueError("No eligible points found in the points file.")

    n = min(int(config["sample_size"]), len(df))
    if n < int(config["sample_size"]):
        logger.warning("Only %d eligible points available; sampling all of them.", n)
    return df.sample(n=n, random_state=config.get("seed"))


def clean_id(raw_id: str) -> str:
    """Normalise an id to the bare ref value, for example p:site:r:abc."""
    value = str(raw_id).strip()
    if value.startswith("r:"):
        value = value[2:].split(" ", 1)[0]
    return value.lstrip("@")


# ---------------------------------------------------------------------- #
# History retrieval
# ---------------------------------------------------------------------- #
def get_point_history(client, point_id: str, config: dict) -> pd.DataFrame:
    expr = config["expr_template"].format(id=point_id, date_range=config["date_range"])
    logger.info("Evaluating: %s", expr)

    grid = eval_axon(client, expr)
    check_error(grid)
    return HaystackConnector._grid_to_df(grid)


def get_skyspark_his_data(client, points: pd.DataFrame, config: dict) -> pd.DataFrame:
    frames = []
    for _, point in points.iterrows():
        point_id = clean_id(point[config["id_column"]])
        try:
            df = get_point_history(client, point_id, config)
        except Exception as exc:
            logger.error("hisRead failed for %s: %s", point_id, exc)
            continue

        if df.empty:
            logger.warning("No history returned for %s", point_id)
            continue

        df.insert(0, "fsk", config["fsk"])
        df.insert(1, "point_id", point_id)
        df.insert(2, "point_dis", point.get("dis") or point.get("navName"))
        frames.append(df)
        logger.info("Retrieved %d rows for %s", len(df), point_id)

    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


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
        points = select_random_points(config)
        conn = HaystackConnector(root_url=config["root_url"])
        logger.info("Connecting to site %s at %s", config["fsk"], conn.build_url(config["site_uri"]))

        with conn.connect(config["site_uri"]) as client:
            df = get_skyspark_his_data(client, points, config)

        df = process_data(df, config)
        write_to_csv(df, config["output_filename"], config)
    except Exception:
        logger.exception("hisRead export failed.")
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())