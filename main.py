import argparse
from pathlib import Path

import yaml

from src.config.config import load_config_yaml
from scripts.main.build_hash_index import run_from_config as build_index_from_config
from scripts.main.scan_duplicates import run_from_paths as scan_dupes_from_paths
from src.utils.logger import setup_logging, get_logger


def main():
    setup_logging()
    log = get_logger("main")
    parser = argparse.ArgumentParser(prog="siteTrackerDuplicateDetection")
    parser.add_argument(
        "--config",
        type=str,
        required=False,
        default=str(Path("configs") / "main_config.yaml"),
        help="Path to YAML config (defaults to configs/main_config.yaml)",
    )
    args = parser.parse_args()

    cfg_path = Path(args.config)
    log.info(f"Using config file: {cfg_path}")
    cfg = load_config_yaml(cfg_path)

    # Step 1: build or incrementally update index
    log.info("Building/updating index from S3")
    build_index_from_config(cfg)

    # Step 2: duplicate scan (optional based on config)
    # If an index path exists from output and duplicates section present, run scan
    if cfg.output_path and cfg.output_target in ("local_csv", "local_parquet"):
        index_path = cfg.output_path.with_suffix(".csv") if cfg.output_target == "local_csv" else cfg.output_path.with_suffix(".parquet")
        if index_path.exists():
            log.info("Running duplicate scan on index")
            dup_out = index_path.with_name(index_path.stem + "_duplicates" + index_path.suffix)
            scan_dupes_from_paths(index_path, dup_out, cfg.duplicate_threshold, cfg.duplicate_bands)


if __name__ == "__main__":
    main()
