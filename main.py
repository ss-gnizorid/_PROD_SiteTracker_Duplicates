import argparse
from pathlib import Path

from src.config.config import load_config_yaml
from scripts.main.build_hash_index import run_from_config as build_index_from_config
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

    # Build or incrementally update index
    log.info("Building/updating index from S3")
    build_index_from_config(cfg)


if __name__ == "__main__":
    main()
