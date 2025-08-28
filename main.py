import argparse
from pathlib import Path

from src.config.config import load_config_yaml
from scripts.main.build_hash_index import run_from_config as build_index_from_config
from scripts.main.generate_presigned_links import run_from_config as generate_links_from_config
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

    # Step 1: Build or incrementally update hash index
    log.info("=== STEP 1: Building/updating hash index from S3 ===")
    build_index_from_config(cfg)
    
    # Step 2: Generate or refresh pre-signed links
    log.info("=== STEP 2: Generating/refreshing pre-signed links ===")
    generate_links_from_config(cfg)
    
    log.info("=== COMPLETE: Hash index and links have been processed ===")


if __name__ == "__main__":
    main()
