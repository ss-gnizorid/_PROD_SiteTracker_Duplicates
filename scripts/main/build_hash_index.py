from pathlib import Path
import sys

# Add the project root to Python path for imports
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

import pandas as pd

from src.clients.aws_client import S3Client
from src.config.config import AppConfig, load_config_yaml
from src.tools.hash_indexer import HashIndexer
from src.tools.permutation_generator import PermutationConfig, PermutationGenerator
from src.utils.io import write_dataframe
from src.utils.state import IncrementalState
from src.utils.logger import get_logger


def run_from_config(cfg: AppConfig) -> None:
    log = get_logger("build_hash_index")
    
    try:
        log.info("Initializing S3 client...")
        s3 = S3Client(
            region_name=cfg.aws_region,
            profile_name=cfg.aws_profile,
            assume_role_arn=cfg.aws_assume_role_arn,
            external_id=cfg.aws_external_id,
        )

        # Incremental state
        log.info("Loading incremental state...")
        state = IncrementalState.load(cfg.state_path)

        # List objects and filter by state
        log.info(f"Starting S3 listing for bucket: {cfg.s3_bucket}, prefix: {cfg.s3_root_prefix}")
        if cfg.s3_max_jobs_to_process:
            log.info(f"Limiting to {cfg.s3_max_jobs_to_process} jobs for testing/debugging")
            
        try:
            all_objs = s3.list_s3_images_with_metadata(
                cfg.s3_bucket, 
                cfg.s3_root_prefix,
                max_jobs_to_process=cfg.s3_max_jobs_to_process
            )
            log.info(f"Discovered {len(all_objs)} image candidates under {cfg.s3_bucket}/{cfg.s3_root_prefix}")
        except KeyboardInterrupt:
            log.error("S3 listing was interrupted by user")
            raise
        except Exception as e:
            log.error(f"Failed to list S3 objects: {e}")
            raise
            
        todo = [o for o in all_objs if state.needs_processing(o.key, o.etag)]
        log.info(f"Images to process (new/changed): {len(todo)}")

        if not todo:
            log.info("No new images to process.")
            return

        gen = PermutationGenerator(PermutationConfig(resize=(cfg.resize_width, cfg.resize_height), mode=cfg.hash_mode))
        indexer = HashIndexer(s3=s3, generator=gen, max_workers=cfg.max_workers)
        log.info(f"Starting hashing with max_workers={cfg.max_workers} mode={cfg.hash_mode}")
        df_new = indexer.build_dataframe(todo)
        log.info(f"Hashed {len(df_new)} images to permutations")

        # Load existing if present and append for idempotent output
        if cfg.output_path and cfg.output_path.exists():
            try:
                if cfg.output_path.suffix.lower() == ".csv":
                    df_old = pd.read_csv(cfg.output_path)
                else:
                    df_old = pd.read_parquet(cfg.output_path)
                # De-dup on image_name in case of reprocessing
                df_all = pd.concat([df_old, df_new], ignore_index=True)
                df_all = df_all.sort_values("image_name").drop_duplicates(subset=["image_name"], keep="last")
            except Exception as e:
                log.warning(f"Failed to load existing index at {cfg.output_path}: {e}")
                df_all = df_new
        else:
            df_all = df_new

        # Output routing (local only)
        if not cfg.output_path:
            raise ValueError("output_path must be set for local outputs")

        if cfg.output_target == "local_parquet":
            out = cfg.output_path.with_suffix(".parquet")
        else:
            out = cfg.output_path.with_suffix(".csv")
        write_dataframe(df_all, out)
        log.info(f"Wrote index to {out}")

        # Update state
        for o in todo:
            state.mark_processed(o.key, o.etag)
        state.save(cfg.state_path)
        log.info(f"Updated state at {cfg.state_path}")
        
    except Exception as e:
        log.error(f"Failed to build hash index: {e}")
        raise


if __name__ == "__main__":
    # Ensure logging is set up at script level
    from src.utils.logger import setup_logging
    setup_logging("INFO")
    
    import argparse
    p = argparse.ArgumentParser(description="Build or update hash index from S3 using YAML config")
    p.add_argument("--config", type=str, required=False, default=str(Path("configs")/"main_config.yaml"))
    args = p.parse_args()
    cfg = load_config_yaml(Path(args.config))
    run_from_config(cfg)


