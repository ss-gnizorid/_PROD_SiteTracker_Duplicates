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
from src.utils.io import write_dataframe, append_dataframe
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

        # Resolve output path now and enforce CSV for append mode
        if not cfg.output_path:
            raise ValueError("output_path must be set for local outputs")
        out = (cfg.output_path.with_suffix(".parquet") if cfg.output_target == "local_parquet" else cfg.output_path.with_suffix(".csv"))
        if out.suffix.lower() != ".csv":
            raise ValueError("Batch append mode requires CSV output. Set output.target to 'local_csv'.")

        # Process in batches and checkpoint state after each batch
        batch_size = 1000
        total = len(todo)
        processed_total = 0
        log.info(f"Starting hashing in batches of {batch_size} with max_workers={cfg.max_workers} mode={cfg.hash_mode}")

        total_success = 0
        total_failed = 0
        for start in range(0, total, batch_size):
            end = min(start + batch_size, total)
            batch = todo[start:end]
            log.info(f"Processing batch {start//batch_size + 1} ({start+1}-{end} of {total})")

            # Build dataframe for this batch
            df_batch = indexer.build_dataframe(batch)
            batch_success = len(df_batch)
            failures = indexer.drain_failures()
            batch_failed = len(failures)
            total_success += batch_success
            total_failed += batch_failed
            log.info(f"Batch completed: success={batch_success}, failed={batch_failed}")

            # Append to CSV immediately
            if not df_batch.empty:
                append_dataframe(df_batch, out)
                processed_total += len(df_batch)

            # Append failures to a sidecar CSV
            if failures:
                from pandas import DataFrame
                err_out = out.with_name(out.stem + "_errors").with_suffix(".csv")
                append_dataframe(DataFrame(failures), err_out)

            # Update incremental state only for successfully processed keys
            key_to_etag = {o.key: o.etag for o in batch}
            for key in df_batch.get("image_name", []):
                etag = key_to_etag.get(key)
                if etag:
                    state.mark_processed(key, etag)
            state.save(cfg.state_path)
            log.info(f"Checkpoint saved to {cfg.state_path} (processed so far: {processed_total}/{total})")

        # Final deduplication pass to ensure unique image_name rows (handles any restarts)
        try:
            df_all = pd.read_csv(out)
            before = len(df_all)
            df_all = df_all.sort_values("image_name").drop_duplicates(subset=["image_name"], keep="last")
            after = len(df_all)
            if after != before:
                write_dataframe(df_all, out)
                log.info(f"Final dedup pass removed {before - after} duplicate rows. Wrote consolidated file to {out}")
            else:
                log.info("Final dedup pass found no duplicates")
        except Exception as e:
            log.warning(f"Final dedup skipped due to error reading {out}: {e}")

        log.info(f"Completed hashing. success={total_success}, failed={total_failed}, written={processed_total}. Output: {out}")
        
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


