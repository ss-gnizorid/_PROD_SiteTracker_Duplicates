from pathlib import Path

import pandas as pd

from src.clients.aws_client import S3Client
from src.clients.fabric_client import FabricClient
from src.clients.redshift_client import RedshiftClient
from src.config.config import AppConfig
from src.tools.hash_indexer import HashIndexer
from src.tools.permutation_generator import PermutationConfig, PermutationGenerator
from src.utils.io import write_dataframe
from src.utils.state import IncrementalState
from src.utils.logger import get_logger


def run_from_config(cfg: AppConfig) -> None:
    log = get_logger("build_hash_index")
    s3 = S3Client(
        region_name=cfg.aws_region,
        profile_name=cfg.aws_profile,
        assume_role_arn=cfg.aws_assume_role_arn,
        external_id=cfg.aws_external_id,
    )

    # Incremental state
    state = IncrementalState.load(cfg.state_path)

    # List objects and filter by state
    all_objs = s3.list_s3_images_with_metadata(cfg.s3_bucket, cfg.s3_root_prefix)
    log.info(f"Discovered {len(all_objs)} image candidates under {cfg.s3_bucket}/{cfg.s3_root_prefix}")
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

    # Output routing
    if cfg.output_target == "local_csv":
        if not cfg.output_path:
            raise ValueError("output_path must be set for local outputs")
        out = cfg.output_path.with_suffix(".csv")
        write_dataframe(df_all, out)
        log.info(f"Wrote index to {out}")
    elif cfg.output_target == "local_parquet":
        if not cfg.output_path:
            raise ValueError("output_path must be set for local outputs")
        out = cfg.output_path.with_suffix(".parquet")
        write_dataframe(df_all, out)
        log.info(f"Wrote index to {out}")
    elif cfg.output_target == "microsoft_fabric":
        if not cfg.output_path:
            raise ValueError("output_path must be set for Microsoft Fabric delivery")
        FabricClient(cfg.output_path).write(df_all)
        log.info(f"Delivered index to Fabric landing at {cfg.output_path}")
    elif cfg.output_target == "aws_redshift":
        # Expect connection URL and table info via env vars for flexibility
        import os

        conn_url = os.getenv("REDSHIFT_URL")
        schema = os.getenv("REDSHIFT_SCHEMA") or None
        table = os.getenv("REDSHIFT_TABLE") or "image_hash_index"
        if not conn_url:
            raise ValueError("REDSHIFT_URL environment variable must be set for Redshift output")
        rs = RedshiftClient(conn_url)
        rs.ensure_table(schema, table, df_all.head(1) if not df_all.empty else df_all)
        rs.upsert_dataframe(schema, table, df_all)
        log.info(f"Upserted {len(df_all)} rows to Redshift {schema or 'public'}.{table}")

    # Update state
    for o in todo:
        state.mark_processed(o.key, o.etag)
    state.save(cfg.state_path)
    log.info(f"Updated state at {cfg.state_path}")


if __name__ == "__main__":
    run(parse_args())


