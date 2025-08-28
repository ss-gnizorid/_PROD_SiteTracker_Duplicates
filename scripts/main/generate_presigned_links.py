from pathlib import Path
import sys

# Add the project root to Python path for imports
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

import pandas as pd

from src.clients.aws_client import S3Client
from src.config.config import AppConfig, load_config_yaml
from src.tools.link_generator import LinkGenerator
from src.utils.io import write_dataframe
from src.utils.state import LinkState
from src.utils.logger import get_logger, setup_logging


def run_from_config(cfg: AppConfig) -> None:
    log = get_logger("generate_presigned_links")

    if not cfg.links_enabled:
        log.info("Links generation disabled via config. Skipping.")
        return

    if not cfg.output_path:
        raise FileNotFoundError("output.path is not set in config")

    index_path = cfg.output_path.with_suffix(
        ".parquet" if cfg.output_target == "local_parquet" else ".csv"
    )
    if not index_path.exists():
        raise FileNotFoundError(f"Hash index output not found at {index_path}. Run the hash index build first.")

    # Load hash index to get keys and optionally metadata for join
    log.info(f"Loading hash index from {index_path}")
    if index_path.suffix.lower() == ".csv":
        df_index = pd.read_csv(index_path)
    else:
        df_index = pd.read_parquet(index_path)

    if "image_name" not in df_index.columns:
        raise ValueError("Hash index missing required column 'image_name'")

    keys = df_index["image_name"].dropna().astype(str).tolist()
    log.info(f"Preparing to generate/refresh links for {len(keys)} objects")

    s3 = S3Client(
        region_name=cfg.aws_region,
        profile_name=cfg.aws_profile,
        assume_role_arn=cfg.aws_assume_role_arn,
        external_id=cfg.aws_external_id,
    )

    state = LinkState.load(cfg.links_state_path)
    gen = LinkGenerator(
        s3=s3,
        bucket=cfg.s3_bucket,
        expiry_days=cfg.links_expiry_days,
        max_workers=cfg.links_workers,
    )
    records = gen.generate_links(keys, state)

    # Persist state
    state.save(cfg.links_state_path)
    log.info(f"Saved link state to {cfg.links_state_path}")

    # Build or merge link index output
    df_new = LinkGenerator.to_dataframe(records)
    log.info(f"Generated/updated {len(df_new)} links this run")

    if not cfg.links_output_path:
        log.warning("links.output.path not set; skipping link index write")
        return

    out = cfg.links_output_path.with_suffix(".parquet" if cfg.links_output_target == "local_parquet" else ".csv")

    # Merge with existing file if present, de-dup by image_name
    if out.exists():
        try:
            if out.suffix.lower() == ".csv":
                df_old = pd.read_csv(out)
            else:
                df_old = pd.read_parquet(out)
            df_all = pd.concat([df_old, df_new], ignore_index=True)
            df_all = df_all.sort_values("image_name").drop_duplicates(subset=["image_name"], keep="last")
        except Exception:
            df_all = df_new
    else:
        # If switching formats and no new links, reuse the alternate format file to avoid empty output
        alt = out.with_suffix(".csv" if out.suffix.lower() == ".parquet" else ".parquet")
        if df_new.empty and alt.exists():
            try:
                if alt.suffix.lower() == ".csv":
                    df_all = pd.read_csv(alt)
                else:
                    df_all = pd.read_parquet(alt)
            except Exception:
                df_all = df_new
        else:
            df_all = df_new

    write_dataframe(df_all, out)
    log.info(f"Wrote link index to {out}")


if __name__ == "__main__":
    setup_logging("INFO")
    import argparse
    p = argparse.ArgumentParser(description="Generate or refresh S3 presigned links using YAML config")
    p.add_argument("--config", type=str, required=False, default=str(Path("configs")/"main_config.yaml"))
    args = p.parse_args()
    cfg = load_config_yaml(Path(args.config))
    run_from_config(cfg)


