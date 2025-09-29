from dataclasses import dataclass
from pathlib import Path
from typing import Literal, Optional

import yaml


OutputTarget = Literal["local_csv", "local_parquet"]


@dataclass
class AppConfig:
    aws_region: Optional[str]
    aws_profile: Optional[str]
    s3_bucket: str
    s3_root_prefix: str
    hash_mode: Literal["basic", "advanced"]
    resize_width: int
    resize_height: int
    max_workers: int
    output_target: OutputTarget
    output_path: Optional[Path]
    state_path: Path
    aws_assume_role_arn: Optional[str] = None
    aws_external_id: Optional[str] = None
    # S3 performance settings
    s3_max_jobs_to_process: Optional[int] = None
    s3_timeout_seconds: int = 300
    s3_max_pool_connections: int = 64
    # Links generation settings
    links_enabled: bool = False
    links_expiry_days: int = 7
    links_output_target: OutputTarget = "local_parquet"
    links_output_path: Optional[Path] = None
    links_state_path: Path = Path("outputs/state/links.json")
    links_workers: int = 8


def default_config() -> AppConfig:
    return AppConfig(
        aws_region=None,
        aws_profile=None,
        s3_bucket="YOUR_BUCKET",
        s3_root_prefix="images/",
        hash_mode="basic",
        resize_width=256,
        resize_height=256,
        max_workers=8,
        output_target="local_csv",
        output_path=None,
        state_path=Path("outputs/state/seen.json"),
    )


def load_config_yaml(path: Path) -> AppConfig:
    with path.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f)

    # Core
    aws_region = data.get("aws", {}).get("region")
    aws_profile = data.get("aws", {}).get("profile")
    aws_assume_role_arn = data.get("aws", {}).get("assume_role_arn")
    aws_external_id = data.get("aws", {}).get("external_id")
    s3 = data.get("s3", {})
    hashing = data.get("hashing", {})
    output = data.get("output", {})
    links = data.get("links", {})
    state = data.get("state", {})

    cfg = AppConfig(
        aws_region=aws_region,
        aws_profile=aws_profile,
        aws_assume_role_arn=aws_assume_role_arn,
        aws_external_id=aws_external_id,
        s3_bucket=s3["bucket"],
        s3_root_prefix=s3.get("root_prefix", ""),
        s3_max_jobs_to_process=s3.get("max_jobs_to_process"),
        s3_timeout_seconds=int(s3.get("timeout_seconds", 300)),
        s3_max_pool_connections=int(s3.get("max_pool_connections", 64)),
        hash_mode=hashing.get("mode", "basic"),
        resize_width=int(hashing.get("resize", {}).get("width", 256)),
        resize_height=int(hashing.get("resize", {}).get("height", 256)),
        max_workers=int(hashing.get("workers", 8)),
        output_target=output.get("target", "local_csv"),
        output_path=Path(output["path"]) if output.get("path") else None,
        state_path=Path(state.get("path", "outputs/state/seen.json")),
        # Links
        links_enabled=bool(links.get("enabled", False)),
        links_expiry_days=int(links.get("expiry_days", 7)),
        links_output_target=links.get("output", {}).get("target", "local_parquet"),
        links_output_path=Path(links.get("output", {}).get("path")) if links.get("output", {}).get("path") else None,
        links_state_path=Path(links.get("state_path", "outputs/state/links.json")),
        links_workers=int(links.get("workers", 8)),
    )
    return cfg


