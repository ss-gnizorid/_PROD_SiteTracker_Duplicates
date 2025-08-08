from dataclasses import dataclass
from pathlib import Path
from typing import Literal, Optional

import yaml


OutputTarget = Literal["local_csv", "local_parquet", "microsoft_fabric", "aws_redshift"]


@dataclass
class AppConfig:
    aws_region: Optional[str]
    aws_profile: Optional[str]
    s3_bucket: str
    s3_root_prefix: str
    hash_mode: Literal["basic", "advanced"]
    resize_width: int
    resize_height: int
    output_target: OutputTarget
    output_path: Optional[Path]
    state_path: Path
    # Duplicate scanning
    duplicate_threshold: int = 5
    duplicate_bands: int = 8


def default_config() -> AppConfig:
    return AppConfig(
        aws_region=None,
        aws_profile=None,
        s3_bucket="YOUR_BUCKET",
        s3_root_prefix="images/",
        hash_mode="basic",
        resize_width=256,
        resize_height=256,
        output_target="local_csv",
        output_path=Path("outputs/hash_index.csv"),
        state_path=Path("outputs/state/seen.json"),
    )


def load_config_yaml(path: Path) -> AppConfig:
    with path.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f)

    # Core
    aws_region = data.get("aws", {}).get("region")
    aws_profile = data.get("aws", {}).get("profile")
    s3 = data.get("s3", {})
    hashing = data.get("hashing", {})
    output = data.get("output", {})
    state = data.get("state", {})
    dup = data.get("duplicates", {})

    cfg = AppConfig(
        aws_region=aws_region,
        aws_profile=aws_profile,
        s3_bucket=s3["bucket"],
        s3_root_prefix=s3.get("root_prefix", ""),
        hash_mode=hashing.get("mode", "basic"),
        resize_width=int(hashing.get("resize", {}).get("width", 256)),
        resize_height=int(hashing.get("resize", {}).get("height", 256)),
        output_target=output.get("target", "local_csv"),
        output_path=Path(output["path"]) if output.get("path") else None,
        state_path=Path(state.get("path", "outputs/state/seen.json")),
        duplicate_threshold=int(dup.get("threshold", 5)),
        duplicate_bands=int(dup.get("bands", 8)),
    )
    return cfg


