from pathlib import Path

import pandas as pd


class FabricClient:
    """
    Placeholder client to deliver parquet files to a configured landing path that Fabric can ingest.
    In many setups, Microsoft Fabric can read parquet from OneLake or a mounted path. Here we simply
    write parquet locally to a specified path, leaving integration specifics for ops/config.
    """

    def __init__(self, landing_path: Path):
        self.landing_path = landing_path
        self.landing_path.parent.mkdir(parents=True, exist_ok=True)

    def write(self, df: pd.DataFrame) -> None:
        if self.landing_path.suffix.lower() != ".parquet":
            path = self.landing_path.with_suffix(".parquet")
        else:
            path = self.landing_path
        df.to_parquet(path, index=False)


