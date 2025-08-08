from pathlib import Path
from typing import Optional

import pandas as pd


def write_dataframe(df: pd.DataFrame, output: Path) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    if output.suffix.lower() == ".csv":
        df.to_csv(output, index=False)
    elif output.suffix.lower() in {".parquet", ".pq"}:
        df.to_parquet(output, index=False)
    else:
        # default to CSV
        df.to_csv(output.with_suffix(".csv"), index=False)


