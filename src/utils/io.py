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


def append_dataframe(df: pd.DataFrame, output: Path) -> None:
    """
    Append a dataframe to a CSV file, creating it with a header if it doesn't exist.

    Notes:
    - Only CSV append is supported. For parquet use write_dataframe with a full merge strategy.
    """
    output.parent.mkdir(parents=True, exist_ok=True)
    if output.suffix.lower() != ".csv":
        raise ValueError("append_dataframe only supports CSV outputs")

    header = not output.exists()
    # Use mode 'a' to append; write header only if file does not yet exist
    df.to_csv(output, mode="a", index=False, header=header)


