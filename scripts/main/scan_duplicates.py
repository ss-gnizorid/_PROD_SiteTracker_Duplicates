from pathlib import Path

import pandas as pd

from src.tools.duplicate_scanner import DuplicateScanner


def load_df(path: Path) -> pd.DataFrame:
    if path.suffix.lower() == ".csv":
        return pd.read_csv(path)
    return pd.read_parquet(path)


def save_df(df: pd.DataFrame, path: Path) -> None:
    if path.suffix.lower() == ".csv":
        df.to_csv(path, index=False)
    else:
        df.to_parquet(path, index=False)


def run_from_paths(index_path: Path, output_path: Path | None, threshold: int, bands: int) -> None:
    df = load_df(index_path)
    scanner = DuplicateScanner(distance_threshold=threshold, num_bands=bands)
    matches = scanner.find_duplicates(df)
    if matches.empty:
        print("No duplicates found.")
    else:
        print(matches.head(50).to_string(index=False))
        if output_path:
            save_df(matches, output_path)
            print(f"Wrote matches to {output_path}")


if __name__ == "__main__":
    # For direct invocation only; main entrypoint is orchestrated via YAML in main.py
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--index", type=str, required=True)
    p.add_argument("--output", type=str, required=False)
    p.add_argument("--threshold", type=int, default=5)
    p.add_argument("--bands", type=int, default=8)
    a = p.parse_args()
    run_from_paths(Path(a.index), Path(a.output) if a.output else None, a.threshold, a.bands)


