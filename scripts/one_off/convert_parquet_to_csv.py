from pathlib import Path
import argparse
import sys

import pandas as pd


def main() -> None:
    p = argparse.ArgumentParser(description="Convert a Parquet file to CSV")
    p.add_argument("--in", dest="inp", required=True, help="Input Parquet file path")
    p.add_argument("--out", dest="out", required=True, help="Output CSV file path")
    args = p.parse_args()

    inp = Path(args.inp)
    out = Path(args.out)

    if not inp.exists():
        print(f"Input parquet not found: {inp}")
        sys.exit(1)

    print(f"Reading parquet: {inp}")
    df = pd.read_parquet(inp)
    out.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out, index=False)
    print(f"Wrote CSV: {out} (rows={len(df)})")


if __name__ == "__main__":
    main()


