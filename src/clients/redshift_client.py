from typing import Optional

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine


class RedshiftClient:
    def __init__(self, connection_url: str):
        self.engine: Engine = create_engine(connection_url, pool_pre_ping=True)

    def ensure_table(self, schema: Optional[str], table: str, sample_df: pd.DataFrame) -> None:
        # Create table if not exists with all columns as VARCHAR(256) for hashes, TEXT for URLs
        cols = []
        for col in sample_df.columns:
            if col in ("job_url",):
                cols.append(f'"{col}" TEXT')
            else:
                cols.append(f'"{col}" VARCHAR(256)')
        sch = f'"{schema}".' if schema else ""
        create_sql = f"CREATE TABLE IF NOT EXISTS {sch}""{table}"" (" + ", ".join(cols) + ")"
        with self.engine.begin() as conn:
            conn.execute(text(create_sql))

    def upsert_dataframe(self, schema: Optional[str], table: str, df: pd.DataFrame) -> None:
        # Simple upsert by deleting existing keys and then inserting
        if df.empty:
            return
        sch = f'"{schema}".' if schema else ""
        key_col = "image_name"
        image_names = df[key_col].astype(str).tolist()
        with self.engine.begin() as conn:
            # Delete existing
            param_names = ",".join([f":p{i}" for i in range(len(image_names))])
            del_sql = text(f"DELETE FROM {sch}""{table}"" WHERE "{key_col}" IN ({param_names})")
            conn.execute(del_sql, {f"p{i}": v for i, v in enumerate(image_names)})
            # Insert via pandas
            df.to_sql(table, con=conn.connection, schema=schema, if_exists="append", index=False, method="multi")


