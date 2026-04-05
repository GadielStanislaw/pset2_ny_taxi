import math
from os import path
import pandas as pd
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.postgres import Postgres
from mage_ai.settings.repo import get_repo_path
if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

CHUNK_SIZE = 50_000

@data_exporter
def export_data(df: pd.DataFrame, **kwargs) -> None:
    config_path    = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'
    total_exported = 0
    first_month    = True

    with Postgres.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
        loader.execute("CREATE SCHEMA IF NOT EXISTS raw")
        loader.conn.commit()
        print("? Schema raw ready\n")

        for month_key in sorted(df['_month_key'].unique()):
            month_df = df[df['_month_key'] == month_key].drop(columns=['_month_key'])
            n_rows   = len(month_df)
            n_chunks = math.ceil(n_rows / CHUNK_SIZE)
            policy   = 'replace' if first_month else 'append'
            print(f"? {month_key}: {n_rows:,} rows / {n_chunks} chunks  (if_exists='{policy}')")

            for i in range(n_chunks):
                start = i * CHUNK_SIZE
                end   = min(start + CHUNK_SIZE, n_rows)
                loader.export(
                    month_df.iloc[start:end],
                    'raw',
                    'taxi_trips_ny',
                    index=False,
                    if_exists=policy,
                    auto_clean_name=False,
                )
                policy      = 'append'
                first_month = False
                print(f"   chunk {i + 1}/{n_chunks}  rows {start:,}?{end:,}  ?")

            total_exported += n_rows

    print(f"\n? Raw ingestion complete")
    print(f"   Table      : raw.taxi_trips_ny")
    print(f"   Total rows : {total_exported:,}")