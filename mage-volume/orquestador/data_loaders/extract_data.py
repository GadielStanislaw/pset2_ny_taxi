if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

import pandas as pd
from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.postgres import Postgres
from os import path
import math

BASE_URL   = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month:02d}.parquet"
YEAR       = 2025
MONTHS     = range(1, 13)
CHUNK_SIZE = 50_000


@data_loader
def load_data(*args, **kwargs):
    config_path    = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    total_inserted = 0
    first_chunk    = True

    with Postgres.with_config(ConfigFileLoader(config_path, config_profile)) as loader:

        loader.execute("CREATE SCHEMA IF NOT EXISTS raw")
        loader.conn.commit()

        for month in MONTHS:
            url = BASE_URL.format(year=YEAR, month=month)
            print(f"\n── Month {month:02d}/{YEAR} → downloading...")

            try:
                df = pd.read_parquet(url)
                df.columns = df.columns.str.lower().str.strip()
                n_rows   = len(df)
                n_chunks = math.ceil(n_rows / CHUNK_SIZE)
                print(f"   {n_rows:,} rows / {n_chunks} chunks")

                for i in range(n_chunks):
                    start  = i * CHUNK_SIZE
                    end    = min(start + CHUNK_SIZE, n_rows)
                    chunk  = df.iloc[start:end]
                    policy = 'replace' if first_chunk else 'append'

                    loader.export(
                        chunk,
                        'raw',
                        'taxi_trips_ny',
                        index=False,
                        if_exists=policy,
                    )
                    print(f"   chunk {i+1}/{n_chunks} rows {start:,}–{end:,} ({policy}) ✓")
                    first_chunk = False

                total_inserted += n_rows
                del df
                print(f"   memory freed ✓")

            except Exception as e:
                print(f"   ✗ Month {month:02d} failed: {e} — skipping")
                continue

    print(f"\n✅ Done — {total_inserted:,} total rows inserted into raw.taxi_trips_ny")

    # Return a small summary DataFrame so save_data receives a valid DataFrame
    return pd.DataFrame([{"year": YEAR, "total_rows": total_inserted}])


@test
def test_output(output, *args) -> None:
    assert output is not None,  'Output is undefined'
    assert len(output) > 0,     'Output is empty'
    print(f"✅ Test passed — {output['total_rows'].values[0]:,} rows inserted")