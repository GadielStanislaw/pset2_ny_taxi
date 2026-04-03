from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.postgres import Postgres
from os import path
import pandas as pd
import math

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

CHUNK_SIZE = 50_000

CREATE_STATEMENTS = [
    "DROP SCHEMA IF EXISTS clean CASCADE",
    "CREATE SCHEMA clean",

    """CREATE TABLE clean.dim_vendor (
        vendor_key   INTEGER PRIMARY KEY,
        vendor_name  TEXT    NOT NULL
    )""",

    """CREATE TABLE clean.dim_payment_type (
        payment_type_key  INTEGER PRIMARY KEY,
        payment_type_name TEXT    NOT NULL
    )""",

    """CREATE TABLE clean.dim_pickup_location (
        pu_location_key  INTEGER PRIMARY KEY,
        location_id      INTEGER NOT NULL,
        zone             TEXT
    )""",

    """CREATE TABLE clean.dim_dropoff_location (
        do_location_key  INTEGER PRIMARY KEY,
        location_id      INTEGER NOT NULL,
        zone             TEXT
    )""",

    """CREATE TABLE clean.dim_datetime (
        datetime_key   INTEGER   PRIMARY KEY,
        datetime_hour  TIMESTAMP,
        year           INTEGER,
        month          INTEGER,
        day            INTEGER,
        hour           INTEGER,
        weekday        TEXT,
        is_weekend     BOOLEAN
    )""",

    """CREATE TABLE clean.fact_trips (
        trip_id            INTEGER PRIMARY KEY,
        vendor_key         INTEGER REFERENCES clean.dim_vendor(vendor_key),
        payment_type_key   INTEGER REFERENCES clean.dim_payment_type(payment_type_key),
        pu_location_key    INTEGER REFERENCES clean.dim_pickup_location(pu_location_key),
        do_location_key    INTEGER REFERENCES clean.dim_dropoff_location(do_location_key),
        datetime_key       INTEGER REFERENCES clean.dim_datetime(datetime_key),
        pickup_datetime    TIMESTAMP,
        dropoff_datetime   TIMESTAMP,
        passenger_count    INTEGER,
        trip_distance      DOUBLE PRECISION,
        trip_duration_min  DOUBLE PRECISION,
        fare_amount        DOUBLE PRECISION,
        tip_amount         DOUBLE PRECISION,
        tolls_amount       DOUBLE PRECISION,
        total_amount       DOUBLE PRECISION
    )""",

    "CREATE INDEX idx_fact_vendor   ON clean.fact_trips (vendor_key)",
    "CREATE INDEX idx_fact_payment  ON clean.fact_trips (payment_type_key)",
    "CREATE INDEX idx_fact_pu_loc   ON clean.fact_trips (pu_location_key)",
    "CREATE INDEX idx_fact_do_loc   ON clean.fact_trips (do_location_key)",
    "CREATE INDEX idx_fact_datetime ON clean.fact_trips (datetime_key)",
    "CREATE INDEX idx_fact_dt       ON clean.fact_trips (pickup_datetime)",
]

UNKNOWN_ROWS = [
    "INSERT INTO clean.dim_vendor           VALUES (-1, 'Unknown')",
    "INSERT INTO clean.dim_payment_type     VALUES (-1, 'Unknown')",
    "INSERT INTO clean.dim_pickup_location  VALUES (-1, -1, 'Unknown')",
    "INSERT INTO clean.dim_dropoff_location VALUES (-1, -1, 'Unknown')",
    "INSERT INTO clean.dim_datetime         VALUES (-1, NULL, NULL, NULL, NULL, NULL, 'Unknown', FALSE)",
]

DIM_LOAD_ORDER = [
    'dim_vendor',
    'dim_payment_type',
    'dim_pickup_location',
    'dim_dropoff_location',
    'dim_datetime',
]


def clean(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(columns={
        'vendorid':              'vendor_id',
        'tpep_pickup_datetime':  'pickup_datetime',
        'tpep_dropoff_datetime': 'dropoff_datetime',
        'pulocationid':          'pu_location_id',
        'dolocationid':          'do_location_id',
        'ratecodeid':            'rate_code_id',
    })
    for col in ['pickup_datetime', 'dropoff_datetime']:
        df[col] = pd.to_datetime(df[col], errors='coerce')
    for col in ['passenger_count', 'trip_distance', 'fare_amount',
                'tip_amount', 'total_amount', 'payment_type',
                'vendor_id', 'pu_location_id', 'do_location_id']:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    if 'tolls_amount' in df.columns:
        df['tolls_amount'] = pd.to_numeric(df['tolls_amount'], errors='coerce')
    else:
        df['tolls_amount'] = 0.0

    df = df.dropna(subset=['pickup_datetime', 'dropoff_datetime',
                           'pu_location_id', 'do_location_id'])
    df = df[
        (df['pickup_datetime']  >= '2015-01-01') &
        (df['dropoff_datetime'] <= '2025-12-31') &
        (df['pickup_datetime']  <= df['dropoff_datetime'])
    ]
    df['trip_duration_min'] = (
        (df['dropoff_datetime'] - df['pickup_datetime']).dt.total_seconds() / 60
    ).round(2)
    df = df[
        (df['trip_duration_min']        >  0)   &
        (df['trip_duration_min']        < 600)  &
        (df['trip_distance'].fillna(0) >= 0)    &
        (df['trip_distance'].fillna(0) < 500)   &
        (df['fare_amount'].fillna(0)   >= 0)    &
        (df['total_amount'].fillna(0)  >= 0)    &
        (df['tip_amount'].fillna(0)    >= 0)
    ]
    df['passenger_count'] = df['passenger_count'].fillna(1).clip(lower=1).astype(int)
    return df.drop_duplicates().reset_index(drop=True)


def build_fact_chunk(df: pd.DataFrame, dt_lookup: dict, trip_id_start: int) -> pd.DataFrame:
    return pd.DataFrame({
        'trip_id':           range(trip_id_start, trip_id_start + len(df)),
        'vendor_key':        df['vendor_id'].fillna(-1).astype(int).values,
        'payment_type_key':  df['payment_type'].fillna(-1).astype(int).values,
        'pu_location_key':   df['pu_location_id'].fillna(-1).astype(int).values,
        'do_location_key':   df['do_location_id'].fillna(-1).astype(int).values,
        'datetime_key':      (df['pickup_datetime']
                               .dt.floor('h')
                               .map(dt_lookup)
                               .fillna(-1)
                               .astype(int)
                               .values),
        'pickup_datetime':   df['pickup_datetime'].values,
        'dropoff_datetime':  df['dropoff_datetime'].values,
        'passenger_count':   df['passenger_count'].astype(int).values,
        'trip_distance':     df['trip_distance'].round(2).values,
        'trip_duration_min': df['trip_duration_min'].values,
        'fare_amount':       df['fare_amount'].round(2).values,
        'tip_amount':        df['tip_amount'].fillna(0).round(2).values,
        'tolls_amount':      df['tolls_amount'].fillna(0).round(2).values,
        'total_amount':      df['total_amount'].round(2).values,
    })


@data_exporter
def export_data_to_postgres(tables: dict, **kwargs) -> None:
    config_path    = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    dt_lookup = dict(zip(
        tables['dim_datetime']['datetime_hour'].astype('datetime64[h]'),
        tables['dim_datetime']['datetime_key'],
    ))
    print(f"datetime lookup built: {len(dt_lookup):,} entries")

    with Postgres.with_config(ConfigFileLoader(config_path, config_profile)) as loader:

        # Step 1: recreate clean schema
        print("\n── Creating schema and tables...")
        for sql in CREATE_STATEMENTS:
            loader.execute(sql)
            loader.conn.commit()
            print(f"  ✓ {sql.strip().splitlines()[0][:70]}")

        # Step 2: insert -1 sentinel rows
        print("\n── Inserting unknown (-1) sentinel rows...")
        for sql in UNKNOWN_ROWS:
            loader.execute(sql)
            loader.conn.commit()
            print(f"  ✓ {sql[:70]}")

        # Step 3: insert dimension tables
        print("\n── Loading dimension tables...")
        for table_name in DIM_LOAD_ORDER:
            df = tables[table_name]
            n_chunks = math.ceil(len(df) / CHUNK_SIZE)
            for i in range(n_chunks):
                chunk = df.iloc[i * CHUNK_SIZE:(i + 1) * CHUNK_SIZE]
                loader.export(
                    chunk,
                    'clean',
                    table_name,
                    index=False,
                    if_exists='append',
                    auto_clean_name=False,      # ← keeps zone, year, month, day, hour as-is
                )
            print(f"  ✓ clean.{table_name}: {len(df):,} rows  cols={list(df.columns)}")

        # Step 4: load fact_trips month by month
        print("\n── Detecting date range in raw.taxi_trips_ny...")
        date_range = loader.load("""
            SELECT
                DATE_TRUNC('month', MIN(tpep_pickup_datetime))::TIMESTAMP AS min_month,
                DATE_TRUNC('month', MAX(tpep_pickup_datetime))::TIMESTAMP AS max_month
            FROM raw.taxi_trips_ny
            WHERE tpep_pickup_datetime IS NOT NULL
        """)
        min_month = pd.Timestamp(date_range['min_month'].iloc[0])
        max_month = pd.Timestamp(date_range['max_month'].iloc[0])
        months    = pd.date_range(start=min_month, end=max_month, freq='MS')
        print(f"  Range: {min_month.strftime('%Y-%m')} → {max_month.strftime('%Y-%m')} ({len(months)} months)")

        trip_id         = 1
        total_fact_rows = 0

        print(f"\n── Loading fact_trips month by month...")
        for month_start in months:
            month_end = month_start + pd.DateOffset(months=1)
            sql = f"""
                SELECT *
                FROM raw.taxi_trips_ny
                WHERE tpep_pickup_datetime >= '{month_start}'
                  AND tpep_pickup_datetime  < '{month_end}'
            """
            df_raw = loader.load(sql)

            if len(df_raw) == 0:
                print(f"  {month_start.strftime('%Y-%m')}: no rows, skipping")
                continue

            df_clean     = clean(df_raw)
            rows_removed = len(df_raw) - len(df_clean)

            if len(df_clean) == 0:
                print(f"  {month_start.strftime('%Y-%m')}: all {len(df_raw):,} removed by cleaning")
                continue

            n_sub = math.ceil(len(df_clean) / CHUNK_SIZE)
            for j in range(n_sub):
                sub        = df_clean.iloc[j * CHUNK_SIZE:(j + 1) * CHUNK_SIZE]
                fact_chunk = build_fact_chunk(sub, dt_lookup, trip_id)
                loader.export(
                    fact_chunk,
                    'clean',
                    'fact_trips',
                    index=False,
                    if_exists='append',
                    auto_clean_name=False,      # ← same fix for fact columns
                )
                trip_id += len(fact_chunk)

            total_fact_rows += len(df_clean)
            print(f"  {month_start.strftime('%Y-%m')}: {len(df_raw):,} raw "
                  f"→ {len(df_clean):,} clean  ({rows_removed:,} removed)  "
                  f"cumulative: {total_fact_rows:,}")

    print(f"\n✅ Star schema complete — {total_fact_rows:,} fact rows, trip_ids 1–{trip_id - 1}")