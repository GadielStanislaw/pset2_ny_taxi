from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.postgres import Postgres
from os import path
import math
import pandas as pd

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
        trip_id            SERIAL           PRIMARY KEY,
        vendor_key         INTEGER,
        payment_type_key   INTEGER,
        pu_location_key    INTEGER,
        do_location_key    INTEGER,
        datetime_key       INTEGER,
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


FACT_INSERT_SQL = """
INSERT INTO clean.fact_trips (
    vendor_key,
    payment_type_key,
    pu_location_key,
    do_location_key,
    datetime_key,
    pickup_datetime,
    dropoff_datetime,
    passenger_count,
    trip_distance,
    trip_duration_min,
    fare_amount,
    tip_amount,
    tolls_amount,
    total_amount
)
SELECT
    COALESCE(CAST(vendorid     AS INTEGER), -1)                                              AS vendor_key,
    COALESCE(CAST(payment_type AS INTEGER), -1)                                              AS payment_type_key,
    COALESCE(CAST(pulocationid AS INTEGER), -1)                                              AS pu_location_key,
    COALESCE(CAST(dolocationid AS INTEGER), -1)                                              AS do_location_key,
    COALESCE(d.datetime_key, -1)                                                             AS datetime_key,
    r.tpep_pickup_datetime                                                                   AS pickup_datetime,
    r.tpep_dropoff_datetime                                                                  AS dropoff_datetime,
    GREATEST(COALESCE(CAST(passenger_count AS INTEGER), 1), 1)                              AS passenger_count,
    ROUND(COALESCE(CAST(trip_distance AS DOUBLE PRECISION), 0)::numeric, 2)                 AS trip_distance,
    ROUND((EXTRACT(EPOCH FROM (r.tpep_dropoff_datetime - r.tpep_pickup_datetime)) / 60.0)::numeric, 2)
                                                                                             AS trip_duration_min,
    ROUND(COALESCE(CAST(fare_amount  AS DOUBLE PRECISION), 0)::numeric, 2)                  AS fare_amount,
    ROUND(COALESCE(CAST(tip_amount   AS DOUBLE PRECISION), 0)::numeric, 2)                  AS tip_amount,
    ROUND(COALESCE(CAST(tolls_amount AS DOUBLE PRECISION), 0)::numeric, 2)                  AS tolls_amount,
    ROUND(COALESCE(CAST(total_amount AS DOUBLE PRECISION), 0)::numeric, 2)                  AS total_amount
FROM raw.taxi_trips_ny r
LEFT JOIN clean.dim_datetime d
       ON DATE_TRUNC('hour', r.tpep_pickup_datetime) = d.datetime_hour
WHERE r.tpep_pickup_datetime  IS NOT NULL
  AND r.tpep_dropoff_datetime IS NOT NULL
  AND r.pulocationid          IS NOT NULL
  AND r.dolocationid          IS NOT NULL
  AND r.tpep_pickup_datetime  >= '2025-01-01'
  AND r.tpep_pickup_datetime  <  '2026-01-01'
  AND r.tpep_pickup_datetime  <= r.tpep_dropoff_datetime
  AND EXTRACT(EPOCH FROM (r.tpep_dropoff_datetime - r.tpep_pickup_datetime)) / 60.0
          BETWEEN 0 AND 600
  AND COALESCE(CAST(r.trip_distance AS DOUBLE PRECISION), 0) >= 0
  AND COALESCE(CAST(r.trip_distance AS DOUBLE PRECISION), 0) <  500
  AND COALESCE(CAST(r.fare_amount   AS DOUBLE PRECISION), 0) >= 0
  AND COALESCE(CAST(r.total_amount  AS DOUBLE PRECISION), 0) >= 0
  AND COALESCE(CAST(r.tip_amount    AS DOUBLE PRECISION), 0) >= 0
"""


def _execute(loader, sql):
    """Run a single SQL statement and commit."""
    with loader.conn.cursor() as cur:
        cur.execute(sql)
    loader.conn.commit()


@data_exporter
def export_data_to_postgres(data: dict, **kwargs) -> None:
    config_path    = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    with Postgres.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
        if not hasattr(loader, '_ctx'):
            loader.open()


        print("\n>> Creating schema and tables...")
        for sql in CREATE_STATEMENTS:
            _execute(loader, sql)
            print(f"  OK {sql.strip().splitlines()[0][:70]}")

        print("\n>> Inserting sentinel (-1) rows...")
        for sql in UNKNOWN_ROWS:
            _execute(loader, sql)
            print(f"  OK {sql[:70]}")


        print("\n>> Loading dimension tables...")
        for table_name in DIM_LOAD_ORDER:
            df = data[table_name]
            n_chunks = math.ceil(len(df) / CHUNK_SIZE)
            for i in range(n_chunks):
                chunk = df.iloc[i * CHUNK_SIZE:(i + 1) * CHUNK_SIZE]
                loader.export(
                    chunk,
                    'clean',
                    table_name,
                    index=False,
                    if_exists='append',
                    auto_clean_name=False,
                )
            print(f"  OK clean.{table_name}: {len(df):,} rows")


        print("\n>> Inserting fact_trips (bulk load, indexes added after)...")
        try:
            with loader.conn.cursor() as cur:
                cur.execute(FACT_INSERT_SQL)
            loader.conn.commit()
        except Exception as exc:
            loader.conn.rollback()
            raise RuntimeError(f"fact_trips INSERT failed: {exc}") from exc

        total = loader.load("SELECT COUNT(*) AS n FROM clean.fact_trips")['n'].iloc[0]
        print(f"  OK clean.fact_trips: {int(total):,} rows inserted")


        print("\n>> Creating indexes on fact_trips...")
        for sql in [
            "CREATE INDEX idx_fact_vendor    ON clean.fact_trips (vendor_key)",
            "CREATE INDEX idx_fact_payment   ON clean.fact_trips (payment_type_key)",
            "CREATE INDEX idx_fact_pu_loc    ON clean.fact_trips (pu_location_key)",
            "CREATE INDEX idx_fact_do_loc    ON clean.fact_trips (do_location_key)",
            "CREATE INDEX idx_fact_datetime  ON clean.fact_trips (datetime_key)",
            "CREATE INDEX idx_fact_dt        ON clean.fact_trips (pickup_datetime)",
        ]:
            _execute(loader, sql)
            print(f"  OK {sql}")

    print("\n>> Star schema complete - clean schema is ready")
