from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.postgres import Postgres
from os import path
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
        _zone            TEXT
    )""",

    """CREATE TABLE clean.dim_dropoff_location (
        do_location_key  INTEGER PRIMARY KEY,
        location_id      INTEGER NOT NULL,
        _zone            TEXT
    )""",

    """CREATE TABLE clean.dim_datetime (
        datetime_key   INTEGER   PRIMARY KEY,
        datetime_hour  TIMESTAMP,
        _year          INTEGER,
        _month         INTEGER,
        _day           INTEGER,
        _hour          INTEGER,
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

    "CREATE INDEX idx_fact_vendor    ON clean.fact_trips (vendor_key)",
    "CREATE INDEX idx_fact_payment   ON clean.fact_trips (payment_type_key)",
    "CREATE INDEX idx_fact_pu_loc    ON clean.fact_trips (pu_location_key)",
    "CREATE INDEX idx_fact_do_loc    ON clean.fact_trips (do_location_key)",
    "CREATE INDEX idx_fact_datetime  ON clean.fact_trips (datetime_key)",
    "CREATE INDEX idx_fact_pickup_dt ON clean.fact_trips (pickup_datetime)",
]

UNKNOWN_ROWS = [
    "INSERT INTO clean.dim_vendor           VALUES (-1, 'Unknown')",
    "INSERT INTO clean.dim_payment_type     VALUES (-1, 'Unknown')",
    "INSERT INTO clean.dim_pickup_location  VALUES (-1, -1, 'Unknown')",
    "INSERT INTO clean.dim_dropoff_location VALUES (-1, -1, 'Unknown')",
    "INSERT INTO clean.dim_datetime         VALUES (-1, NULL, NULL, NULL, NULL, NULL, 'Unknown', FALSE)",
]

LOAD_ORDER = [
    'dim_vendor',
    'dim_payment_type',
    'dim_pickup_location',
    'dim_dropoff_location',
    'dim_datetime',
    'fact_trips',
]


@data_exporter
def export_data_to_postgres(tables: dict, **kwargs) -> None:
    config_path    = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    # Print actual columns so we can verify they match the table definitions
    print("── DataFrame columns from transformer:")
    for name in LOAD_ORDER:
        print(f"  {name}: {list(tables[name].columns)}")

    with Postgres.with_config(ConfigFileLoader(config_path, config_profile)) as loader:

        # ── Step 1: Drop old schema and recreate fresh ─────────────────
        print("\n── Creating schema and tables...")
        for sql in CREATE_STATEMENTS:
            loader.execute(sql)
            loader.conn.commit()
            print(f"  ✓ {sql.strip().splitlines()[0][:60]}")

        # ── Step 2: Insert unknown rows (-1) into every dimension ───────
        print("\n── Inserting unknown rows (-1)...")
        for sql in UNKNOWN_ROWS:
            loader.execute(sql)
            loader.conn.commit()
            print(f"  ✓ {sql.strip()[:60]}")

        # ── Step 3: Insert data chunk by chunk ──────────────────────────
        print("\n── Loading data...")
        for table_name in LOAD_ORDER:
            df         = tables[table_name]
            total_rows = len(df)
            n_chunks   = math.ceil(total_rows / CHUNK_SIZE)
            print(f"\n  clean.{table_name} → {total_rows:,} rows / {n_chunks} chunk(s)")

            for i in range(n_chunks):
                start = i * CHUNK_SIZE
                end   = min(start + CHUNK_SIZE, total_rows)
                chunk = df.iloc[start:end]

                loader.export(
                    chunk,
                    'clean',
                    table_name,
                    index=False,
                    if_exists='append',
                )
                print(f"    chunk {i+1}/{n_chunks} rows {start:,}–{end:,} ✓")

    print("\n✅ Star schema loaded with PKs, FKs and indexes")