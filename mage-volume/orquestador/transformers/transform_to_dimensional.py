import pandas as pd
import numpy as np

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

# ── Static catalogs ───────────────────────────────────────────────────────────
VENDOR_MAP = {
    1: 'Creative Mobile Technologies',
    2: 'VeriFone Inc',
}

PAYMENT_MAP = {
    1: 'Credit card',
    2: 'Cash',
    3: 'No charge',
    4: 'Dispute',
    5: 'Unknown',
    6: 'Voided trip',
}


# ── Step 1: Cleaning ──────────────────────────────────────────────────────────
def clean(df: pd.DataFrame) -> pd.DataFrame:
    original = len(df)

    # Rename columns to snake_case standard names
    df = df.rename(columns={
        'vendorid':              'vendor_id',
        'tpep_pickup_datetime':  'pickup_datetime',
        'tpep_dropoff_datetime': 'dropoff_datetime',
        'pulocationid':          'pu_location_id',
        'dolocationid':          'do_location_id',
        'ratecodeid':            'rate_code_id',
    })

    # Fix types
    df['pickup_datetime']  = pd.to_datetime(df['pickup_datetime'],  errors='coerce')
    df['dropoff_datetime'] = pd.to_datetime(df['dropoff_datetime'], errors='coerce')
    df['passenger_count']  = pd.to_numeric(df['passenger_count'],   errors='coerce')
    df['trip_distance']    = pd.to_numeric(df['trip_distance'],     errors='coerce')
    df['fare_amount']      = pd.to_numeric(df['fare_amount'],       errors='coerce')
    df['tip_amount']       = pd.to_numeric(df['tip_amount'],        errors='coerce')
    df['total_amount']     = pd.to_numeric(df['total_amount'],      errors='coerce')
    df['payment_type']     = pd.to_numeric(df['payment_type'],      errors='coerce')
    df['vendor_id']        = pd.to_numeric(df['vendor_id'],         errors='coerce')
    df['pu_location_id']   = pd.to_numeric(df['pu_location_id'],    errors='coerce')
    df['do_location_id']   = pd.to_numeric(df['do_location_id'],    errors='coerce')

    # Drop rows with null critical columns
    df = df.dropna(subset=['pickup_datetime', 'dropoff_datetime',
                           'pu_location_id',  'do_location_id'])

    # Remove impossible dates
    df = df[
        (df['pickup_datetime']  >= '2015-01-01') &
        (df['dropoff_datetime'] <= '2025-12-31') &
        (df['pickup_datetime']  <= df['dropoff_datetime'])
    ]

    # Calculate trip duration in minutes (derived metric)
    df['trip_duration_min'] = (
        (df['dropoff_datetime'] - df['pickup_datetime'])
        .dt.total_seconds() / 60
    ).round(2)

    # Remove impossible durations and distances
    df = df[
        (df['trip_duration_min']  >  0)   &
        (df['trip_duration_min']  < 600)  &   # less than 10 hours
        (df['trip_distance'].fillna(0) >= 0) &
        (df['trip_distance'].fillna(0) < 500)
    ]

    # Remove negative amounts
    df = df[
        (df['fare_amount'].fillna(0)  >= 0) &
        (df['total_amount'].fillna(0) >= 0) &
        (df['tip_amount'].fillna(0)   >= 0)
    ]

    # Impute passenger_count: null or 0 → 1
    df['passenger_count'] = df['passenger_count'].fillna(1).clip(lower=1).astype(int)

    # Remove exact duplicates
    df = df.drop_duplicates().reset_index(drop=True)

    removed = original - len(df)
    print(f"  Rows removed in cleaning : {removed:,} ({removed/original*100:.1f}%)")
    print(f"  Valid rows remaining     : {len(df):,}")
    return df


# ── Step 2: Dimension tables ──────────────────────────────────────────────────
def build_dim_vendor(df):
    ids = sorted(df['vendor_id'].dropna().unique().astype(int))
    return pd.DataFrame([
        {'vendor_key': vid, 'vendor_name': VENDOR_MAP.get(vid, f'Vendor {vid}')}
        for vid in ids
    ])


def build_dim_payment_type(df):
    ids = sorted(df['payment_type'].dropna().unique().astype(int))
    return pd.DataFrame([
        {'payment_type_key': pid, 'payment_type_name': PAYMENT_MAP.get(pid, f'Type {pid}')}
        for pid in ids
    ])


def build_dim_location(df, col, key_col):
    ids = sorted(df[col].dropna().unique().astype(int))
    return pd.DataFrame([
        {key_col: lid, 'location_id': lid, 'zone': f'Zone {lid}'}
        for lid in ids
    ])


def build_dim_datetime(df):
    # One row per unique hour — avoids millions of rows in the dimension
    hours = (
        df['pickup_datetime']
        .dt.floor('h')
        .drop_duplicates()
        .sort_values()
        .reset_index(drop=True)
    )
    dim = pd.DataFrame({'datetime_key': range(1, len(hours) + 1),
                        'datetime_hour': hours.values})
    idx = pd.DatetimeIndex(dim['datetime_hour'])
    dim['year']       = idx.year
    dim['month']      = idx.month
    dim['day']        = idx.day
    dim['hour']       = idx.hour
    dim['weekday']    = idx.day_name()
    dim['is_weekend'] = idx.dayofweek >= 5
    return dim


# ── Step 3: Fact table ────────────────────────────────────────────────────────
def build_fact_trips(df, dim_datetime):
    # Build a lookup: truncated hour → datetime_key
    dt_lookup = dict(zip(
        dim_datetime['datetime_hour'].astype('datetime64[h]'),
        dim_datetime['datetime_key']
    ))

    fact = pd.DataFrame({
        'trip_id':           range(1, len(df) + 1),
        'vendor_key':        df['vendor_id'].fillna(-1).astype(int).values,
        'payment_type_key':  df['payment_type'].fillna(-1).astype(int).values,
        'pu_location_key':   df['pu_location_id'].fillna(-1).astype(int).values,
        'do_location_key':   df['do_location_id'].fillna(-1).astype(int).values,
        'datetime_key':      df['pickup_datetime']
                               .dt.floor('h')
                               .map(dt_lookup)
                               .fillna(-1)
                               .astype(int)
                               .values,
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
    return fact


# ── Main transformer block ────────────────────────────────────────────────────
@transformer
def transform(data, *args, **kwargs):
    print(f"Input rows from raw: {len(data):,}")

    # 1 — Clean
    df = clean(data)

    # 2 — Build dimensions
    dim_vendor    = build_dim_vendor(df)
    dim_payment   = build_dim_payment_type(df)
    dim_pickup    = build_dim_location(df, 'pu_location_id', 'pu_location_key')
    dim_dropoff   = build_dim_location(df, 'do_location_id', 'do_location_key')
    dim_datetime  = build_dim_datetime(df)

    # 3 — Build fact table
    fact_trips    = build_fact_trips(df, dim_datetime)

    # Print summary
    print(f"\n── Dimensional model summary ──")
    print(f"  dim_vendor           : {len(dim_vendor):>8,} rows")
    print(f"  dim_payment_type     : {len(dim_payment):>8,} rows")
    print(f"  dim_pickup_location  : {len(dim_pickup):>8,} rows")
    print(f"  dim_dropoff_location : {len(dim_dropoff):>8,} rows")
    print(f"  dim_datetime         : {len(dim_datetime):>8,} rows")
    print(f"  fact_trips           : {len(fact_trips):>8,} rows")

    return {
        'dim_vendor':           dim_vendor,
        'dim_payment_type':     dim_payment,
        'dim_pickup_location':  dim_pickup,
        'dim_dropoff_location': dim_dropoff,
        'dim_datetime':         dim_datetime,
        'fact_trips':           fact_trips,
    }


@test
def test_output(output, *args) -> None:
    assert isinstance(output, dict),         'Output must be a dictionary'
    assert 'fact_trips' in output,           'fact_trips is missing'
    assert len(output['fact_trips']) > 0,    'fact_trips is empty'
    assert 'dim_vendor' in output,           'dim_vendor is missing'
    assert 'dim_datetime' in output,         'dim_datetime is missing'
    print("All dimension and fact tables generated ✓")