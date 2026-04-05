import pandas as pd

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

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


def build_dim_vendor(vendor_ids):
    return pd.DataFrame([
        {'vendor_key': int(vid), 'vendor_name': VENDOR_MAP.get(int(vid), f'Vendor {vid}')}
        for vid in sorted(vendor_ids)
    ])


def build_dim_payment_type(payment_ids):
    return pd.DataFrame([
        {'payment_type_key': int(pid), 'payment_type_name': PAYMENT_MAP.get(int(pid), f'Type {pid}')}
        for pid in sorted(payment_ids)
    ])


def build_dim_location(location_ids, key_col):
    return pd.DataFrame([
        {key_col: int(lid), 'location_id': int(lid), 'zone': f'Zone {lid}'}
        for lid in sorted(location_ids)
    ])


def build_dim_datetime(pickup_hours):
    hours = pickup_hours.drop_duplicates().sort_values().reset_index(drop=True)
    dim = pd.DataFrame({
        'datetime_key':  range(1, len(hours) + 1),
        'datetime_hour': hours.values,
    })
    idx = pd.DatetimeIndex(dim['datetime_hour'])
    dim['year']       = idx.year.astype(int)
    dim['month']      = idx.month.astype(int)
    dim['day']        = idx.day.astype(int)
    dim['hour']       = idx.hour.astype(int)
    dim['weekday']    = idx.day_name()
    dim['is_weekend'] = (idx.dayofweek >= 5)
    return dim


@transformer
def transform(data, *args, **kwargs):
    vendor_ids   = data['vendor_ids']['vendor_id'].dropna().unique().astype(int)
    payment_ids  = data['payment_types']['payment_type'].dropna().unique().astype(int)
    pu_loc_ids   = data['pu_location_ids']['pu_location_id'].dropna().unique().astype(int)
    do_loc_ids   = data['do_location_ids']['do_location_id'].dropna().unique().astype(int)
    pickup_hours = data['pickup_hours']['pickup_datetime'].dropna()

    dim_vendor   = build_dim_vendor(vendor_ids)
    dim_payment  = build_dim_payment_type(payment_ids)
    dim_pickup   = build_dim_location(pu_loc_ids, 'pu_location_key')
    dim_dropoff  = build_dim_location(do_loc_ids, 'do_location_key')
    dim_datetime = build_dim_datetime(pickup_hours)

    print("?? Dimension tables built ??")
    print(f"  dim_vendor           : {len(dim_vendor):>6,} rows  cols={list(dim_vendor.columns)}")
    print(f"  dim_payment_type     : {len(dim_payment):>6,} rows  cols={list(dim_payment.columns)}")
    print(f"  dim_pickup_location  : {len(dim_pickup):>6,} rows  cols={list(dim_pickup.columns)}")
    print(f"  dim_dropoff_location : {len(dim_dropoff):>6,} rows  cols={list(dim_dropoff.columns)}")
    print(f"  dim_datetime         : {len(dim_datetime):>6,} rows  cols={list(dim_datetime.columns)}")

    return {
        'dim_vendor':           dim_vendor,
        'dim_payment_type':     dim_payment,
        'dim_pickup_location':  dim_pickup,
        'dim_dropoff_location': dim_dropoff,
        'dim_datetime':         dim_datetime,
    }


@test
def test_output(output, *args) -> None:
    assert isinstance(output, dict), 'Output must be a dict'
    for t in ['dim_vendor', 'dim_payment_type', 'dim_pickup_location',
              'dim_dropoff_location', 'dim_datetime']:
        assert t in output and len(output[t]) > 0, f'{t} missing or empty'
    assert 'zone' in output['dim_pickup_location'].columns, '"zone" column missing'
    assert 'year' in output['dim_datetime'].columns,        '"year" column missing'
    print("All dimension tables verified.")