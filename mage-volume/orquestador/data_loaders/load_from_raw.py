from os import path
import pandas as pd
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.postgres import Postgres
from mage_ai.settings.repo import get_repo_path

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data(*args, **kwargs) -> dict:
    config_path    = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    queries = {
        'vendor_ids':      'SELECT DISTINCT vendorid       AS vendor_id       FROM raw.taxi_trips_ny WHERE vendorid IS NOT NULL',
        'payment_types':   'SELECT DISTINCT payment_type                       FROM raw.taxi_trips_ny WHERE payment_type IS NOT NULL',
        'pu_location_ids': 'SELECT DISTINCT pulocationid   AS pu_location_id  FROM raw.taxi_trips_ny WHERE pulocationid IS NOT NULL',
        'do_location_ids': 'SELECT DISTINCT dolocationid   AS do_location_id  FROM raw.taxi_trips_ny WHERE dolocationid IS NOT NULL',
        'pickup_hours':    "SELECT DISTINCT date_trunc('hour', tpep_pickup_datetime) AS pickup_datetime FROM raw.taxi_trips_ny WHERE tpep_pickup_datetime IS NOT NULL",
    }

    result = {}
    with Postgres.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
        for key, sql in queries.items():
            result[key] = loader.load(sql)
            print(f"  {key}: {len(result[key]):,} rows")

    print("Raw data loaded from Postgres.")
    return result


@test
def test_output(output, *args) -> None:
    assert isinstance(output, dict), 'Output must be a dict'
    expected_keys = ['vendor_ids', 'payment_types', 'pu_location_ids', 'do_location_ids', 'pickup_hours']
    for key in expected_keys:
        assert key in output, f'Missing key: {key}'
        assert len(output[key]) > 0, f'{key} is empty'
    print("load_from_raw output verified.")
