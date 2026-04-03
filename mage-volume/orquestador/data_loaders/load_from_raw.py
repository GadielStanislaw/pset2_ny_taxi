from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.postgres import Postgres
from os import path

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data(*args, **kwargs):
    config_path    = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    queries = {
        'vendor_ids': """
            SELECT DISTINCT CAST(vendorid AS INTEGER) AS vendor_id
            FROM raw.taxi_trips_ny
            WHERE vendorid IS NOT NULL
        """,
        'payment_types': """
            SELECT DISTINCT CAST(payment_type AS INTEGER) AS payment_type
            FROM raw.taxi_trips_ny
            WHERE payment_type IS NOT NULL
        """,
        'pu_location_ids': """
            SELECT DISTINCT CAST(pulocationid AS INTEGER) AS pu_location_id
            FROM raw.taxi_trips_ny
            WHERE pulocationid IS NOT NULL
        """,
        'do_location_ids': """
            SELECT DISTINCT CAST(dolocationid AS INTEGER) AS do_location_id
            FROM raw.taxi_trips_ny
            WHERE dolocationid IS NOT NULL
        """,
        'pickup_hours': """
            SELECT DISTINCT DATE_TRUNC('hour', tpep_pickup_datetime)::TIMESTAMP AS pickup_datetime
            FROM raw.taxi_trips_ny
            WHERE tpep_pickup_datetime IS NOT NULL
              AND tpep_pickup_datetime >= '2015-01-01'
              AND tpep_pickup_datetime <= '2025-12-31'
            ORDER BY 1
        """,
    }

    result = {}
    with Postgres.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
        for name, sql in queries.items():
            df = loader.load(sql)
            result[name] = df
            print(f"  {name:20s}: {len(df):>6,} rows")

    print("\nDimension candidate data loaded.")
    return result


@test
def test_output(output, *args) -> None:
    assert isinstance(output, dict), 'Output must be a dict'
    for key in ['vendor_ids', 'payment_types', 'pu_location_ids',
                'do_location_ids', 'pickup_hours']:
        assert key in output, f'Missing key: {key}'
        assert len(output[key]) > 0, f'{key} is empty'
    print("Dimension candidate data loaded ✓")