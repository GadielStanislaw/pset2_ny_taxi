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
    schema_name    = 'raw'
    table_name     = 'taxi_trips_ny'
    config_path    = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    query = f"SELECT * FROM {schema_name}.{table_name} LIMIT 100"

    with Postgres.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
        df = loader.load(query)

    print(f"Loaded {len(df):,} rows from {schema_name}.{table_name}")
    return df


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
    assert len(output) > 0,    'The dataframe is empty'
