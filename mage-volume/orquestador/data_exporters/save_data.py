from pandas import DataFrame
from os import path

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_data(df: DataFrame, **kwargs) -> None:
    year       = df['year'].values[0]
    total_rows = df['total_rows'].values[0]

    print(f"✅ Raw ingestion pipeline complete")
    print(f"   Year       : {year}")
    print(f"   Total rows : {total_rows:,}")
    print(f"   Table      : raw.taxi_trips_ny")
