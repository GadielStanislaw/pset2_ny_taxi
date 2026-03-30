from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.postgres import Postgres
from pandas import DataFrame
from os import path
import math

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_data_to_postgres(df: DataFrame, **kwargs) -> None:
    schema_name   = 'raw'
    table_name    = 'taxi_trips_ny'
    config_path   = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'
 
    CHUNK_SIZE = 50_000          # filas por chunk — ajusta según RAM disponible
    total_rows = len(df)
    n_chunks   = math.ceil(total_rows / CHUNK_SIZE)
 
    print(f"Total filas: {total_rows:,} | Chunk size: {CHUNK_SIZE:,} | Chunks: {n_chunks}")
 
    with Postgres.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
        for i in range(n_chunks):
            start = i * CHUNK_SIZE
            end   = min(start + CHUNK_SIZE, total_rows)
            chunk = df.iloc[start:end]
 
            # Solo el primer chunk hace replace; el resto hace append
            policy = 'replace' if i == 0 else 'append'
 
            loader.export(
                chunk,
                schema_name,
                table_name,
                index=False,
                if_exists=policy,
            )
 
            print(f"  ✓ Chunk {i + 1}/{n_chunks} → filas {start:,}–{end:,} ({policy})")
 
    print(f"\n✅ Carga completada: {total_rows:,} filas en {n_chunks} chunks → {schema_name}.{table_name}")
