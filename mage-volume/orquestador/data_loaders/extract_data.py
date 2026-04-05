import pandas as pd
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month:02d}.parquet"
YEAR   = 2025
MONTHS = range(1, 13)

@data_loader
def load_data(*args, **kwargs):
    frames = []
    for month in MONTHS:
        url = BASE_URL.format(year=YEAR, month=month)
        print(f"\n? Month {month:02d}/{YEAR} ? downloading...")
        try:
            df = pd.read_parquet(url)
            df.columns = df.columns.str.lower().str.strip()
            df['_month_key'] = f'month_{month:02d}'   # ? preserve grouping
            frames.append(df)
            print(f"   {len(df):,} rows  {len(df.columns)} columns  ?")
        except Exception as e:
            print(f"   ? Month {month:02d} failed: {e} ? skipping")

    if not frames:
        raise ValueError("No months were downloaded successfully.")

    result = pd.concat(frames, ignore_index=True)
    print(f"\n? {len(frames)}/12 months downloaded ? {len(result):,} total rows")
    return result

@test
def test_output(output, *args) -> None:
    assert isinstance(output, pd.DataFrame), 'Output must be a DataFrame'
    assert len(output) > 0,                  'Downloaded data is empty'
    assert '_month_key' in output.columns,   'Missing _month_key column'
    print(f"? {output['_month_key'].nunique()} months / {len(output):,} rows")