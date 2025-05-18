import polars as pl


def add_max_forward_return(df) -> pl.DataFrame:
    """
    Add max possible forward return percent.
    Zero if no possible forward return.
    """
    df = df.with_columns(
        # Max value in the rest of the array
        pl.col('close').reverse().rolling_max(df.height, min_samples=1).reverse().shift(-1).over('symbol').alias('next_max')
    ).with_columns(
        # Max possible return
        ( (pl.col('next_max') - pl.col('close')) / pl.col('close') ).over('symbol').clip(lower_bound=0).alias('max_forward_return')
    )

    return df

def load_spy_sample(months: list[int] = [1]) -> pl.DataFrame:
    df = load_spy()
    df = df.filter(pl.col("month").is_in(months))
    return df

def load_spy() -> pl.DataFrame:
    df = pl.read_parquet("./data/spy_2024.parquet")
    df = add_max_forward_return(df)
    cols = ['symbol', 'date', 'month', 'option_type','close', 'minute_index', 'close_moneyness', 'max_forward_return']
    df = df[cols]
    df = df.drop_nulls(subset='max_forward_return')
    return df

def load_bike() -> pl.DataFrame:
    bike_data = pl.read_csv("./data/bike.csv")
    bike_data = bike_data.select('cnt', 'season','holiday', 'workday', 'weather', 'temp', 'hum', 'windspeed', 'cnt_2d_bfr')
    return bike_data


def load_penguins() -> pl.DataFrame:
    penguins = pl.read_csv(
        "./data/penguins.csv",
        null_values="NA",
        schema_overrides={
            "bill_length_mm": pl.Float64,
            "bill_depth_mm": pl.Float64,
            "flipper_length_mm": pl.Float64,
            "body_mass_g": pl.Float64,
        },
    )

    penguins = penguins.drop('rowid', 'year')
    return penguins
