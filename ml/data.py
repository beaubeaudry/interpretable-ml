import polars as pl

def add_last_close_pct(df) -> pl.DataFrame:
    """Difference between close now and close at end of series as a fractional percent."""
    df = df.with_columns(
        ((pl.col("close").last() - pl.col("close"))/pl.col("close")).over('symbol').alias("last_close_pct")
    )
    return df

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
    df = df.filter(pl.col("date").dt.month().is_in(months))
    return df

def load_spy() -> pl.DataFrame:
    df = pl.read_parquet("./data/spy_2024.parquet")
    #df = add_max_forward_return(df)
    cols = ['symbol', 'date', 'option_type', 'strike', 'close', 'minute_index', 'moneyness', 'close_underlying']
    df = df[cols]
    df = df.sort('date', 'minute_index')
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


def load_vix() -> pl.DataFrame:
    vix = pl.read_csv("./data/VIX_History.csv")
    vix = vix.with_columns(pl.col("DATE").str.strptime(pl.Date, "%m/%d/%Y").alias("DATE"))
    vix = vix.rename({col: col.lower() for col in vix.columns})
    vix = vix.filter(pl.col("date").dt.year() >= 2023)

    vix = vix.with_columns([
        pl.col("high").shift(1).alias("prev_high"),
        pl.col("low").shift(1).alias("prev_low"),
        pl.col("close").shift(1).alias("prev_close")
    ]).drop('high', 'low', 'close')

    vix = vix.rename({col: f"vix_{col}" for col in vix.columns if col != 'date'})

    return vix