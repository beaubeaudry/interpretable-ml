import polars as pl
import polars.selectors as cs
from sklearn.model_selection import train_test_split

def unique_dates(df : pl.DataFrame) -> list[int]:
    return df.select(pl.col('date').unique().sort()).to_series().to_list()

def by_date(df: pl.DataFrame, dates : list[int]) -> pl.DataFrame:
    return df.filter(pl.col('date').is_in(dates))

def train_holdout_dates(df: pl.DataFrame, test_size=0.2, seed=1):
    """Split data into train and holdout. Train can be further split."""
    dates_unique = unique_dates(df)
    train_dates, holdout_dates = train_test_split(dates_unique, test_size=test_size, random_state=seed, shuffle=False)
    return train_dates, holdout_dates

def get_corr(df) -> pl.DataFrame:
    # Numeric columns only
    df = df.select(cs.numeric())

    # Pearson correlation
    df = (df.corr()
        .with_columns(index = pl.lit(pl.Series(df.columns)))
        .unpivot(index = "index")
        .filter(pl.col("index") != pl.col("variable"))
    ).sort("value", descending=True)

    # Remove diagonals by making a list from variable names, sorting list, removing duplicates
    df = df.with_columns(
            pl.struct(a='index', b='variable').alias('fields')
        ).with_columns(
            pl.concat_list(pl.col("fields").struct["a"], pl.col("fields").struct["b"]).alias("fields").list.sort()
        ).unique(subset=["fields"]).drop('fields')

    # Cleanup column names
    df = df.rename({"index": "Variable 1", "variable": "Variable 2", "value": "Correlation"})

    # Highest correlation first
    df = df.sort('Correlation', descending=True)

    return df