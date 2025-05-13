import polars as pl
import polars.selectors as cs

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