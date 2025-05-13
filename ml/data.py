import polars as pl


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
    return penguins
