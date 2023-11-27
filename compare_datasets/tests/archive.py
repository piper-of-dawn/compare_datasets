df = pl.scan_csv(
    "compare_datasets/tests/wec.csv").select(
    [
        "country",
        "year",
        "coal_elec_per_capita",
        "electricity_generation",
        "oil_electricity",
    ]).filter(pl.col("year") == 2018).with_columns([
    pl.exclude(["country", "year"]).cast(pl.Float64),
]).collect()