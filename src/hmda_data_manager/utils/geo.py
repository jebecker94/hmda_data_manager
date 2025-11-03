"""
Geospatial helpers (tract variable splitting/export helpers).
"""

from pathlib import Path
import pandas as pd
import polars as pl


def split_and_save_tract_variables(df, save_folder, file_name):
    """
    Split and save tract variables from the HMDA data frame.
    Returns the original frame with those variables removed.
    """
    if not isinstance(df, [pd.DataFrame, pl.DataFrame, pl.LazyFrame]):
        raise ValueError(
            "The input dataframe must be a pandas DataFrame, polars lazyframe, or polars dataframe."
        )

    tract_variables = [
        "tract_population",
        "tract_minority_population_percent",
        "ffiec_msa_md_median_family_income",
        "tract_to_msa_income_percentage",
        "tract_owner_occupied_units",
        "tract_one_to_four_family_homes",
        "tract_median_age_of_housing_units",
    ]
    tract_variables = [x for x in tract_variables if x in df.columns]

    if isinstance(df, pd.DataFrame):
        for tract_variable in tract_variables:
            df[tract_variable] = pd.to_numeric(df[tract_variable], errors="coerce")
        if tract_variables:
            df_tract = df[["activity_year", "census_tract"] + tract_variables].drop_duplicates()
            df_tract.to_parquet(f"{save_folder}/tract_variables/tract_vars_{file_name}.parquet", index=False)
            df = df.drop(columns=tract_variables)
    elif isinstance(df, pl.DataFrame) | isinstance(df, pl.LazyFrame):
        for tract_variable in tract_variables:
            df = df.with_columns(pl.col(tract_variable).cast(pl.Float64))
        if tract_variables:
            df_tract = df.select(["activity_year", "census_tract"] + tract_variables).drop_duplicates()
            df_tract.write_parquet(f"{save_folder}/tract_variables/tract_vars_{file_name}.parquet")
            df = df.drop(tract_variables)
    return df


__all__ = [
    "split_and_save_tract_variables",
]


