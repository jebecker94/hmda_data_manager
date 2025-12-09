"""
Schema utilities: column renaming and standardization.
"""

import pandas as pd
import polars as pl


def rename_hmda_columns(
    df: pd.DataFrame | pl.DataFrame | pl.LazyFrame, df_type: str = "polars"
) -> pd.DataFrame | pl.DataFrame | pl.LazyFrame:
    """Standardize HMDA column names across data formats."""
    column_dictionary = {
        "occupancy": "occupancy_type",
        "as_of_year": "activity_year",
        "owner_occupancy": "occupancy_type",
        "loan_amount_000s": "loan_amount",
        "census_tract_number": "census_tract",
        "applicant_income_000s": "income",
        "derived_msa-md": "msa_md",
        "derived_msa_md": "msa_md",
        "msamd": "msa_md",
        "population": "tract_population",
        "minority_population": "tract_minority_population_percent",
        "hud_median_family_income": "ffiec_msa_md_median_family_income",
        "tract_to_msamd_income": "tract_to_msa_income_percentage",
        "number_of_owner_occupied_units": "tract_owner_occupied_units",
        "number_of_1_to_4_family_units": "tract_one_to_four_family_homes",
    }

    if df_type == "pandas":
        return df.rename(columns=column_dictionary, errors="ignore")  # type: ignore[return-value]
    return df.rename(column_dictionary, strict=False)  # type: ignore[return-value]


__all__ = [
    "rename_hmda_columns",
]


