"""
Schema utilities: loading CFPB HTML schemas and renaming columns.
"""

from pathlib import Path
import pandas as pd
import polars as pl
import pyarrow as pa


def get_file_schema(
    schema_file: Path | str, schema_type: str = "pyarrow"
) -> pa.Schema | dict[str, str] | dict[str, pl.DataType]:
    """Convert the CFPB HMDA schema HTML to pyarrow/pandas/polars schema."""
    if schema_type not in ["pyarrow", "pandas", "polars"]:
        raise ValueError('schema_type must be "pyarrow", "pandas" or "polars"')

    df = pd.read_html(schema_file)[0]
    FieldVar = "Field" if "Field" in df.columns else "Fields"
    LengthVar = "Max Length" if "Max Length" in df.columns else "Maximum Length"

    if schema_type == "pyarrow":
        schema = []
        for _, row in df.iterrows():
            pa_type = pa.string()
            if row["Type"] == "Numeric":
                pa_type = pa.float64()
            if (row["Type"] == "Numeric") & (row[LengthVar] <= 4):
                pa_type = pa.int16()
            if (row["Type"] == "Numeric") & (row[LengthVar] > 4):
                pa_type = pa.int32()
            if (row["Type"] == "Numeric") & (row[LengthVar] > 9):
                pa_type = pa.int64()
            schema.append((row[FieldVar], pa_type))
        return pa.schema(schema)

    if schema_type == "pandas":
        schema: dict[str, str] = {}
        for _, row in df.iterrows():
            pd_type = "str"
            if row["Type"] == "Numeric":
                pd_type = "Float64"
            if (row["Type"] == "Numeric") & (row[LengthVar] <= 4):
                pd_type = "Int16"
            if (row["Type"] == "Numeric") & (row[LengthVar] > 4):
                pd_type = "Int32"
            if (row["Type"] == "Numeric") & (row[LengthVar] > 9):
                pd_type = "Int64"
            schema[row[FieldVar]] = pd_type
        return schema

    # polars
    schema_pl: dict[str, pl.DataType] = {}
    for _, row in df.iterrows():
        pl_type: pl.DataType = pl.String
        if row["Type"] == "Numeric":
            pl_type = pl.Float64
        if (row["Type"] == "Numeric") & (row[LengthVar] <= 4):
            pl_type = pl.Int16
        if (row["Type"] == "Numeric") & (row[LengthVar] > 4):
            pl_type = pl.Int32
        if (row["Type"] == "Numeric") & (row[LengthVar] > 9):
            pl_type = pl.Int64
        schema_pl[row[FieldVar]] = pl_type
    return schema_pl


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
    "get_file_schema",
    "rename_hmda_columns",
]


