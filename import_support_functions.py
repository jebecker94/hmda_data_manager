# Import Packages
import io
import zipfile
import pandas as pd
import numpy as np
import subprocess
import pyarrow as pa
from csv import Sniffer
import polars as pl
import ast
from pathlib import Path


# Get Delimiter
def get_delimiter(file_path: Path | str, bytes: int = 4096) -> str:
    """Determine the delimiter used in a delimited text file.

    Parameters
    ----------
    file_path : Path | str
        Path to the delimited text file.
    bytes : int, optional
        Number of bytes to read for delimiter detection. Defaults to 4096.

    Returns
    -------
    str
        Detected delimiter character.
    """

    # Initialize CSV Sniffer
    sniffer = Sniffer()

    # Open File
    data = io.open(file_path, mode="r", encoding="latin-1").read(bytes)

    # Find Delimiter
    delimiter = sniffer.sniff(data).delimiter

    # Return Delimiter
    return delimiter


# Get File Schema
def get_file_schema(
    schema_file: Path | str, schema_type: str = "pyarrow"
) -> pa.Schema | dict[str, str] | pl.Schema:
    """Convert the CFPB HMDA schema to a specified representation.

    Parameters
    ----------
    schema_file : Path | str
        Path to the CFPB HMDA schema HTML file.
    schema_type : str, optional
        Desired schema representation: ``"pyarrow"``, ``"pandas`` or
        ``"polars"``. Defaults to ``"pyarrow"``.

    Returns
    -------
    pa.Schema | dict[str, str] | pl.Schema
        Schema compatible with the requested ``schema_type``.

    Raises
    ------
    ValueError
        If ``schema_type`` is not one of the supported options.
    """

    # Check Schema Type
    if schema_type not in ["pyarrow", "pandas", "polars"]:
        raise ValueError(
            'The schema type must be either "pyarrow" or "pandas" or "polars".'
        )

    # Load the schema file
    df = pd.read_html(schema_file)[0]

    # Get Field Column
    FieldVar = "Field"
    if "Field" not in df.columns:
        FieldVar = "Fields"

    LengthVar = "Max Length"
    if "Max Length" not in df.columns:
        LengthVar = "Maximum Length"

    # Convert the schema to a PyArrow schema
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
        schema = pa.schema(schema)

    # Convert the schema to a Pandas schema
    elif schema_type == "pandas":
        schema = {}
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

    # Convert the schema to a Polars schema (In progress)
    elif schema_type == "polars":
        schema = {}
        for _, row in df.iterrows():
            pd_type = pl.String()
            if row["Type"] == "Numeric":
                pd_type = pl.Float64()
            if (row["Type"] == "Numeric") & (row[LengthVar] <= 4):
                pd_type = pl.Int16()
            if (row["Type"] == "Numeric") & (row[LengthVar] > 4):
                pd_type = pl.Int32()
            if (row["Type"] == "Numeric") & (row[LengthVar] > 9):
                pd_type = pl.Int64()
            schema[row[FieldVar]] = pd_type
        schema = pl.Schema(schema)

    # Return the schema
    return schema


# Replace Column Names in CSV
def replace_csv_column_names(
    csv_file: Path | str, column_name_mapper: dict[str, str] | None = None
) -> None:
    """Replace column headers in a CSV file based on a mapping.

    Parameters
    ----------
    csv_file : Path | str
        Path to the CSV file whose header should be updated.
    column_name_mapper : dict[str, str] | None, optional
        Mapping from existing header names to replacement names.

    Returns
    -------
    None
        The CSV file is modified in place.
    """

    if column_name_mapper is None:
        column_name_mapper = {}

    # Get File Delimiter
    delimiter = get_delimiter(csv_file, bytes=16000)

    # Read First Line
    with open(csv_file, "r") as f:
        first_line = f.readline().strip()

    # Replace Column Names
    first_line_items = first_line.split(delimiter)
    new_first_line_items = []
    for first_line_item in first_line_items:
        for key, item in column_name_mapper.items():
            if first_line_item == key:
                first_line_item = item
        new_first_line_items.append(first_line_item)
    new_first_line = delimiter.join(new_first_line_items)

    # Write New First Line and Copy Rest of File
    with open(csv_file, "r") as f:
        lines = f.readlines()
    lines[0] = new_first_line + "\n"
    with open(csv_file, "w") as f:
        f.writelines(lines)


# Unzip HMDA Data
def unzip_hmda_file(
    zip_file: Path | str, raw_folder: Path | str, replace: bool = False
) -> Path:
    """Extract a compressed HMDA archive.

    Parameters
    ----------
    zip_file : Path | str
        Path to the ``.zip`` archive containing HMDA data.
    raw_folder : Path | str
        Directory where extracted files will be written.
    replace : bool, optional
        If ``True`` existing extracted files will be overwritten. Defaults to
        ``False``.

    Returns
    -------
    Path
        Path to the extracted delimited file.
    """

    # Check that File is Zip. If not, check for similar named zip file
    zip_file = Path(zip_file)
    raw_folder = Path(raw_folder)
    if zip_file.suffix.lower() != ".zip":
        zip_file = zip_file.with_suffix(".zip")
        if not zip_file.exists():
            raise ValueError(
                "The file name was not given as a zip file. Failed to find a comparably-named zip file."
            )

    # Unzip File
    with zipfile.ZipFile(zip_file) as z:
        delimited_files = [
            x
            for x in z.namelist()
            if (x.endswith(".txt") or x.endswith(".csv")) and "/" not in x
        ]
        for file in delimited_files:
            # Unzip if New File Doesn't Exist or Replace Option is On
            raw_file_name = raw_folder / file
            if (not raw_file_name.exists()) or replace:
                # Extract and Create Temporary File
                print("Extracting File:", file)
                try:
                    z.extract(file, path=raw_folder)
                except Exception:
                    print(
                        "Could not unzip file:",
                        file,
                        "with Pythons Zipfile package. Using 7z instead.",
                    )
                    unzip_string = "C:/Program Files/7-Zip/7z.exe"
                    p = subprocess.Popen(
                        [
                            unzip_string,
                            "e",
                            str(zip_file),
                            f"-o{raw_folder}",
                            file,
                            "-y",
                        ]
                    )
                    p.wait()

            # Convert First Line of Panel Files
            if "panel" in file:
                column_name_mapper = {
                    "topholder_rssd": "top_holder_rssd",
                    "topholder_name": "top_holder_name",
                    "upper": "lei",
                }
                replace_csv_column_names(
                    raw_file_name, column_name_mapper=column_name_mapper
                )

    # Return Raw File Name
    return raw_file_name


# Rename HMDA Columns
def rename_hmda_columns(
    df: pd.DataFrame | pl.DataFrame | pl.LazyFrame, df_type: str = "polars"
) -> pd.DataFrame | pl.DataFrame | pl.LazyFrame:
    """Standardize HMDA column names across data formats.

    Parameters
    ----------
    df : pd.DataFrame | pl.DataFrame | pl.LazyFrame
        DataFrame with original HMDA column names.
    df_type : str, optional
        Indicates the DataFrame library used (``"pandas"`` or ``"polars"``).
        Defaults to ``"polars"``.

    Returns
    -------
    pd.DataFrame | pl.DataFrame | pl.LazyFrame
        DataFrame with standardized column names.
    """

    # Column Name Dictionary
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

    # Rename
    if df_type == "pandas":
        df = df.rename(columns=column_dictionary, errors="ignore")
    elif df_type == "polars":
        df = df.rename(column_dictionary, strict=False)

    # Return DataFrame
    return df


# Dstring HMDA Columns before 2007
def destring_hmda_cols_pre2007(df: pl.DataFrame) -> pl.DataFrame:
    """Convert numeric HMDA columns stored as strings to numeric types.

    Parameters
    ----------
    df : pl.DataFrame
        HMDA data with numeric fields stored as strings.

    Returns
    -------
    pl.DataFrame
        DataFrame with numeric columns converted to numeric dtype.
    """

    # Numeric and Categorical Columns
    numeric_columns = [
        "activity_year",
        "loan_type",
        "loan_purpose",
        "occupancy_type",
        "loan_amount",
        "action_taken",
        "msa_md",
        "state_code",
        "county_code",
        "applicant_race_1",
        "co_applicant_race_1",
        "applicant_sex",
        "co_applicant_sex",
        "income",
        "purchaser_type",
        "denial_reason_1",
        "denial_reason_2",
        "denial_reason_3",
        "edit_status",
        "sequence_number",
    ]

    casts = [
        pl.col(col).cast(pl.Float64, strict=False)
        for col in numeric_columns
        if col in df.columns
    ]
    if casts:
        df = df.with_columns(casts)

    return df


# Destring HMDA Columns
def destring_hmda_cols_2007_2017(df):
    """
    Destring numeric HMDA columns

    Parameters
    ----------
    df : pd.DataFrame, pl.DataFrame, pl.LazyFrame
        DataFrame to destring.

    Returns
    -------
    df : pd.DataFrame, pl.DataFrame, pl.LazyFrame
        DataFrame with destringed columns.

    """

    # Dsplay Progress
    print("Destringing HMDA Variables")

    # Fix County Code and Census Tract
    geo_cols = ["state_code", "county_code", "census_tract"]
    df[geo_cols] = df[geo_cols].apply(pd.to_numeric, errors="coerce")
    df["state_code"].astype("Int16")
    df["county_code"] = (1000 * df["state_code"] + df["county_code"]).astype("Int32")
    df["census_tract"] = np.round(100 * df["census_tract"]).astype("Int32")
    df["census_tract"] = df["census_tract"].astype(str)
    df["census_tract"] = [x.zfill(6) for x in df["census_tract"]]
    df["census_tract"] = df["county_code"].astype("str") + df["census_tract"]
    df["census_tract"] = pd.to_numeric(df["census_tract"], errors="coerce")
    df["census_tract"] = df["census_tract"].astype("Int64")

    # Numeric and Categorical Columns
    numeric_columns = [
        "activity_year",
        "loan_type",
        "loan_purpose",
        "occupancy_type",
        "loan_amount",
        "action_taken",
        "msa_md",
        "applicant_race_1",
        "applicant_race_2",
        "applicant_race_3",
        "applicant_race_4",
        "applicant_race_5",
        "co_applicant_race_1",
        "co_applicant_race_2",
        "co_applicant_race_3",
        "co_applicant_race_4",
        "co_applicant_race_5",
        "applicant_sex",
        "co_applicant_sex",
        "income",
        "purchaser_type",
        "denial_reason_1",
        "denial_reason_2",
        "denial_reason_3",
        "edit_status",
        "sequence_number",
        "rate_spread",
        "tract_population",
        "tract_minority_population_percent",
        "ffiec_msa_md_median_family_income",
        "tract_to_msa_income_percentage",
        "tract_owner_occupied_units",
        "tract_one_to_four_family_homes",
        "tract_median_age_of_housing_units",
    ]

    # Convert Columns to Numeric
    for numeric_column in numeric_columns:
        if numeric_column in df.columns:
            df[numeric_column] = pd.to_numeric(df[numeric_column], errors="coerce")

    # Return DataFrame
    return df


# Destring HMDA Columns
def destring_hmda_cols_after_2018(lf):
    """
    Destring numeric HMDA variables after 2018.

    Parameters
    ----------
    lf : pl.DataFrame or pl.LazyFrame
        HMDA data with numeric columns represented as strings.

    Returns
    -------
    lf : pl.DataFrame or pl.LazyFrame
        DataFrame with numeric fields cast to appropriate numeric types.

    """

    # Dsplay Progress
    print("Destringing HMDA Variables")

    # Replace Exempt w/ -99999
    exempt_cols = [
        "combined_loan_to_value_ratio",
        "interest_rate",
        "rate_spread",
        "loan_term",
        "prepayment_penalty_term",
        "intro_rate_period",
        "income",
        "multifamily_affordable_units",
        "property_value",
        "total_loan_costs",
        "total_points_and_fees",
        "origination_charges",
        "discount_points",
        "lender_credits",
    ]
    for exempt_col in exempt_cols:
        lf = lf.with_columns(
            pl.col(exempt_col).replace("Exempt", "-99999").alias(exempt_col)
        )
        lf = lf.cast({exempt_col: pl.Float64}, strict=False)

    # Clean Units
    replace_column = "total_units"
    lf = lf.with_columns(
        pl.col(replace_column)
        .replace(["5-24", "25-49", "50-99", "100-149", ">149"], [5, 6, 7, 8, 9])
        .alias(replace_column)
    )
    lf = lf.cast({replace_column: pl.Float64}, strict=False)

    # Clean Age
    for replace_column in ["applicant_age", "co_applicant_age"]:
        lf = lf.with_columns(
            pl.col(replace_column)
            .replace(
                ["<25", "25-34", "35-44", "45-54", "55-64", "65-74", ">74"],
                [1, 2, 3, 4, 5, 6, 7],
            )
            .alias(replace_column)
        )
        lf = lf.cast({replace_column: pl.Float64}, strict=False)

    # Clean Age Dummy Variables
    for replace_column in ["applicant_age_above_62", "co_applicant_age_above_62"]:
        lf = lf.with_columns(
            pl.col(replace_column)
            .replace(
                ["No", "no", "NO", "Yes", "yes", "YES", "Na", "na", "NA"],
                [0, 0, 0, 1, 1, 1, None, None, None],
            )
            .alias(replace_column)
        )
        lf = lf.cast({replace_column: pl.Float64}, strict=False)

    # Clean Debt-to-Income
    replace_column = "debt_to_income_ratio"
    lf = lf.with_columns(
        pl.col(replace_column)
        .replace(
            ["<20%", "20%-<30%", "30%-<36%", "50%-60%", ">60%", "Exempt"],
            [10, 20, 30, 50, 60, -99999],
        )
        .alias(replace_column)
    )
    lf = lf.cast({replace_column: pl.Float64}, strict=False)

    # Clean Conforming Loan Limit
    replace_column = "conforming_loan_limit"
    lf = lf.with_columns(
        pl.col(replace_column)
        .replace(["NC", "C", "U", "NA"], [0, 1, 1111, -1111])
        .alias(replace_column)
    )
    lf = lf.cast({replace_column: pl.Float64}, strict=False)

    # Numeric and Categorical Columns
    numeric_columns = [
        "loan_type",
        "loan_purpose",
        "occupancy_type",
        "loan_amount",
        "action_taken",
        "msa_md",
        "county_code",
        "applicant_race_1",
        "applicant_race_2",
        "applicant_race_3",
        "applicant_race_4",
        "applicant_race_5",
        "co_applicant_race_1",
        "co_applicant_race_2",
        "co_applicant_race_3",
        "co_applicant_race_4",
        "co_applicant_race_5",
        "applicant_ethnicity_1",
        "applicant_ethnicity_2",
        "applicant_ethnicity_3",
        "applicant_ethnicity_4",
        "applicant_ethnicity_5",
        "co_applicant_ethnicity_1",
        "co_applicant_ethnicity_2",
        "co_applicant_ethnicity_3",
        "co_applicant_ethnicity_4",
        "co_applicant_ethnicity_5",
        "applicant_sex",
        "co_applicant_sex",
        "income",
        "purchaser_type",
        "submission_of_application",
        "initially_payable_to_institution",
        "aus_1",
        "aus_2",
        "aus_3",
        "aus_4",
        "aus_5",
        "denial_reason_1",
        "denial_reason_2",
        "denial_reason_3",
        "denial_reason_4",
        "edit_status",
        "sequence_number",
        "rate_spread",
        "tract_population",
        "tract_minority_population_percent",
        "ffiec_msa_md_median_family_income",
        "tract_to_msa_income_percentage",
        "tract_owner_occupied_units",
        "tract_one_to_four_family_homes",
        "tract_median_age_of_housing_units",
    ]
    # Convert Columns to Numeric
    for numeric_column in numeric_columns:
        if numeric_column in lf.collect_schema().names():
            lf = lf.cast({numeric_column: pl.Float64}, strict=False)

    # Return DataFrame
    return lf


# Rename HMDA Columns
def split_and_save_tract_variables(df, save_folder, file_name):
    """
    Split and save tract variables from the HMDA data frame.

    Parameters
    ----------
    df : pd.DataFrame, pl.DataFrame, pl.LazyFrame
        Data with tract variables.
    save_folder : str
        Folder to save the tract variables.
    file_name : str
        File name to save the tract variables.

    Returns
    -------
    df : pd.DataFrame, pl.DataFrame, pl.LazyFrame
        Data frame without the tract variables.

    """

    # Check DataFrame Type
    if not isinstance(df, [pd.DataFrame, pl.DataFrame, pl.LazyFrame]):
        raise ValueError(
            "The input dataframe must be a pandas DataFrame, polars lazyframe, or polars dataframe."
        )

    # Column Name Dictionary
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

    # Pandas Implementation
    if isinstance(df, pd.DataFrame):
        # Convert Columns to Numeric
        for tract_variable in tract_variables:
            df[tract_variable] = pd.to_numeric(df[tract_variable], errors="coerce")

        # Separate and DropExisting Tract Variables
        if tract_variables:
            # Separate Tract Variables
            df_tract = df[
                ["activity_year", "census_tract"] + tract_variables
            ].drop_duplicates()
            df_tract.to_parquet(
                f"{save_folder}/tract_variables/tract_vars_{file_name}.parquet",
                index=False,
            )

            # Drop Tract Variables and Return DataFrame
            df = df.drop(columns=tract_variables)

    # Polars Implementation
    elif isinstance(df, pl.DataFrame) | isinstance(df, pl.LazyFrame):
        # Convert Columns to Numeric
        for tract_variable in tract_variables:
            df = df.with_columns(pl.col(tract_variable).cast(pl.Float64))

        # Separate and Drop Existing Tract Variables
        if tract_variables:
            # Separate Tract Variables
            df_tract = df.select(
                ["activity_year", "census_tract"] + tract_variables
            ).drop_duplicates()
            df_tract.write_parquet(
                f"{save_folder}/tract_variables/tract_vars_{file_name}.parquet"
            )

            # Drop Tract Variables and Return DataFrame
            df = df.drop(tract_variables)

    # Return DataFrame
    return df


# Prepare for Stata
def downcast_hmda_variables(df):
    """
    Downcast HMDA variables

    Parameters
    ----------
    df : pandas DataFrame
        DataFrame whose numeric columns should be downcast to smaller dtypes.

    Returns
    -------
    df : pandas DataFrame
        DataFrame with downcasted numeric columns.

    """

    # Downcast Numeric Types
    # for col in df.columns :
    #     try :
    #         df[col] = df[col].astype('Int16')
    #     except (TypeError, OverflowError) :
    #         print('Cannot downcast variable:', col)
    for col in ["msa_md", "county_code", "sequence_number"]:
        if col in df.columns:
            df[col] = df[col].astype("Int32")

    # Return DataFrame and Labels
    return df


# Save to Stata
def save_file_to_stata(file):
    df = pd.read_parquet(file)
    df, variable_labels, value_labels = prepare_hmda_for_stata(df)
    save_file_dta = file.replace(".parquet", ".dta")
    df.to_stata(
        save_file_dta,
        write_index=False,
        variable_labels=variable_labels,
        value_labels=value_labels,
    )


# Prepare for Stata
def prepare_hmda_for_stata(df, labels_folder=None):
    """
    Create variable and value labels to save DTA files for stata.

    Parameters
    ----------
    df : pandas DataFrame
        Data.

    Returns
    -------
    df : pandas DataFrame
        Data cleaned for stata format.
    variable_labels : dictionary
        Labels for variables in the data.
    value_labels : dictionary
        Labels for values in the data.

    """

    # Read Value Labels
    value_label_file = labels_folder / "loans/hmda_value_labels.txt"
    with open(value_label_file, "r") as f:
        value_labels = ast.literal_eval(f.read())

    # Read Variable Labels
    variable_label_file = labels_folder / "loans/hmda_variable_labels.txt"
    with open(variable_label_file, "r") as f:
        variable_labels = ast.literal_eval(f.read())

    # Trim Value and Variable Labels
    variable_labels = {
        key[0:32].replace("-", "_"): value[0:80]
        for key, value in variable_labels.items()
        if key in df.columns
    }
    value_labels = {
        key[0:32].replace("-", "_"): value
        for key, value in value_labels.items()
        if key in df.columns
    }
    df.columns = [x[0:32].replace("-", "_") for x in df.columns]

    # Downcast Numeric Types
    vl = [key for key, value in value_labels.items()]
    for col in vl + ["activity_year"]:
        try:
            df[col] = df[col].astype("Int16")
        except (TypeError, OverflowError):
            print("Cannot downcast variable:", col)
    for col in ["msa_md", "county_code", "sequence_number"]:
        if col in df.columns:
            df[col] = df[col].astype("Int32")

    # Return DataFrame and Labels
    return df, variable_labels, value_labels
