"""
HMDA Import Functions for Post-2018 Data (2018+)

This module handles import and processing of modern HMDA data files from 2018 onward.
This period introduced significant changes including the HMDAIndex, expanded demographic
fields, and more complex data structures.

Key Characteristics:
- HMDAIndex unique identifier system
- File type codes (a, b, c, d, e)
- Derived demographic columns
- Extensive tract variables
- More complex schema with many optional fields

This module may continue to evolve as CFPB updates the HMDA data format.
"""

import logging
import shutil
import time
from collections.abc import Sequence
from pathlib import Path
import polars as pl
from ...utils.support import get_delimiter, get_file_schema, unzip_hmda_file
from .common import HMDA_INDEX_COLUMN


logger = logging.getLogger(__name__)


# Constants specific to post2018 data
DERIVED_COLUMNS = [
    "derived_loan_product_type",
    "derived_race",
    "derived_ethnicity",
    "derived_sex",
    "derived_dwelling_category",
]

POST2018_TRACT_COLUMNS = [
    "tract_population",
    "tract_minority_population_percent",
    "ffiec_msa_md_median_family_income",
    "tract_to_msa_income_percentage",
    "tract_owner_occupied_units",
    "tract_one_to_four_family_homes",
    "tract_median_age_of_housing_units",
]

POST2018_NUMERIC_COLUMNS = [
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

POST2018_EXEMPT_COLUMNS = [
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

FLOAT_DTYPES = {pl.Float32, pl.Float64}
INTEGER_DTYPES = {
    pl.Int8,
    pl.Int16,
    pl.Int32,
    pl.Int64,
    pl.UInt8,
    pl.UInt16,
    pl.UInt32,
    pl.UInt64,
}


def normalized_file_stem(stem: str) -> str:
    """Remove common suffixes from extracted archive names.

    Parameters
    ----------
    stem : str
        File stem (name without extension)

    Returns
    -------
    str
        Normalized file stem with common suffixes removed
    """
    if stem.endswith("_csv"):
        stem = stem[:-4]
    if stem.endswith("_pipe"):
        stem = stem[:-5]
    return stem


def should_process_output(path: Path, replace: bool) -> bool:
    """Return ``True`` when the target path should be generated.

    Parameters
    ----------
    path : Path
        Target output file path
    replace : bool
        Whether to replace existing files

    Returns
    -------
    bool
        True if file should be processed
    """
    return replace or not path.exists()


def _limit_schema_to_available_columns(
    raw_file: Path, delimiter: str, schema: dict[str, pl.DataType | str]
) -> dict[str, pl.DataType | str]:
    """Restrict a schema to the columns available in a delimited file.

    Parameters
    ----------
    raw_file : Path
        Path to the raw CSV/delimited file
    delimiter : str
        Column delimiter character
    schema : dict[str, pl.DataType | str]
        Full schema dictionary

    Returns
    -------
    dict[str, pl.DataType | str]
        Schema restricted to available columns
    """
    csv_columns = pl.read_csv(
        raw_file, separator=delimiter, n_rows=0, ignore_errors=True
    ).columns
    logger.debug("CSV columns: %s", csv_columns)
    return {column: schema[column] for column in csv_columns if column in schema}


def _get_file_type_code(file_name: Path | str) -> str:
    """Derive the HMDA file type code from a file name.

    Parameters
    ----------
    file_name : Path | str
        Name of the HMDA file.

    Returns
    -------
    str
        Single-character code representing the HMDA file type.

    Raises
    ------
    ValueError
        If the file type cannot be determined from ``file_name``.
    """
    # Get Base Name of File
    base_name = Path(file_name).stem

    # Get Version Types from Prefixes
    base_name_lower = base_name.lower()
    if "three_year" in base_name_lower:
        return "a"
    elif "one_year" in base_name_lower:
        return "b"
    elif (
        "public_lar" in base_name_lower
        or "public_panel" in base_name_lower
        or "public_ts" in base_name_lower
    ):
        return "c"
    elif "nationwide" in base_name_lower:
        return "d"
    elif "mlar" in base_name_lower:
        return "e"
    raise ValueError("Cannot parse the HMDA file type from the provided file name.")


def _format_census_tract(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Standardize the census tract column within a lazy frame.

    Parameters
    ----------
    lf : pl.LazyFrame
        Input lazy frame with census tract column

    Returns
    -------
    pl.LazyFrame
        Lazy frame with standardized census tract column
    """
    return (
        lf.cast({"census_tract": pl.Float64}, strict=False)
        .cast({"census_tract": pl.Int64}, strict=False)
        .cast({"census_tract": pl.String}, strict=False)
        .with_columns(
            pl.col("census_tract").str.zfill(11).alias("census_tract")
        )
    )


def _integer_like_float_columns(
    lf: pl.LazyFrame, float_columns: Sequence[str]
) -> list[str]:
    """Return float columns whose non-null values are all integers."""
    
    if not float_columns:
        return []

    checks = []
    for column in float_columns:
        # Check if all non-null values are integers
        check = (
            pl.when(pl.col(column).is_null())
            .then(True)  # Null values are fine
            .otherwise(
                pl.when(pl.col(column).is_infinite())
                .then(False)  # Infinite values are not integers
                .otherwise(
                    # Check if the value equals its floor (integer check)
                    pl.col(column) == pl.col(column).floor()
                )
            )
            .all()
            .alias(column)
        )
        checks.append(check)

    if not checks:
        return []

    result = lf.select(checks).collect()
    return [column for column in float_columns if bool(result[column][0])]


def _destring_and_cast_hmda_cols_post2018(
    lf: pl.LazyFrame, schema: dict[str, pl.DataType | str] | None = None
) -> pl.LazyFrame:
    """
    Destring numeric HMDA variables and cast integer-like floats to integers.

    This function combines the functionality of destring_hmda_cols_post2018 and
    _cast_integer_like_floats to reduce redundancy and improve performance.

    Parameters
    ----------
    lf : pl.LazyFrame
        HMDA data with numeric columns represented as strings.
    schema : dict[str, pl.DataType | str], optional
        Schema dictionary for determining target integer types. If None, defaults to Int64.

    Returns
    -------
    pl.LazyFrame
        LazyFrame with numeric fields cast to appropriate numeric types and
        integer-like floats downcast to integers.
    """
    logger.info("Destringing HMDA variables and casting integer-like floats")

    # Replace exempt columns with -99999
    for exempt_col in POST2018_EXEMPT_COLUMNS:
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
    
    # Convert Columns to Numeric
    for numeric_column in POST2018_NUMERIC_COLUMNS:
        if numeric_column in lf.collect_schema().names():
            lf = lf.cast({numeric_column: pl.Float64}, strict=False)

    # Cast to integers based on schema first, then detect remaining integer-like floats
    if schema:
        schema_int_casts = []
        for column, target_dtype in schema.items():
            if target_dtype in INTEGER_DTYPES and column in lf.schema:
                # Only cast if the column is currently a float (preserve existing integers)
                if lf.schema[column] in FLOAT_DTYPES:
                    schema_int_casts.append(
                        pl.col(column).cast(target_dtype, strict=False).alias(column)  # type: ignore[arg-type]
                    )
        
        if schema_int_casts:
            lf = lf.with_columns(schema_int_casts)
    
    # Then handle remaining float columns that should be integers (including those not in schema)
    float_columns = [
        column for column, dtype in lf.schema.items() 
        if dtype in FLOAT_DTYPES
    ]
    integer_like_columns = _integer_like_float_columns(lf, float_columns)
    
    if integer_like_columns:
        casts = []
        for column in integer_like_columns:
            # Use schema dtype if available, otherwise Int64
            target_dtype = pl.Int64
            if schema and column in schema:
                schema_dtype = schema[column]
                if schema_dtype in INTEGER_DTYPES:
                    target_dtype = schema_dtype
            casts.append(
                pl.col(column).cast(target_dtype, strict=False).alias(column)  # type: ignore[arg-type]
            )
        
        lf = lf.with_columns(casts)

    return lf


def _cast_integer_like_floats_only(
    lf: pl.LazyFrame, schema: dict[str, pl.DataType | str]
) -> pl.LazyFrame:
    """Cast float columns with integer-only values to integer dtypes (without destringing)."""
    float_columns = [
        column for column, dtype in lf.schema.items() if dtype in FLOAT_DTYPES
    ]
    integer_like_columns = _integer_like_float_columns(lf, float_columns)
    if not integer_like_columns:
        return lf

    casts = []
    for column in integer_like_columns:
        target_dtype = schema.get(column, pl.Int64)
        if target_dtype not in INTEGER_DTYPES:
            target_dtype = pl.Int64
        casts.append(pl.col(column).cast(target_dtype, strict=False).alias(column))  # type: ignore[arg-type]

    return lf.with_columns(casts)


def _append_hmda_index(
    lf: pl.LazyFrame, year: int, file_type_code: str
) -> pl.LazyFrame:
    """Format the HMDA index values as strings with a consistent prefix.
    
    The HMDAIndex provides a unique identifier for each HMDA record consisting of:
    - Activity year (4 digits)  
    - File type code (1 character: a, b, c, d, e)
    - Row number (9 digits, zero-padded)
    
    For example: 2018a_000000001
    
    Parameters
    ----------
    lf : pl.LazyFrame
        Input lazy frame with row index
    year : int
        Activity year
    file_type_code : str
        Single character file type code
        
    Returns
    -------
    pl.LazyFrame
        Lazy frame with formatted HMDAIndex column
    """
    prefix = f"{year}{file_type_code}_"
    return (
        lf.cast({HMDA_INDEX_COLUMN: pl.String}, strict=False)
        .with_columns(pl.col(HMDA_INDEX_COLUMN).str.zfill(9).alias(HMDA_INDEX_COLUMN))
        .with_columns(
            (pl.lit(prefix) + pl.col(HMDA_INDEX_COLUMN)).alias(HMDA_INDEX_COLUMN)
        )
    )


def _rename_columns_post2018(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Rename columns for post2018 files to standardize naming.

    Note: If other errors in the variable names are discovered, add them here.
    
    Parameters
    ----------
    lf : pl.LazyFrame
        Input lazy frame
        
    Returns
    -------
    pl.LazyFrame
        Lazy frame with renamed columns
    """
    rename_dict = {
        "loan_to_value_ratio": "combined_loan_to_value_ratio",
    }
    return lf.rename(rename_dict, strict=False)


def import_hmda_post2018(
    data_folder: Path,
    save_folder: Path,
    schema_file: Path,
    min_year: int = 2018,
    max_year: int = 2024,
    replace: bool = False,
    remove_raw_file: bool = True,
    add_hmda_index: bool = True,
    add_file_type: bool = True,
    clean: bool = False,
    overwrite_clean: bool = False,
) -> None:
    """
    Import and optionally clean HMDA data for 2018 onward.

    This function processes modern HMDA zip archives and adds the HMDAIndex unique
    identifier system. The HMDAIndex is a unique identifier for each loan application
    record consisting of:
    - Activity year (4 digits)
    - File type code (1 character)
    - Row number (9 digits, zero padded)
    For example: 2018a_000000001

    Parameters
    ----------
    data_folder : Path
        Folder where raw zip archives are stored.
    save_folder : Path
        Folder where cleaned parquet files will be saved.
    schema_file : Path
        Path to the HTML schema file for post2018 data.
    min_year : int, optional
        First year of data to include. Default is 2018.
    max_year : int, optional
        Last year of data to include. Default is 2024.
    replace : bool, optional
        Whether to replace existing files. Default is False.
    remove_raw_file : bool, optional
        Whether to remove the extracted raw file after processing. Default is True.
    add_hmda_index : bool, optional
        Whether to add the HMDAIndex to the data. Default is True.
    add_file_type : bool, optional
        Whether to add the file type column to the data. Default is True.
    clean : bool, optional
        Whether to apply cleaning transformations. Default is False.
    overwrite_clean : bool, optional
        Whether to overwrite existing cleaned files. Default is False.

    Returns
    -------
    None
        Files are written to save_folder as parquet files.

    Examples
    --------
    >>> from hmda_data_manager.core.import_data import import_hmda_post2018
    >>> import_hmda_post2018(
    ...     data_folder=Path("data/raw/loans"),
    ...     save_folder=Path("data/clean/loans"),
    ...     schema_file=Path("schemas/hmda_lar_schema_post2018.html"),
    ...     min_year=2020,
    ...     max_year=2024,
    ...     clean=True
    ... )
    """

    data_folder = Path(data_folder)
    save_folder = Path(save_folder)
    save_folder.mkdir(parents=True, exist_ok=True)
    schema_file = Path(schema_file)

    for year in range(min_year, max_year + 1):
        archives_found = list(data_folder.glob(f"*{year}*.zip"))
        
        if not archives_found:
            logger.debug("No archives found for year %s", year)
            continue
            
        for archive in archives_found:
            file_name = normalized_file_stem(archive.stem)
            save_file = save_folder / f"{file_name}.parquet"
            clean_save_file = save_folder / f"{file_name}_clean.parquet"

            # Check if we should process the raw file
            if not should_process_output(save_file, replace):
                logger.debug("Skipping existing file: %s", save_file)
                # If we need clean file, check if it exists
                if clean and not should_process_output(clean_save_file, overwrite_clean):
                    logger.debug("Skipping existing cleaned file: %s", clean_save_file)
                continue

            logger.info("Processing archive: %s", archive)
            
            # Extract and process the archive
            raw_file_path = Path(unzip_hmda_file(archive, archive.parent))
            try:
                # Detect delimiter and load schema
                delimiter = get_delimiter(raw_file_path, bytes=16000)
                schema = get_file_schema(schema_file=schema_file, schema_type="polars")
                if not isinstance(schema, dict):
                    raise ValueError(f"Expected dict schema, got {type(schema)}")
                limited_schema = _limit_schema_to_available_columns(
                    raw_file_path,
                    delimiter,
                    schema,  # type: ignore[arg-type]
                )
                
                # Create lazy frame based on year and settings
                if (year < 2017) or (not add_hmda_index):
                    lf = pl.scan_csv(raw_file_path, separator=delimiter, low_memory=True, schema=limited_schema)  # type: ignore[arg-type]
                    lf = _cast_integer_like_floats_only(lf, limited_schema)
                else:
                    # Scan CSV File with HMDA index
                    lf = pl.scan_csv(
                        raw_file_path,
                        separator=delimiter,
                        low_memory=True,
                        row_index_name=HMDA_INDEX_COLUMN,
                        infer_schema_length=None,
                    )

                    # Add File Type and HMDA Index
                    file_type_code = _get_file_type_code(archive)
                    if add_file_type:
                        lf = lf.with_columns(pl.lit(file_type_code).alias("file_type"))
                    if add_hmda_index:
                        lf = _append_hmda_index(lf, year, file_type_code)
                    
                    # lf = _cast_integer_like_floats_only(lf, limited_schema)
                
                # Remove derived columns (calculated by CFPB)
                lf = lf.drop(DERIVED_COLUMNS, strict=False)
                
                # Remove tract variables (demographic data)
                lf = lf.drop(POST2018_TRACT_COLUMNS, strict=False)

                # Save raw file
                lf.sink_parquet(save_file)
                
                # Apply cleaning if requested
                if clean:
                    logger.info("Applying cleaning transformations to: %s", save_file)
                    
                    # Load the saved file and apply cleaning
                    lf_clean = pl.scan_parquet(save_file, low_memory=True)
                    
                    # Rename columns for consistency
                    lf_clean = _rename_columns_post2018(lf_clean)
                    
                    # Convert string columns to numeric types
                    lf_clean = _destring_and_cast_hmda_cols_post2018(lf_clean, limited_schema)
                    
                    # Format census tract column
                    lf_clean = _format_census_tract(lf_clean)
                    
                    # Save cleaned file
                    lf_clean.sink_parquet(clean_save_file)
                    logger.debug("Saved cleaned file: %s", clean_save_file)
                
            finally:
                if remove_raw_file:
                    time.sleep(1)
                    raw_file_path.unlink(missing_ok=True)


def save_to_dataset(
    data_folder: Path,
    save_folder: Path,
    min_year: int = 2018,
    max_year: int = 2024,
):
    """Save HMDA data to dataset with Hive Partitioning.

    Parameters
    ----------
    data_folder : Path
        Folder containing processed parquet files
    save_folder : Path
        Target folder for partitioned dataset
    min_year : int, optional
        First year to include. Default is 2018.
    max_year : int, optional
        Last year to include. Default is 2024.
    """
    data_folder = Path(data_folder)
    save_folder = Path(save_folder)

    df = []
    years = range(min_year, max_year + 1)
    for year in years:
        for file in data_folder.glob(f"*{year}*.parquet"):
            df_a = pl.scan_parquet(file)
            df.append(df_a)
    df = pl.concat(df, how="diagonal_relaxed")
    year_column = "activity_year" if "activity_year" in df.columns else "as_of_year"
    df.sink_parquet(
        pl.PartitionByKey(
            save_folder,
            by=[pl.col(year_column), pl.col("file_type")],
            include_key=True,
        ),
        mkdir=True,
    )
