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
import time
from collections.abc import Sequence
from pathlib import Path
from typing import Literal
import polars as pl
from ...utils.support import get_delimiter, get_file_schema, unzip_hmda_file
from .common import HMDA_INDEX_COLUMN
from ..config import RAW_DIR, get_medallion_dir
from ...schemas import get_schema_path


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

POST2018_FLOAT_COLUMNS = [
    "interest_rate",
    "combined_loan_to_value_ratio",
    "rate_spread",
    "total_loan_costs",
    "total_points_and_fees",
    "origination_charges",
    "discount_points",
    "lender_credits",
    "tract_minority_population_percent",
    "ffiec_msa_md_median_family_income",
    "tract_to_msa_income_percentage",
]

POST2018_INTEGER_COLUMNS = [
    "loan_type",
    "loan_purpose",
    "occupancy_type",
    "loan_amount",
    "action_taken",
    "msa_md",
    "derived_msa_md",
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
    "multifamily_affordable_units",
    "property_value",
    "prepayment_penalty_term",
    "intro_rate_period",
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
    "tract_population",
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
            pl.col(exempt_col)
            .replace("Exempt", "-99999")
            .alias(exempt_col)
        )

    # Clean Units
    replace_column = "total_units"
    lf = lf.with_columns(
        pl.col(replace_column)
        .replace(
            ["5-24", "25-49", "50-99", "100-149", ">149"],
            [5, 6, 7, 8, 9],
        )
        .cast(pl.Int16, strict=False)
        .alias(replace_column)
    )

    # Clean Age
    for replace_column in ["applicant_age", "co_applicant_age"]:
        lf = lf.with_columns(
            pl.col(replace_column)
            .replace(
                ["<25", "25-34", "35-44", "45-54", "55-64", "65-74", ">74"],
                [1, 2, 3, 4, 5, 6, 7],
            )
            .cast(pl.Int16, strict=False)
            .alias(replace_column)
        )

    # Clean Age Dummy Variables
    for replace_column in ["applicant_age_above_62", "co_applicant_age_above_62"]:
        lf = lf.with_columns(
            pl.col(replace_column)
            .replace(
                ["No", "no", "NO", "Yes", "yes", "YES", "Na", "na", "NA"],
                [0, 0, 0, 1, 1, 1, None, None, None],
            )
            .cast(pl.Int16, strict=False)
            .alias(replace_column)
        )

    # Clean Debt-to-Income
    replace_column = "debt_to_income_ratio"
    lf = lf.with_columns(
        pl.col(replace_column)
        .replace(
            ["<20%", "20%-<30%", "30%-<36%", "50%-60%", ">60%", "Exempt"],
            [10, 20, 30, 50, 60, -99999],
        )
        .cast(pl.Int64, strict=False)
        .alias(replace_column)
    )

    # # Clean Conforming Loan Limit (Leave as string for now)
    # replace_column = "conforming_loan_limit"
    # lf = lf.with_columns(
    #     pl.col(replace_column)
    #     .replace(["NC", "C", "U", "NA"], [0, 1, 1111, -1111])
    #     .cast(pl.Int64, strict=False)
    #     .alias(replace_column)
    # )

    # Cast safe strings to floats
    lf_columns = lf.collect_schema().names()
    for column in POST2018_FLOAT_COLUMNS:
        if column in lf_columns:
            lf = lf.with_columns(
                pl.col(column)
                .cast(pl.Float64, strict=False)
                .alias(column)
            )
    for column in POST2018_INTEGER_COLUMNS:
        if column in lf_columns:
            lf = lf.with_columns(
                pl.col(column)
                .cast(pl.Int64, strict=False)
                .alias(column)
            )

    return lf


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


def _schema_name_for_dataset(dataset: str) -> str:
    """Return schema file base name for a given post-2018 dataset."""
    if dataset == "loans":
        return "hmda_lar_schema_post2018"
    if dataset == "panel":
        return "hmda_panel_schema_post2018"
    if dataset == "transmissal_series":
        return "hmda_ts_schema_post2018"
    raise ValueError(f"Unsupported dataset: {dataset}")


def build_bronze_post2018(
    dataset: Literal["loans", "panel", "transmissal_series"],
    min_year: int = 2018,
    max_year: int = 2024,
    replace: bool = False,
) -> None:
    """Create bronze layer parquet files for post-2018 data.

    Reads raw ZIPs from data/raw/<dataset>, extracts, detects delimiter, limits
    schema, adds file_type and (for loans) HMDAIndex, drops derived/tract columns,
    and writes one parquet per archive to data/bronze/<dataset>/post2018.
    """
    raw_folder = RAW_DIR / dataset
    bronze_folder = get_medallion_dir("bronze", dataset, "post2018")
    bronze_folder.mkdir(parents=True, exist_ok=True)

    # Determine schema and index settings per dataset
    schema_path = get_schema_path(_schema_name_for_dataset(dataset))
    add_hmda_index = dataset == "loans"

    for year in range(min_year, max_year + 1):
        archives_found = list(raw_folder.glob(f"*{year}*.zip"))
        if not archives_found:
            logger.debug("No archives found for year %s in %s", year, raw_folder)
            continue

        for archive in archives_found:
            file_name = normalized_file_stem(archive.stem)
            save_file = bronze_folder / f"{file_name}.parquet"

            # Check if we should process the raw file
            if not should_process_output(save_file, replace):
                logger.debug("Skipping existing bronze file: %s", save_file)
                continue

            logger.info("[bronze] Processing archive: %s", archive)

            # Extract and process the archive
            raw_file_path = Path(unzip_hmda_file(archive, archive.parent))
            try:
                # Detect delimiter and load schema
                delimiter = get_delimiter(raw_file_path, bytes=16000)
                schema = get_file_schema(schema_file=Path(schema_path), schema_type="polars")
                if not isinstance(schema, dict):
                    raise ValueError(f"Expected dict schema, got {type(schema)}")
                limited_schema = _limit_schema_to_available_columns(
                    raw_file_path,
                    delimiter,
                    schema,  # type: ignore[arg-type]
                )

                # Build lazyframe; add row index only when creating HMDAIndex
                if add_hmda_index:
                    lf = pl.scan_csv(
                        raw_file_path,
                        separator=delimiter,
                        low_memory=True,
                        row_index_name=HMDA_INDEX_COLUMN,
                        infer_schema_length=None,
                    )
                else:
                    lf = pl.scan_csv(
                        raw_file_path,
                        separator=delimiter,
                        low_memory=True,
                        infer_schema_length=None,
                    )

                # Add file_type and HMDAIndex if requested
                file_type_code = _get_file_type_code(archive)
                lf = lf.with_columns(pl.lit(file_type_code).alias("file_type"))
                if add_hmda_index:
                    lf = _append_hmda_index(lf, year, file_type_code)

                # Drop derived and tract columns only (no renames or destring here)
                lf = lf.drop(DERIVED_COLUMNS, strict=False)
                lf = lf.drop(POST2018_TRACT_COLUMNS, strict=False)

                # Write bronze file
                lf.sink_parquet(save_file)
                logger.debug("Saved bronze file: %s", save_file)

            finally:
                # Always remove extracted raw CSV to keep raw folder clean
                time.sleep(1)
                raw_file_path.unlink(missing_ok=True)


def build_silver_post2018(
    dataset: Literal["loans", "panel", "transmissal_series"],
    min_year: int = 2018,
    max_year: int = 2024,
    replace: bool = False,
) -> None:
    """Create hive-partitioned silver layer for post-2018 data.

    Processes bronze parquet files one-at-a-time, applies standard cleaning
    transforms with schema-guided typing, and writes to
    data/silver/<dataset>/post2018/activity_year=YYYY/file_type=X/.
    """
    bronze_folder = get_medallion_dir("bronze", dataset, "post2018")
    silver_folder = get_medallion_dir("silver", dataset, "post2018")
    silver_folder.mkdir(parents=True, exist_ok=True)

    # Optionally clear silver folder if replace requested
    if replace and silver_folder.exists():
        shutil.rmtree(silver_folder)
        silver_folder.mkdir(parents=True, exist_ok=True)

    # Load schema for consistent typing across files
    schema_path = get_schema_path(_schema_name_for_dataset(dataset))
    schema = get_file_schema(schema_file=Path(schema_path), schema_type="polars")
    if not isinstance(schema, dict):
        raise ValueError(f"Expected dict schema, got {type(schema)}")

    files_processed = 0
    for year in range(min_year, max_year + 1):
        for file in bronze_folder.glob(f"*{year}*.parquet"):
            lf = pl.scan_parquet(file, low_memory=True)

            # Apply renames and conversions
            if dataset == "loans":
                lf = _rename_columns_post2018(lf)
                lf = _destring_and_cast_hmda_cols_post2018(lf, schema)
                lf = _format_census_tract(lf)

            # Validate non-empty then write using hive partitioning
            if lf.limit(1).collect().height == 0:
                logger.warning("Skipping empty file: %s", file)
                continue

            lf.sink_parquet(
                pl.PartitionByKey(
                    silver_folder,
                    by=[pl.col("activity_year"), pl.col("file_type")],
                    include_key=True,
                ),
                mkdir=True,
            )
            files_processed += 1

    if files_processed == 0:
        logger.info("No bronze files found for %s in %s-%s", dataset, min_year, max_year)
