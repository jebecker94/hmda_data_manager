"""
HMDA Import Functions for Pre-2007 Data (1981-2006)

This module handles import and processing of legacy HMDA data files from 1981-2006.
These files have a simpler format and different column structure compared to later
HMDA data releases.

Key Characteristics:
- Simple delimited text format
- Basic loan application fields
- No derived columns or tract variables
- Different column naming conventions

This module is relatively stable since the legacy data format is unlikely to change.
"""

import logging
from typing import Literal
import polars as pl
from ...utils.io import get_delimiter, should_process_output
from ..config import RAW_DIR, get_medallion_dir, RENAME_DICTIONARY


logger = logging.getLogger(__name__)


def build_bronze_pre2007(
    dataset: Literal["loans", "panel", "transmissal_series"],
    min_year: int = 1990,
    max_year: int = 2006,
    replace: bool = False,
) -> None:
    """Create bronze layer parquet files for pre-2007 data.

    Reads raw ZIPs, extracts TXT files, and writes one parquet per year to
    data/bronze/<dataset>/pre2007/. All columns are kept as strings for
    maximum data preservation - type conversions happen in silver layer.

    Parameters
    ----------
    dataset : {"loans", "panel", "transmissal_series"}
        Dataset type to process
    min_year : int
        First year to process (default: 1990, skips 1981-1989 aggregates)
    max_year : int
        Last year to process (default: 2006)
    replace : bool
        Whether to replace existing bronze files (default: False)
    """
    import subprocess
    import time

    # Determine raw folder and archives based on dataset
    if dataset == "loans":
        raw_folder = RAW_DIR / "loans"
    elif dataset == "panel":
        raw_folder = RAW_DIR
    elif dataset == "transmissal_series":
        raw_folder = RAW_DIR
    else:
        raise ValueError(f"Unknown dataset: {dataset}")

    bronze_folder = get_medallion_dir("bronze", dataset, "pre2007")
    bronze_folder.mkdir(parents=True, exist_ok=True)

    for year in range(min_year, max_year + 1):
        # Determine archive path based on dataset and year
        if dataset == "loans":
            if 1981 <= year <= 1989:
                archive = raw_folder / "HMDA_LAR_1981_1989.zip"
            else:
                archive = raw_folder / f"HMDA_LAR_{year}.zip"
        elif dataset == "panel":
            archive = raw_folder / "HMDA_PANEL.zip"
        elif dataset == "transmissal_series":
            archive = raw_folder / "HMDA_TS.zip"

        if not archive.exists():
            logger.debug("Archive not found for year %s: %s", year, archive)
            continue

        # Output filename
        save_file = bronze_folder / f"{dataset}_{year}.parquet"
        if not should_process_output(save_file, replace):
            logger.debug("Skipping existing bronze file: %s", save_file)
            continue

        logger.info("[bronze pre2007] Processing %s year %s", dataset, year)

        # Get list of files in archive
        result = subprocess.run(
            ["unzip", "-l", str(archive)],
            capture_output=True,
            text=True,
            check=True
        )

        # Find TXT file for this year
        txt_file = None
        for line in result.stdout.split('\n'):
            if '.txt' in line.lower():
                filename = line.split()[-1]
                if '/' not in filename and str(year) in filename:
                    txt_file = filename
                    break

        if not txt_file:
            logger.warning("No TXT file found for year %s in %s", year, archive)
            continue

        logger.info("Extracting: %s", txt_file)

        # Extract using system unzip
        temp_path = raw_folder / txt_file
        subprocess.run(
            ["unzip", "-o", str(archive), txt_file, "-d", str(raw_folder)],
            check=True,
            capture_output=True
        )

        try:
            # Detect delimiter
            delimiter = get_delimiter(temp_path, bytes=16000)
            logger.info("Detected delimiter: %r", delimiter)

            # Load data with ALL COLUMNS AS STRINGS
            logger.info("Loading data (all columns as strings)...")
            df = pl.read_csv(
                temp_path,
                separator=delimiter,
                ignore_errors=True,
                infer_schema=False,  # Force all columns to String type
                encoding="utf8-lossy",  # Handle invalid UTF-8 sequences
            )

            logger.info("Loaded %d rows, %d columns", len(df), len(df.columns))

            # Save to parquet
            df.write_parquet(save_file)
            logger.info("Saved bronze file: %s", save_file)

        finally:
            # Clean up extracted file
            time.sleep(1)
            if temp_path.exists():
                temp_path.unlink()
                logger.debug("Cleaned up: %s", temp_path)


def _standardize_geographic_codes(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Standardize geographic codes for pre-2007 data.

    Creates properly formatted FIPS codes and MSA/MD codes:
    - state_code: 2-digit string with leading zeros
    - county_code: 5-digit string (state + county) with leading zeros
    - census_tract: 11-digit string (state + county + tract) with leading zeros
    - msa_md: 5-digit string with leading zeros

    Pre-2007 format:
    - state_code: numeric 1-2 digits
    - county_code: numeric 3 digits (county within state)
    - census_tract: string format "####.##" (e.g., "9509.02")
    - msa_md: numeric variable length

    Post-standardization format:
    - state_code: "01" to "56" (2-digit FIPS)
    - county_code: "01001" (state + county, 5-digit)
    - census_tract: "01001950902" (state + county + tract, 11-digit)
    - msa_md: "01234" (5-digit MSA/MD code)

    Parameters
    ----------
    lf : pl.LazyFrame
        Input lazy frame with geographic codes as strings

    Returns
    -------
    pl.LazyFrame
        Lazy frame with standardized geographic codes
    """

    # Step 1: Standardize state_code to 2-digit string with leading zeros
    lf = lf.with_columns(
        pl.col("state_code")
        .cast(pl.Int64, strict=False)  # Convert to int first
        .cast(pl.String)  # Then to string
        .str.zfill(2)  # Pad to 2 digits
        .alias("state_code")
    )

    # Step 2: Create 5-digit county_code (state + county)
    # Bronze county_code is 3-digit county within state
    lf = lf.with_columns(
        # Combine state (2-digit) + county (3-digit)
        (
            pl.col("state_code") +
            pl.col("county_code")
            .cast(pl.Int64, strict=False)
            .cast(pl.String)
            .str.zfill(3)
        ).alias("county_code")
    )

    # Step 3: Standardize census_tract to 11-digit string
    # Format: "####.##" -> convert to "01001950902"
    # Formula: state (2) + county (3) + tract*100 (6 digits)
    if "census_tract" in lf.collect_schema().names():
        lf = lf.with_columns(
            (
                # Take state_code (2-digit) + county last 3 digits + tract (6-digit)
                pl.col("state_code") +
                pl.col("county_code").str.slice(-3, 3) +
                pl.col("census_tract")
                .cast(pl.Float64, strict=False)
                .mul(100)
                .round(0)
                .cast(pl.Int64, strict=False)
                .cast(pl.String)
                .str.zfill(6)
            ).alias("census_tract")
        )

    # Step 4: Standardize msa_md to 5-digit string with leading zeros
    if "msa_md" in lf.collect_schema().names():
        lf = lf.with_columns(
            pl.col("msa_md")
            .cast(pl.Float64, strict=False)
            .cast(pl.Int64, strict=False)
            .cast(pl.String)
            .str.zfill(5)
            .alias("msa_md")
        )

    return lf


def _harmonize_schema_pre2007(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Harmonize schema for pre-2007 data - type conversions and standardization.

    This function performs type conversions from bronze (all strings) to silver
    (appropriate types for analysis):

    Numeric conversions (string -> integer):
    - activity_year, occupancy_type, edit_status, sequence_number: int
    - loan_amount: int (multiplied by 1000 to get actual dollar amount)
    - income: int (multiplied by 1000 to get actual dollar amount)

    String standardization:
    - msa_md: 5-digit string with leading zeros (like county_code)

    Geographic codes (handled separately by _standardize_geographic_codes):
    - state_code: 2-digit string with leading zeros
    - county_code: 5-digit string (state + county)
    - census_tract: 11-digit string (state + county + tract)

    Columns NOT converted (kept as strings due to non-numeric values):
    - respondent_id: contains Tax ID formats like "95-2318940"
    - agency_code: contains letters (C, E, D, B)
    - Other categorical variables with non-numeric codes

    Parameters
    ----------
    lf : pl.LazyFrame
        Input lazy frame with all columns as strings

    Returns
    -------
    pl.LazyFrame
        Lazy frame with harmonized schema
    """
    from ..config import PRE2007_INTEGER_COLUMNS, PRE2007_FLOAT_COLUMNS

    lf_columns = lf.collect_schema().names()

    # Convert integer columns (except loan_amount and income which need special handling)
    # Use strict=False to convert non-numeric values to null (treats stray characters as errors)
    integer_cols_to_convert = [
        col for col in PRE2007_INTEGER_COLUMNS
        if col in lf_columns and col not in ["loan_amount", "income"]
    ]

    if integer_cols_to_convert:
        lf = lf.with_columns([
            pl.col(col).cast(pl.Int64, strict=False).alias(col)
            for col in integer_cols_to_convert
        ])

    # Convert float columns
    float_cols_to_convert = [
        col for col in PRE2007_FLOAT_COLUMNS
        if col in lf_columns
    ]

    if float_cols_to_convert:
        lf = lf.with_columns([
            pl.col(col).cast(pl.Float64, strict=False).alias(col)
            for col in float_cols_to_convert
        ])

    # Convert loan_amount: string -> int -> multiply by 1000
    if "loan_amount" in lf_columns and "loan_amount" in PRE2007_INTEGER_COLUMNS:
        lf = lf.with_columns(
            pl.col("loan_amount")
            .cast(pl.Int64, strict=False)
            .mul(1000)
            .alias("loan_amount")
        )

    # Convert income: string -> int -> multiply by 1000
    if "income" in lf_columns and "income" in PRE2007_INTEGER_COLUMNS:
        lf = lf.with_columns(
            pl.col("income")
            .cast(pl.Int64, strict=False)
            .mul(1000)
            .alias("income")
        )

    return lf


def build_silver_pre2007(
    dataset: Literal["loans", "panel", "transmissal_series"],
    min_year: int = 1990,
    max_year: int = 2006,
    replace: bool = False,
) -> None:
    """Create silver layer parquet files for pre-2007 data.

    Reads bronze parquet files, applies schema harmonization (type conversions),
    geographic code standardization, and column renaming. Outputs Hive-partitioned
    parquet files to data/silver/<dataset>/pre2007/.

    Transformations applied:
    - Integer conversions: 31 columns (activity_year, loan_amount*1000, income*1000,
      loan_type, loan_purpose, action_taken, demographics, denial_reasons, etc.)
    - Float conversions: rate_spread
    - Geographic standardization: state_code (2-digit), county_code (5-digit),
      census_tract (11-digit), msa_md (5-digit)
    - Column renaming: occupancy -> occupancy_type, msamd -> msa_md

    Parameters
    ----------
    dataset : {"loans", "panel", "transmissal_series"}
        Dataset type to process
    min_year : int
        First year to process (default: 1990)
    max_year : int
        Last year to process (default: 2006)
    replace : bool
        Whether to replace existing silver files (default: False)
    """
    from ..config import get_medallion_dir

    bronze_folder = get_medallion_dir("bronze", dataset, "pre2007")
    silver_folder = get_medallion_dir("silver", dataset, "pre2007")
    silver_folder.mkdir(parents=True, exist_ok=True)

    for year in range(min_year, max_year + 1):
        bronze_file = bronze_folder / f"{dataset}_{year}.parquet"

        if not bronze_file.exists():
            logger.debug("Bronze file not found for year %s: %s", year, bronze_file)
            continue

        # Output filename (year-based partitioning)
        save_file = silver_folder / f"activity_year={year}" / f"{dataset}_{year}.parquet"

        if not should_process_output(save_file, replace):
            logger.debug("Skipping existing silver file: %s", save_file)
            continue

        logger.info("[silver pre2007] Processing %s year %s", dataset, year)

        # Load bronze as LazyFrame
        lf = pl.scan_parquet(bronze_file)

        # Apply column renames (only renames columns that exist)
        existing_cols = lf.collect_schema().names()
        renames_to_apply = {
            old: new for old, new in RENAME_DICTIONARY.items() if old in existing_cols
        }
        if renames_to_apply:
            logger.debug(
                "Renaming %d columns: %s", len(renames_to_apply), renames_to_apply
            )
            lf = lf.rename(renames_to_apply)

        # Apply transformations
        lf = _harmonize_schema_pre2007(lf)
        lf = _standardize_geographic_codes(lf)

        # Create output directory
        save_file.parent.mkdir(parents=True, exist_ok=True)

        # Write to parquet
        lf.sink_parquet(save_file)
        logger.info("Saved silver file: %s", save_file)


