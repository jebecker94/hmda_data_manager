"""
HMDA Import Functions for 2007-2017 Data

This module handles import and processing of HMDA data files from 2007-2017.
This period represents the standardized HMDA format before the major changes
introduced in 2018.

Key Characteristics:
- Standardized zip archive format
- Consistent schema across years
- No HMDA index (introduced in 2018)
- Uses destring_hmda_cols_2007_2017 processing
- Includes tract variables that need to be handled

This module is relatively stable since the 2007-2017 data format is finalized.
"""

import logging
import time
from pathlib import Path
from typing import Literal

import polars as pl

from ...utils.io import (
    get_delimiter,
    unzip_hmda_file,
    normalized_file_stem,
    should_process_output,
)
from ..config import (
    RAW_DIR,
    get_medallion_dir,
    DERIVED_COLUMNS,
    PERIOD_2007_2017_TRACT_COLUMNS,
    PERIOD_2007_2017_INTEGER_COLUMNS,
    PERIOD_2007_2017_FLOAT_COLUMNS,
)

logger = logging.getLogger(__name__)


def _rename_columns_period_2007_2017(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Rename columns for 2007-2017 files to standardize naming.
    
    Handles the inconsistency where 1 file uses modern names while 11 files
    use legacy names. This maps legacy -> modern standardized names.
    
    Parameters
    ----------
    lf : pl.LazyFrame
        Input lazy frame
        
    Returns
    -------
    pl.LazyFrame
        Lazy frame with standardized column names
    """
    rename_dict = {
        # Legacy -> Modern standardized names
        "as_of_year": "activity_year",
        "applicant_income_000s": "income", 
        "loan_amount_000s": "loan_amount",
        "census_tract_number": "census_tract",
        "owner_occupancy": "occupancy_type",
        "msamd": "msa_md",
        "population": "tract_population",
        "minority_population": "tract_minority_population_percent",
        "hud_median_family_income": "ffiec_msa_md_median_family_income",
        "tract_to_msamd_income": "tract_to_msa_income_percentage",
        "number_of_owner_occupied_units": "tract_owner_occupied_units",
        "number_of_1_to_4_family_units": "tract_one_to_four_family_units",
    }
    
    # Only rename columns that actually exist in this file
    existing_renames = {old: new for old, new in rename_dict.items() if old in lf.columns}
    
    if existing_renames:
        logger.debug("Renaming %d columns: %s", len(existing_renames), existing_renames)
        return lf.rename(existing_renames)
    else:
        logger.debug("No columns to rename (already using modern names)")
        return lf


def _destring_and_cast_hmda_cols_2007_2017(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Destring numeric HMDA variables and cast to consistent types.

    Casts integer columns to Int64 and float columns to Float64.
    Non-numeric strings are automatically converted to null via strict=False.

    Parameters
    ----------
    lf : pl.LazyFrame
        HMDA data with potentially mixed string/numeric columns

    Returns
    -------
    pl.LazyFrame
        LazyFrame with consistent Int64/Float64 types for numeric columns
    """
    logger.info("Destringing HMDA variables and casting to consistent types (2007-2017)")

    # Cast float columns to Float64
    float_cols_to_cast = [col for col in lf.columns if col in PERIOD_2007_2017_FLOAT_COLUMNS]
    if float_cols_to_cast:
        lf = lf.with_columns([
            pl.col(col).cast(pl.Float64, strict=False).alias(col)
            for col in float_cols_to_cast
        ])

    # Cast integer columns to Int64
    int_cols_to_cast = [col for col in lf.columns if col in PERIOD_2007_2017_INTEGER_COLUMNS]
    if int_cols_to_cast:
        lf = lf.with_columns([
            pl.col(col).cast(pl.Int64, strict=False).alias(col)
            for col in int_cols_to_cast
        ])

    return lf


def _infer_pre2018_file_type_from_name(name: str) -> int:
    """Infer a simple file_type code from a pre-2018 filename.

    1 = single-year public LAR (default)
    3 = three-year aggregate file (if 'three_year' present)
    """
    lower = name.lower()
    if "three_year" in lower:
        return 'a'
    elif "one_year" in lower:
        return 'b'
    elif "public_lar" in lower:
        return 'c'
    elif "nationwide" in lower:
        return 'd'
    elif "mlar" in lower:
        return 'e'
    else:
        raise ValueError(f"Unknown file type: {name}")


def build_bronze_period_2007_2017(
    dataset: Literal["loans"],
    min_year: int = 2007,
    max_year: int = 2017,
    replace: bool = False,
) -> None:
    """Create bronze parquet files for 2007–2017 data.

    - Reads raw ZIPs from data/raw/<dataset>
    - Extracts, detects delimiter, loads all columns as strings
    - Writes one parquet per archive to data/bronze/<dataset>/period_2007_2017

    All columns are stored as strings in bronze to preserve raw values and
    enable inspection/validation before silver layer type conversions.
    """
    raw_folder = RAW_DIR / dataset
    bronze_folder = get_medallion_dir("bronze", dataset, "period_2007_2017")
    bronze_folder.mkdir(parents=True, exist_ok=True)

    for year in range(min_year, max_year + 1):
        archives = sorted(raw_folder.glob(f"*{year}*.zip"))
        if not archives:
            logger.debug("No raw archives found for %s %s", dataset, year)
            continue

        for archive in archives:
            file_stem = normalized_file_stem(archive.stem)
            save_file = bronze_folder / f"{file_stem}.parquet"
            if not should_process_output(save_file, replace):
                logger.debug("Skipping existing bronze file: %s", save_file)
                continue

            logger.info("[bronze 2007-2017] Processing %s", archive)
            raw_file_path = Path(unzip_hmda_file(archive, archive.parent))
            try:
                delimiter = get_delimiter(raw_file_path, bytes=16000)

                # Load all columns as strings (bronze = raw data preservation)
                lf = pl.scan_csv(
                    raw_file_path,
                    separator=delimiter,
                    low_memory=True,
                    infer_schema=False,  # Force all columns to String type
                )

                # Keep bronze minimal: no renames, no derived handling
                lf.sink_parquet(save_file)
            finally:
                time.sleep(1)
                raw_file_path.unlink(missing_ok=True)


def build_silver_period_2007_2017(
    dataset: Literal["loans"],
    min_year: int = 2007,
    max_year: int = 2017,
    replace: bool = False,
    drop_tract_vars: bool = True,
) -> None:
    """Create hive-partitioned silver layer for 2007–2017 data.

    Processes bronze parquet files one-at-a-time, applies light standardization
    (renames and integer-friendly casting), and writes to
    data/silver/<dataset>/period_2007_2017/activity_year=YYYY/file_type=X/.
    
    Parameters
    ----------
    dataset : {"loans"}
        Dataset to process
    min_year : int
        First year to process
    max_year : int
        Last year to process  
    replace : bool
        Whether to replace existing silver files
    drop_tract_vars : bool
        Whether to drop tract summary variables (default True)
    """
    bronze_folder = get_medallion_dir("bronze", dataset, "period_2007_2017")
    silver_folder = get_medallion_dir("silver", dataset, "period_2007_2017")
    silver_folder.mkdir(parents=True, exist_ok=True)

    if replace and silver_folder.exists():
        # remove existing silver outputs for a clean rebuild
        import shutil

        shutil.rmtree(silver_folder)
        silver_folder.mkdir(parents=True, exist_ok=True)

    for year in range(min_year, max_year + 1):
        for file in sorted(bronze_folder.glob(f"*{year}*.parquet")):
            lf = pl.scan_parquet(file, low_memory=True)

            # Standardize column names (legacy -> modern)
            lf = _rename_columns_period_2007_2017(lf)

            # Destring and cast integer columns to consistent Int64 types
            lf = _destring_and_cast_hmda_cols_2007_2017(lf)

            # Drop derived columns that only appear in some files (inconsistent)
            lf = lf.drop(DERIVED_COLUMNS, strict=False)

            # Optionally drop tract summary variables
            if drop_tract_vars:
                lf = lf.drop(PERIOD_2007_2017_TRACT_COLUMNS, strict=False)

            # Ensure partition keys exist
            if "activity_year" not in lf.collect().names():
                lf = lf.with_columns(pl.lit(year).alias("activity_year"))

            file_type_code = _infer_pre2018_file_type_from_name(file.name)
            lf = lf.with_columns(pl.lit(file_type_code).alias("file_type"))

            # Validate and write using hive partitioning
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