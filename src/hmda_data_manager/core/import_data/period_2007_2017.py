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
from collections.abc import Sequence
from pathlib import Path
import polars as pl
import pandas as pd
from typing import Literal
from ...utils.io import get_delimiter, unzip_hmda_file
from ...utils.schema import get_file_schema, rename_hmda_columns
from ...schemas import get_schema_path
from ..config import RAW_DIR, get_medallion_dir

logger = logging.getLogger(__name__)


# Constants specific to 2007-2017 data
# Tract summary variables to optionally drop in silver layer
PERIOD_2007_2017_TRACT_COLUMNS = [
    "tract_population",
    "tract_minority_population_percent", 
    "ffiec_msa_md_median_family_income",
    "tract_to_msa_income_percentage",
    "tract_owner_occupied_units",
    "tract_one_to_four_family_units",
    "tract_median_age_of_housing_units",
]

# Integer columns that need consistent Int64 casting (based on audit findings)
PERIOD_2007_2017_INTEGER_COLUMNS = [
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
    "denial_reason_1",
    "denial_reason_2",
    "denial_reason_3",
    "denial_reason_4",
    "edit_status",
    "sequence_number",
    "application_date_indicator",
]

# Float columns that should be cast to Float64 for consistency
PERIOD_2007_2017_FLOAT_COLUMNS = [
    "rate_spread",
    "tract_minority_population_percent",
    "ffiec_msa_md_median_family_income", 
    "tract_to_msa_income_percentage",
    "tract_owner_occupied_units",
    "tract_one_to_four_family_units",
    "tract_median_age_of_housing_units",
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


def limit_schema_to_available_columns(
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


# ----------------------------
# Medallion: Bronze Builders
# ----------------------------

def _schema_name_for_dataset_2007_2017(dataset: str) -> str:
    """Return schema file base name for a given 2007–2017 dataset.

    Currently only loans are supported for this period.
    """
    if dataset == "loans":
        return "hmda_lar_schema_2007-2017"
    raise ValueError(f"Unsupported dataset for 2007-2017: {dataset}")


def build_bronze_period_2007_2017(
    dataset: Literal["loans"],
    min_year: int = 2007,
    max_year: int = 2017,
    replace: bool = False,
) -> None:
    """Create bronze parquet files for 2007–2017 data.

    - Reads raw ZIPs from data/raw/<dataset>
    - Extracts, detects delimiter, limits schema
    - Writes one parquet per archive to data/bronze/<dataset>/period_2007_2017
    """
    raw_folder = RAW_DIR / dataset
    bronze_folder = get_medallion_dir("bronze", dataset, "period_2007_2017")
    bronze_folder.mkdir(parents=True, exist_ok=True)

    schema_path = get_schema_path(_schema_name_for_dataset_2007_2017(dataset))

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

                lf = pl.scan_csv(
                    raw_file_path,
                    separator=delimiter,
                    low_memory=True,
                    infer_schema_length=None,
                )

                # Keep bronze minimal: no renames, no derived handling
                lf.sink_parquet(save_file)
            finally:
                time.sleep(1)
                raw_file_path.unlink(missing_ok=True)


# ----------------------------
# Medallion: Silver Builders
# ----------------------------

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
    
    Handles string values like "NA", "N/A", "Not applicable" by converting them
    to null, then casts integer columns to Int64 and float columns to Float64.
    
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
    
    # Cast float columns to Float64 with robust NA handling
    for col in lf.columns:
        if col in PERIOD_2007_2017_FLOAT_COLUMNS:
            col_dtype = lf.schema[col]
            if col_dtype == pl.String:
                # Handle string columns with NA values
                lf = lf.with_columns(
                    pl.when(
                        pl.col(col).is_in(["NA", "N/A", "Not applicable", "Not Applicable", "na", "n/a", ""])
                    )
                    .then(None)
                    .otherwise(pl.col(col))
                    .cast(pl.Float64, strict=False)
                    .alias(col)
                )
            else:
                # Already numeric, just cast to ensure Float64
                lf = lf.with_columns(pl.col(col).cast(pl.Float64, strict=False).alias(col))
    
    # Cast integer columns to Int64 with robust NA handling
    for col in lf.columns:
        if col in PERIOD_2007_2017_INTEGER_COLUMNS:
            col_dtype = lf.schema[col]
            if col_dtype == pl.String:
                # Handle string columns with NA values
                lf = lf.with_columns(
                    pl.when(
                        pl.col(col).is_in(["NA", "N/A", "Not applicable", "Not Applicable", "na", "n/a", ""])
                    )
                    .then(None)
                    .otherwise(pl.col(col))
                    .cast(pl.Int64, strict=False)
                    .alias(col)
                )
            else:
                # Already numeric, just cast to ensure Int64
                lf = lf.with_columns(pl.col(col).cast(pl.Int64, strict=False).alias(col))
    
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

    schema_path = get_schema_path(_schema_name_for_dataset_2007_2017(dataset))
    schema = get_file_schema(schema_file=Path(schema_path), schema_type="polars")
    if not isinstance(schema, dict):
        raise ValueError(f"Expected dict schema, got {type(schema)}")

    files_processed = 0
    for year in range(min_year, max_year + 1):
        for file in sorted(bronze_folder.glob(f"*{year}*.parquet")):
            lf = pl.scan_parquet(file, low_memory=True)

            # Standardize column names (legacy -> modern)
            lf = _rename_columns_period_2007_2017(lf)

            # Destring and cast integer columns to consistent Int64 types
            lf = _destring_and_cast_hmda_cols_2007_2017(lf)

            # Drop derived columns that only appear in some files (inconsistent)
            derived_cols_to_drop = [
                "derived_dwelling_category",
                "derived_ethnicity", 
                "derived_loan_product_type",
                "derived_race",
                "derived_sex",
            ]
            lf = lf.drop(derived_cols_to_drop, strict=False)

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
            files_processed += 1

    if files_processed == 0:
        logger.info(
            "No bronze files found for %s in %s-%s at %s",
            dataset,
            min_year,
            max_year,
            bronze_folder,
        )
