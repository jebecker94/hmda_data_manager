#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on: Saturday December 3, 2022
Last updated on: Wednesday May 21, 2025
@author: Jonathan E. Becker
"""

# Import Packages
import logging
import shutil
import time
from pathlib import Path
import polars as pl
import config
from import_support_functions import (
    destring_hmda_cols_2007_2017,
    destring_hmda_cols_after_2018,
    destring_hmda_cols_pre2007,
    get_delimiter,
    get_file_schema,
    rename_hmda_columns,
    unzip_hmda_file,
)


logger = logging.getLogger(__name__)


CENSUS_TRACT_COLUMN = "census_tract"
HMDA_INDEX_COLUMN = "HMDAIndex"
DERIVED_COLUMNS = [
    "derived_loan_product_type",
    "derived_race",
    "derived_ethnicity",
    "derived_sex",
    "derived_dwelling_category",
]
POST_2017_TRACT_COLUMNS = [
    "tract_population",
    "tract_minority_population_percent",
    "ffiec_msa_md_median_family_income",
    "tract_to_msa_income_percentage",
    "tract_owner_occupied_units",
    "tract_one_to_four_family_homes",
    "tract_median_age_of_housing_units",
]
PRE2018_TS_DROP_COLUMNS = [
    "Respondent Name (Panel)",
    "Respondent City (Panel)",
    "Respondent State (Panel)",
]


def _normalized_file_stem(stem: str) -> str:
    """Remove common suffixes from extracted archive names."""

    if stem.endswith("_csv"):
        stem = stem[:-4]
    if stem.endswith("_pipe"):
        stem = stem[:-5]
    return stem


def _should_process_output(path: Path, replace: bool) -> bool:
    """Return ``True`` when the target path should be generated."""

    return replace or not path.exists()


def _limit_schema_to_available_columns(
    raw_file: Path, delimiter: str, schema: dict[str, pl.DataType]
) -> dict[str, pl.DataType]:
    """Restrict a schema to the columns available in a delimited file."""

    csv_columns = pl.read_csv(raw_file, separator=delimiter, n_rows=0, ignore_errors=True).columns
    logger.debug("CSV columns: %s", csv_columns)
    return {column: schema[column] for column in csv_columns if column in schema}


def _append_hmda_index(
    lf: pl.LazyFrame, year: int, file_type_code: str
) -> pl.LazyFrame:
    """Format the HMDA index values as strings with a consistent prefix."""

    prefix = f"{year}{file_type_code}_"
    return (
        lf.cast({HMDA_INDEX_COLUMN: pl.String}, strict=False)
        .with_columns(pl.col(HMDA_INDEX_COLUMN).str.zfill(9).alias(HMDA_INDEX_COLUMN))
        .with_columns(
            (pl.lit(prefix) + pl.col(HMDA_INDEX_COLUMN)).alias(HMDA_INDEX_COLUMN)
        )
    )


def _build_hmda_lazyframe(
    raw_file: Path,
    delimiter: str,
    schema: dict[str, pl.DataType],
    year: int,
    add_hmda_index: bool,
    archive_path: Path,
    add_file_type: bool,
) -> pl.LazyFrame:
    """Create a ``polars`` lazy frame for a raw HMDA delimited file."""

    if (year < 2017) or (not add_hmda_index):
        return pl.scan_csv(
            raw_file, separator=delimiter, low_memory=True, schema=schema
        )

    # Scan CSV File
    lf = pl.scan_csv(
        raw_file,
        separator=delimiter,
        low_memory=True,
        row_index_name=HMDA_INDEX_COLUMN,
        infer_schema_length=None,
    )

    # Add File Type and HMDA Index
    file_type_code = _get_file_type_code(archive_path)
    if add_file_type:
        lf = lf.with_columns(pl.lit(file_type_code).alias("file_type"))
    if add_hmda_index:
        lf = _append_hmda_index(lf, year, file_type_code)

    return lf


def _process_hmda_archive(
    archive_path: Path,
    save_path: Path,
    schema_file: Path,
    year: int,
    remove_raw_file: bool,
    add_hmda_index: bool,
    add_file_type: bool,
) -> None:
    """Read, clean and persist a single HMDA archive."""

    raw_file_path = Path(unzip_hmda_file(archive_path, archive_path.parent))
    try:
        delimiter = get_delimiter(raw_file_path, bytes=16000)
        schema = get_file_schema(schema_file=schema_file, schema_type="polars")
        limited_schema = _limit_schema_to_available_columns(
            raw_file_path,
            delimiter,
            schema,
        )
        lf = _build_hmda_lazyframe(
            raw_file=raw_file_path,
            delimiter=delimiter,
            schema=limited_schema,
            year=year,
            add_hmda_index=add_hmda_index,
            add_file_type=add_file_type,
            archive_path=archive_path,
        )
        lf.sink_parquet(save_path)
    finally:
        if remove_raw_file:
            time.sleep(1)
            raw_file_path.unlink(missing_ok=True)


def _format_census_tract_lazy(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Standardize the census tract column within a lazy frame."""

    return (
        lf.cast({CENSUS_TRACT_COLUMN: pl.Float64}, strict=False)
        .cast({CENSUS_TRACT_COLUMN: pl.Int64}, strict=False)
        .cast({CENSUS_TRACT_COLUMN: pl.String}, strict=False)
        .with_columns(
            pl.col(CENSUS_TRACT_COLUMN).str.zfill(11).alias(CENSUS_TRACT_COLUMN)
        )
    )


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


def _rename_columns_post2018(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Rename columns for post-2018 files."""
    rename_dict = {
        "loan_to_value_ratio": "combined_loan_to_value_ratio",
    }
    return lf.rename(rename_dict, strict=False)

# %% Import Functions
# Import Historic HMDA Files (Still Needs Work)
def import_hmda_pre_2007(
    data_folder: Path,
    save_folder: Path,
    min_year: int = 1981,
    max_year: int = 2006,
    contains_string: str = "HMDA_LAR",
):
    """
    Import and clean HMDA data before 2007.

    Parameters
    ----------
    data_folder : Path
        Folder containing raw HMDA text files.
    save_folder : Path
        Directory where cleaned files will be written.
    contains_string : str, optional
        Substring used to identify HMDA files to process. The default is 'HMDA_LAR'.

    Returns
    -------
    None.

    """

    data_folder = Path(data_folder)
    save_folder = Path(save_folder)
    # Loop Over Years
    for year in range(min_year, max_year + 1):
        # Get Files
        for file in data_folder.glob(f"*{year}*.txt"):
            # Get File Name
            file_name = file.stem
            save_file_csv = save_folder / f"{file_name}.csv.gz"
            save_file_parquet = save_folder / f"{file_name}.parquet"

            # Read File
            if not save_file_parquet.exists():
                # Load Raw Data
                logger.info("Reading file: %s", file)
                df = pl.read_csv(file, separator=get_delimiter(file, bytes=16000), ignore_errors=True)

                # Rename Columns
                df = rename_hmda_columns(df, df_type="polars")

                # Destring Numeric Columns
                df = destring_hmda_cols_pre2007(df)

                # Save to Parquet
                df.write_parquet(save_file_parquet)


# Import Data w/ Streaming
def import_hmda_streaming(
    data_folder: Path,
    save_folder: Path,
    schema_file: Path,
    min_year: int = 2007,
    max_year: int = 2024,
    replace: bool = False,
    remove_raw_file: bool = True,
    add_hmda_index: bool = True,
    add_file_type: bool = True,
):
    """
    Import and clean HMDA data for 2007 onward.

    Also adds HMDAIndex to HMDA files from 2018 onward. The HMDAIndex is a unique identifier
    for each loan application record consisting of:
    - Activity year (4 digits)
    - File type code (1 character)
    - Row number (9 digits, zero padded)
    For example: 2018a_000000000

    Parameters
    ----------
    data_folder : Path
        Folder where raw data is stored.
    save_folder : Path
        Folder where cleaned data will be saved.
    min_year : int, optional
        First year of data to include. The default is 2007.
    max_year : int, optional
        Last year of data to include. The default is 2024.
    replace : bool, optional
        Whether to replace existing files. The default is False.
    remove_raw_file : bool, optional
        Whether to remove the raw file after processing. The default is True.
    add_hmda_index : bool, optional
        Whether to add the HMDAIndex to the data starting in 2017. The default is True.
    add_file_type : bool, optional
        Whether to add the file type to the data. The default is True.
    Returns
    -------
    None.

    """

    data_folder = Path(data_folder)
    save_folder = Path(save_folder)
    save_folder.mkdir(parents=True, exist_ok=True)
    schema_file = Path(schema_file)

    for year in range(min_year, max_year + 1):
        for archive in data_folder.glob(f"*{year}*.zip"):
            file_name = _normalized_file_stem(archive.stem)
            save_file = save_folder / f"{file_name}.parquet"

            if not _should_process_output(save_file, replace):
                continue

            logger.info("Reading file: %s", archive)
            _process_hmda_archive(
                archive_path=archive,
                save_path=save_file,
                schema_file=schema_file,
                year=year,
                remove_raw_file=remove_raw_file,
                add_hmda_index=add_hmda_index,
                add_file_type=add_file_type,
            )


# Clean Data After 2017
def clean_hmda_post_2017(
    data_folder: Path, min_year: int = 2018, max_year: int = 2024, overwrite: bool = False
):
    """
    Import and clean HMDA data for 2018 onward.

    Parameters
    ----------
    data_folder : Path
        Folder where parquet files are stored.
    min_year : int, optional
        First year of data to include. The default is 2018.
    max_year : int, optional
        Last year of data to include. The default is 2024.
    overwrite : bool, optional
        Whether to overwrite existing files. The default is False.

    Returns
    -------
    None.

    """

    data_folder = Path(data_folder)

    for year in range(min_year, max_year + 1):
        for file in data_folder.glob(f"*{year}*.parquet"):
            save_file_parquet = file.with_name(f"{file.stem}_clean.parquet")

            if not _should_process_output(save_file_parquet, overwrite):
                continue

            # Clean File
            lf = pl.scan_parquet(file, low_memory=True)
            lf = lf.drop(DERIVED_COLUMNS, strict=False)
            lf = lf.drop(POST_2017_TRACT_COLUMNS, strict=False)
            lf = _rename_columns_post2018(lf)
            lf = destring_hmda_cols_after_2018(lf)
            lf = _format_census_tract_lazy(lf)
            lf.sink_parquet(save_file_parquet)

            # Move Cleaned File to Original File
            if overwrite:
                shutil.move(save_file_parquet, file)


# Clean Historic HMDA Files (2007-2017)
def clean_hmda_2007_2017(
    data_folder: Path, min_year: int = 2007, max_year: int = 2017, replace: bool = False
):
    """
    Import and clean HMDA data for 2007-2017.

    Parameters
    ----------
    data_folder : Path
        Folder containing HMDA parquet files.
    min_year : int, optional
        First year to process. The default is 2007.
    max_year : int, optional
        Last year to process. The default is 2017.
    replace : bool, optional
        Overwrite existing cleaned files if True. The default is False.

    Returns
    -------
    None.

    """

    data_folder = Path(data_folder)

    for year in range(min_year, max_year + 1):
        files = list(data_folder.glob(f"*{year}*records*.parquet")) + list(
            data_folder.glob(f"*{year}*public*.parquet")
        )
        for file in files:
            save_file_parquet = file.with_name(f"{file.stem}_clean.parquet")

            if not _should_process_output(save_file_parquet, replace):
                continue

            _clean_2007_2017_file(file, save_file_parquet, year)


#%% Save Functions
# Save to dataset
def save_to_dataset(
    data_folder: Path,
    save_folder: Path,
    min_year: int = 2018,
    max_year: int = 2024,
):
    """Save HMDA data to dataset with Hive Partitioning."""

    data_folder = Path(data_folder)
    save_folder = Path(save_folder)

    df = []
    years = range(min_year, max_year + 1)
    for year in years:
        for file in data_folder.glob(f"*{year}*.parquet"):
            df_a = pl.scan_parquet(file)
            df.append(df_a)
    df = pl.concat(df, how='diagonal_relaxed')
    df.sink_parquet(
        pl.PartitionByKey(
            save_folder / "{key[0].name}={key[0].value}/{key[1].name}={key[1].value}/000.parquet",
            by=[pl.col('activity_year'), pl.col('file_type')],
            include_key=True,
        ),
        mkdir=True,
    )


# %% Main Routine
if __name__ == "__main__":

    # Define Folder Paths
    RAW_DIR = config.RAW_DIR
    CLEAN_DIR = config.CLEAN_DIR
    PROJECT_DIR = config.PROJECT_DIR

    # Set Year Ranges
    MIN_YEAR = 2018
    MAX_YEAR = 2024

    # Import HMDA Loan Data
    import_hmda_streaming(
        RAW_DIR / "loans",
        CLEAN_DIR / "loans",
        PROJECT_DIR / "schemas/hmda_lar_schema_post2018.html",
        min_year=MIN_YEAR,
        max_year=MAX_YEAR
    )

    # Import HMDA Transmittal Series Data
    import_hmda_streaming(
        RAW_DIR / "transmissal_series",
        CLEAN_DIR / "transmissal_series",
        PROJECT_DIR / "schemas/hmda_ts_schema_post2018.html",
        min_year=MIN_YEAR,
        max_year=MAX_YEAR
    )

    # Import HMDA Panel Data
    import_hmda_streaming(
        RAW_DIR / "panel",
        CLEAN_DIR / "panel",
        PROJECT_DIR / "schemas/hmda_panel_schema_post2018.html",
        min_year=MIN_YEAR,
        max_year=MAX_YEAR
    )

    # Clean loans data
    clean_hmda_post_2017(
        CLEAN_DIR / "loans",
        min_year=MIN_YEAR,
        max_year=MAX_YEAR,
        overwrite=True,
    )

    # Combine Lender Files
    ts_folder = CLEAN_DIR / "transmissal_series"
    panel_folder = CLEAN_DIR / "panel"
    save_folder = PROJECT_DIR / "data"
    # combine_lenders_panel_ts_pre2018(panel_folder, ts_folder, save_folder, min_year=2007, max_year=2017)
    # combine_lenders_panel_ts_post2018(panel_folder, ts_folder, save_folder, min_year=2018, max_year=2023)

    # # Save to Dataset
    # save_to_dataset(
    #     CLEAN_DIR / 'loans',
    #     PROJECT_DIR / "data/database/loans",
    #     min_year=2018,
    #     max_year=2024,
    # )
