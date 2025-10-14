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
from collections.abc import Iterable
from pathlib import Path

import numpy as np
import pandas as pd
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
from pyarrow import csv
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
import HMDALoader


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

    csv_columns = pl.read_csv(raw_file, separator=delimiter, n_rows=0).columns
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
) -> pl.LazyFrame:
    """Create a ``polars`` lazy frame for a raw HMDA delimited file."""

    if (year < 2017) or (not add_hmda_index):
        return pl.scan_csv(
            raw_file, separator=delimiter, low_memory=True, schema=schema
        )

    lf = pl.scan_csv(
        raw_file,
        separator=delimiter,
        low_memory=True,
        row_index_name=HMDA_INDEX_COLUMN,
        infer_schema_length=None,
    )
    file_type_code = HMDALoader.get_file_type_code(archive_path)
    return _append_hmda_index(lf, year, file_type_code)


def _process_hmda_archive(
    archive_path: Path,
    save_path: Path,
    schema_file: Path,
    year: int,
    remove_raw_file: bool,
    add_hmda_index: bool,
) -> None:
    """Read, clean and persist a single HMDA archive."""

    raw_file_path = Path(unzip_hmda_file(archive_path, archive_path.parent))
    try:
        delimiter = get_delimiter(raw_file_path, bytes=16000)
        schema = get_file_schema(schema_file=schema_file, schema_type="polars")
        limited_schema = _limit_schema_to_available_columns(
            raw_file_path, delimiter, schema
        )
        lf = _build_hmda_lazyframe(
            raw_file=raw_file_path,
            delimiter=delimiter,
            schema=limited_schema,
            year=year,
            add_hmda_index=add_hmda_index,
            archive_path=archive_path,
        )
        lf.sink_parquet(save_path)
    finally:
        if remove_raw_file:
            time.sleep(1)
            raw_file_path.unlink(missing_ok=True)


def _clean_post_2017_lazyframe(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Apply the standard cleaning routine for post-2017 HMDA data."""

    lf = lf.drop(DERIVED_COLUMNS, strict=False)
    lf = lf.drop(POST_2017_TRACT_COLUMNS, strict=False)
    lf = destring_hmda_cols_after_2018(lf)
    return _format_census_tract_lazy(lf)


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


def _clean_post_2017_file(source: Path, destination: Path) -> None:
    """Run the post-2017 cleaning pipeline and persist the result."""

    lf = pl.scan_parquet(source, low_memory=True)
    cleaned = _clean_post_2017_lazyframe(lf)
    cleaned.sink_parquet(destination)


def _add_hmda_index_2017(
    df: pd.DataFrame, archive_path: Path, year: int
) -> pd.DataFrame:
    """Append HMDA index information for 2017 loan application records."""

    file_type_code = HMDALoader.get_file_type_code(archive_path)
    hmda_index = (
        pd.Series(range(df.shape[0]), index=df.index, dtype="int64")
        .astype("string")
        .str.zfill(9)
    )
    df = df.copy()
    df[HMDA_INDEX_COLUMN] = (
        df["activity_year"].astype("string") + file_type_code + "_" + hmda_index
    )
    return df


def _format_census_tract_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize the census tract column within a pandas ``DataFrame``."""

    df = df.copy()
    df[CENSUS_TRACT_COLUMN] = pd.to_numeric(df[CENSUS_TRACT_COLUMN], errors="coerce")
    df[CENSUS_TRACT_COLUMN] = df[CENSUS_TRACT_COLUMN].astype("Int64")
    df[CENSUS_TRACT_COLUMN] = df[CENSUS_TRACT_COLUMN].astype("string")
    df[CENSUS_TRACT_COLUMN] = df[CENSUS_TRACT_COLUMN].str.zfill(11)
    df.loc[
        df[CENSUS_TRACT_COLUMN].str.contains("NA", regex=False),
        CENSUS_TRACT_COLUMN,
    ] = ""
    return df


def _clean_2007_2017_dataframe(
    df: pd.DataFrame, year: int, archive_path: Path
) -> pd.DataFrame:
    """Run the cleaning steps shared by the 2007-2017 HMDA files."""

    df = rename_hmda_columns(df)
    if year == 2017:
        df = _add_hmda_index_2017(df, archive_path, year)
    df = df.drop(columns=DERIVED_COLUMNS, errors="ignore")
    df = destring_hmda_cols_2007_2017(df)
    return _format_census_tract_dataframe(df)


def _clean_2007_2017_file(source: Path, destination: Path, year: int) -> None:
    """Execute the full 2007-2017 cleaning pipeline and persist the result."""

    df = pd.read_parquet(source)
    cleaned = _clean_2007_2017_dataframe(df, year, source)
    table = pa.Table.from_pandas(cleaned, preserve_index=False)
    pq.write_table(table, destination)


def _combined_file_stem(min_year: int, max_year: int) -> str:
    """Return the output stem used for combined lender files."""

    return f"hmda_lenders_combined_{min_year}-{max_year}"


def _find_year_file(folder: Path, year: int, pattern: str) -> Path:
    """Return the first file matching a year specific pattern."""

    matches = list(folder.glob(pattern.format(year=year)))
    if not matches:
        raise FileNotFoundError(
            f"No files found for pattern '{pattern}' in {folder} for year {year}."
        )
    return matches[0]


def _load_parquet_series(folder: Path, years: Iterable[int]) -> pd.DataFrame:
    """Concatenate parquet files across multiple years."""

    frames = [
        pd.read_parquet(_find_year_file(folder, year, "*{year}*.parquet"))
        for year in years
    ]
    return pd.concat(frames, ignore_index=True)


def _load_ts_pre2018(ts_folder: Path, years: Iterable[int]) -> pd.DataFrame:
    """Load and lightly clean pre-2018 Transmittal Series files."""

    frames = []
    for year in years:
        file = _find_year_file(ts_folder, year, "*{year}*.csv")
        df_year = pd.read_csv(file, low_memory=False)
        df_year.columns = [column.strip() for column in df_year.columns]
        df_year = df_year.drop(columns=PRE2018_TS_DROP_COLUMNS, errors="ignore")
        frames.append(df_year)
    return pd.concat(frames, ignore_index=True)


def _load_panel_pre2018(panel_folder: Path, years: Iterable[int]) -> pd.DataFrame:
    """Load and harmonize the pre-2018 panel files."""

    rename_map = {
        "Respondent Identification Number": "Respondent ID",
        "Parent Identification Number": "Parent Respondent ID",
        "Parent State (Panel)": "Parent State",
        "Parent City (Panel)": "Parent City",
        "Parent Name (Panel)": "Parent Name",
        "Respondent State (Panel)": "Respondent State",
        "Respondent Name (Panel)": "Respondent Name",
        "Respondent City (Panel)": "Respondent City",
    }
    frames = []
    for year in years:
        file = _find_year_file(panel_folder, year, "*{year}*.csv")
        df_year = pd.read_csv(file, low_memory=False)
        df_year = df_year.rename(columns=rename_map)
        frames.append(df_year)
    return pd.concat(frames, ignore_index=True)


def _merge_panel_ts_post2018(panel: pd.DataFrame, ts: pd.DataFrame) -> pd.DataFrame:
    """Merge panel and TS data for post-2018 files."""

    df = panel.merge(
        ts, on=["activity_year", "lei"], how="outer", suffixes=("_panel", "_ts")
    )
    return df[df.columns.sort_values()]


def _strip_whitespace_and_replace_missing(df: pd.DataFrame) -> pd.DataFrame:
    """Trim whitespace and normalise missing indicators."""

    for column in df.columns:
        df[column] = [
            value.strip() if isinstance(value, str) else value for value in df[column]
        ]
        df.loc[df[column].isin([np.nan, ""]), column] = None
    return df


def _merge_panel_ts_pre2018(panel: pd.DataFrame, ts: pd.DataFrame) -> pd.DataFrame:
    """Merge and tidy the pre-2018 panel and TS data."""

    df = panel.merge(
        ts,
        on=["Activity Year", "Respondent ID", "Agency Code"],
        how="outer",
        suffixes=(" Panel", " TS"),
    )
    df = df[df.columns.sort_values()]
    return _strip_whitespace_and_replace_missing(df)


# %% Import Functions
# Import Historic HMDA Files (Still Needs Work)
def import_hmda_pre_2007(
    data_folder: Path,
    save_folder: Path,
    min_year: int = 1981,
    max_year: int = 2006,
    contains_string: str = "HMDA_LAR",
    save_to_parquet: bool = True,
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
                parse_options = csv.ParseOptions(
                    delimiter=get_delimiter(file, bytes=16000)
                )
                table = csv.read_csv(file, parse_options=parse_options)
                df = pl.from_arrow(table)

                # Rename Columns
                df = rename_hmda_columns(df, df_type="polars")

                # Destring Numeric Columns
                df = destring_hmda_cols_pre2007(df)

                # Convert to PyArrow Table
                dt = df.to_arrow()

                # Save to CSV
                write_options = csv.WriteOptions(delimiter="|")
                with pa.CompressedOutputStream(str(save_file_csv), "gzip") as out:
                    csv.write_csv(dt, out, write_options=write_options)
                if save_to_parquet:
                    pq.write_table(dt, save_file_parquet)


# Import Data w/ Streaming
def import_hmda_streaming(
    data_folder: Path,
    save_folder: Path,
    schema_file: Path,
    min_year: int = 2007,
    max_year: int = 2023,
    replace: bool = False,
    remove_raw_file: bool = True,
    add_hmda_index: bool = True,
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
        Last year of data to include. The default is 2023.
    replace : bool, optional
        Whether to replace existing files. The default is False.
    remove_raw_file : bool, optional
        Whether to remove the raw file after processing. The default is True.
    add_hmda_index : bool, optional
        Whether to add the HMDAIndex to the data starting in 2017. The default is True.

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
            )


# %% Cleaning Functions
# Clean Data After 2017
def clean_hmda_post_2017(
    data_folder: Path, min_year: int = 2018, max_year: int = 2023, replace: bool = False
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
        Last year of data to include. The default is 2022.
    replace : bool, optional
        Whether to replace existing files. The default is False.

    Returns
    -------
    None.

    """

    data_folder = Path(data_folder)

    for year in range(min_year, max_year + 1):
        for file in data_folder.glob(f"*{year}*.parquet"):
            save_file_parquet = file.with_name(f"{file.stem}_clean.parquet")

            if not _should_process_output(save_file_parquet, replace):
                continue

            _clean_post_2017_file(file, save_file_parquet)
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


# %% Combine Files
# Combine Lenders After 2018
def combine_lenders_panel_ts_post2018(
    panel_folder: Path,
    ts_folder: Path,
    save_folder: Path,
    min_year: int = 2018,
    max_year: int = 2023,
):
    """
    Combine Transmissal Series and Panel data for lenders between 2018 and 2022.

    Parameters
    ----------
    panel_folder : Path
        Folder where raw panel data is stored.
    ts_folder : Path
        Folder where raw transmissal series data is stored.
    save_folder : Path
        Folder where combined data will be saved.
    min_year : int, optional
        First year of data to include. The default is 2018.
    max_year : int, optional
        Last year of data to include. The default is 2023.

    Returns
    -------
    None.

    """

    panel_folder = Path(panel_folder)
    ts_folder = Path(ts_folder)
    save_folder = Path(save_folder)
    save_folder.mkdir(parents=True, exist_ok=True)
    years = range(min_year, max_year + 1)

    df_panel = _load_parquet_series(panel_folder, years)
    df_ts = _load_parquet_series(ts_folder, years)
    df = _merge_panel_ts_post2018(df_panel, df_ts)

    file_stem = _combined_file_stem(min_year, max_year)
    csv_path = save_folder / f"{file_stem}.csv"
    parquet_path = save_folder / f"{file_stem}.parquet"
    df.to_csv(csv_path, index=False, sep="|")
    df.to_parquet(parquet_path, index=False)

    ## Deprecated Code
    # ts_folder = '/project/cl/external_data/HMDA/raw_files/transmissal_series'
    # ts_files = glob.glob(f'{ts_folder}/*.txt')
    # df_ts = []
    # for year in range(2018, 2022+1) :
    #     file = [x for x in ts_files if str(year) in x][0]
    #     df_a = pd.read_csv(file, sep = '|', quoting = 1)
    #     df_ts.append(df_a)
    # df_ts = pd.concat(df_ts)

    # # Import Panel and TS Data
    # panel_folder = '/project/cl/external_data/HMDA/raw_files/panel'
    # panel_files = glob.glob(f'{panel_folder}/*.txt')
    # df_panel = []
    # for year in range(2018, 2022+1) :
    #     file = [x for x in panel_files if str(year) in x][0]
    #     df_a = pd.read_csv(file, sep = '|', quoting = 1)
    #     df_a = df_a.rename(columns = {'upper':'lei'})
    #     df_panel.append(df_a)
    # df_panel = pd.concat(df_panel)


# Combine Lenders Before 2018
def combine_lenders_panel_ts_pre2018(
    panel_folder: Path,
    ts_folder: Path,
    save_folder: Path,
    min_year: int = 2007,
    max_year: int = 2017,
):
    """
    Combine Transmissal Series and Panel data for lenders between 2007 and 2017.

    Parameters
    ----------
    panel_folder : Path
        Folder where raw panel data is stored.
    ts_folder : Path
        Folder where raw transmissal series data is stored.
    save_folder : Path
        Folder where combined data will be saved.
    min_year : int, optional
        First year of data to include. The default is 2007.
    max_year : int, optional
        Last year of data to include. The default is 2017.

    Returns
    -------
    None.

    """

    panel_folder = Path(panel_folder)
    ts_folder = Path(ts_folder)
    save_folder = Path(save_folder)
    save_folder.mkdir(parents=True, exist_ok=True)
    years = range(min_year, max_year + 1)

    df_ts = _load_ts_pre2018(ts_folder, years)
    df_panel = _load_panel_pre2018(panel_folder, years)
    df = _merge_panel_ts_pre2018(df_panel, df_ts)

    csv_path = save_folder / f"{_combined_file_stem(min_year, max_year)}.csv"
    df.to_csv(csv_path, index=False, sep="|")


# %% Main Routine
if __name__ == "__main__":
    # Define Folder Paths
    RAW_DIR = config.RAW_DIR
    CLEAN_DIR = config.CLEAN_DIR
    PROJECT_DIR = config.PROJECT_DIR

    # Import HMDA Loan Data
    data_folder = RAW_DIR / "loans"
    save_folder = CLEAN_DIR / "loans"
    schema_file = "./schemas/hmda_lar_schema_post2018.html"
    import_hmda_streaming(
        data_folder, save_folder, schema_file, min_year=2018, max_year=2023
    )
    # clean_hmda_post_2017(save_folder, min_year=2018, max_year=2023, replace=False)

    # Import HMDA Transmittal Series Data
    data_folder = RAW_DIR / "transmissal_series"
    save_folder = CLEAN_DIR / "transmissal_series"
    schema_file = "./schemas/hmda_ts_schema_post2018.html"
    # import_hmda_streaming(data_folder, save_folder, schema_file)

    # Import HMDA Panel Data
    data_folder = RAW_DIR / "panel"
    save_folder = CLEAN_DIR / "panel"
    schema_file = "./schemas/hmda_panel_schema_post2018.html"
    # import_hmda_post_streaming(data_folder, save_folder, schema_file)

    # Combine Lender Files
    ts_folder = CLEAN_DIR / "transmissal_series"
    panel_folder = CLEAN_DIR / "panel"
    save_folder = PROJECT_DIR / "data"
    # combine_lenders_panel_ts_pre2018(panel_folder, ts_folder, save_folder, min_year=2007, max_year=2017)
    # combine_lenders_panel_ts_post2018(panel_folder, ts_folder, save_folder, min_year=2018, max_year=2023)

    # Update File List
    data_folder = CLEAN_DIR
    # HMDALoader.update_file_list(data_folder)

# %%
