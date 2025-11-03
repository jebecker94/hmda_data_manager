"""
HMDA Summary and Lender Data Functions
=======================================

This module provides functions for creating summary files and combining
HMDA lender data across different time periods. It handles merging panel
and transmittal series (TS) data to create comprehensive lender datasets.

Key Features:
- Combine panel and TS data for different time periods (pre-2018 vs post2018)
- Combine panel and TS data for different time periods (pre-2018 vs post2018)
- Handle different data formats and column structures across HMDA vintages
- Create combined lender files spanning multiple years
- Data cleaning and standardization

Main Functions:
- combine_lenders_panel_ts_post2018: Combine modern lender data (2018+)
- combine_lenders_panel_ts_pre2018: Combine legacy lender data (2007-2017)

Notes:
- Pre-2018 and post2018 data have different schemas and merge keys
- Pre-2018 and post2018 data have different schemas and merge keys
- Post-2018 uses 'lei' (Legal Entity Identifier) as primary key
- Pre-2018 uses combination of 'Respondent ID' and 'Agency Code'
"""

import logging
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd


logger = logging.getLogger(__name__)


# Constants for column handling
PRE2018_TS_DROP_COLUMNS = [
    "Respondent Name (Panel)",
    "Respondent City (Panel)",
    "Respondent State (Panel)",
]


def _merge_panel_ts_post2018(panel: pd.DataFrame, ts: pd.DataFrame) -> pd.DataFrame:
    """Merge panel and TS data for post2018 files.
    """Merge panel and TS data for post2018 files.
    
    Post-2018 data uses LEI (Legal Entity Identifier) as the primary key
    for matching between panel and transmittal series data.
    
    Parameters
    ----------
    panel : pd.DataFrame
        Panel data with lender information
    ts : pd.DataFrame
        Transmittal series data with submission information
        
    Returns
    -------
    pd.DataFrame
        Merged dataset with sorted columns
    """
    df = panel.merge(
        ts, on=["activity_year", "lei"], how="outer", suffixes=("_panel", "_ts")
    )
    return df[df.columns.sort_values()]


def _merge_panel_ts_pre2018(panel: pd.DataFrame, ts: pd.DataFrame) -> pd.DataFrame:
    """Merge and tidy the pre-2018 panel and TS data.
    
    Pre-2018 data uses a combination of Activity Year, Respondent ID, and
    Agency Code as the composite key for matching.
    
    Parameters
    ----------
    panel : pd.DataFrame
        Panel data with lender information
    ts : pd.DataFrame
        Transmittal series data with submission information
        
    Returns
    -------
    pd.DataFrame
        Merged and cleaned dataset with sorted columns
    """
    df = panel.merge(
        ts,
        on=["Activity Year", "Respondent ID", "Agency Code"],
        how="outer",
        suffixes=(" Panel", " TS"),
    )
    df = df[df.columns.sort_values()]
    return _strip_whitespace_and_replace_missing(df)


def _load_parquet_series(folder: Path, years: Iterable[int]) -> pd.DataFrame:
    """Concatenate parquet files across multiple years.
    
    Parameters
    ----------
    folder : Path
        Directory containing parquet files
    years : Iterable[int]
        Years to include in the concatenation
        
    Returns
    -------
    pd.DataFrame
        Concatenated dataset across all specified years
    """
    frames = [
        pd.read_parquet(_find_year_file(folder, year, "*{year}*.parquet"))
        for year in years
    ]
    return pd.concat(frames, ignore_index=True)


def _load_ts_pre2018(ts_folder: Path, years: Iterable[int]) -> pd.DataFrame:
    """Load and lightly clean pre-2018 Transmittal Series files.
    
    Pre-2018 TS files are in CSV format and need column name cleaning
    and removal of panel-related columns that are duplicated.
    
    Parameters
    ----------
    ts_folder : Path
        Directory containing TS CSV files
    years : Iterable[int]
        Years to process
        
    Returns
    -------
    pd.DataFrame
        Concatenated and cleaned TS data
    """
    frames = []
    for year in years:
        file = _find_year_file(ts_folder, year, "*{year}*.csv")
        df_year = pd.read_csv(file, low_memory=False)
        df_year.columns = [column.strip() for column in df_year.columns]
        df_year = df_year.drop(columns=PRE2018_TS_DROP_COLUMNS, errors="ignore")
        frames.append(df_year)
    return pd.concat(frames, ignore_index=True)


def _load_panel_pre2018(panel_folder: Path, years: Iterable[int]) -> pd.DataFrame:
    """Load and harmonize the pre-2018 panel files.
    
    Pre-2018 panel files have inconsistent column naming that needs
    to be standardized across years.
    
    Parameters
    ----------
    panel_folder : Path
        Directory containing panel CSV files
    years : Iterable[int]
        Years to process
        
    Returns
    -------
    pd.DataFrame
        Concatenated and harmonized panel data
    """
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


def _strip_whitespace_and_replace_missing(df: pd.DataFrame) -> pd.DataFrame:
    """Trim whitespace and normalize missing indicators.
    
    Parameters
    ----------
    df : pd.DataFrame
        DataFrame to clean
        
    Returns
    -------
    pd.DataFrame
        Cleaned DataFrame with trimmed strings and normalized missing values
    """
    for column in df.columns:
        df[column] = [
            value.strip() if isinstance(value, str) else value for value in df[column]
        ]
        df.loc[df[column].isin([np.nan, ""]), column] = None
    return df


def _combined_file_stem(min_year: int, max_year: int) -> str:
    """Return the output stem used for combined lender files.
    
    Parameters
    ----------
    min_year : int
        First year in the range
    max_year : int
        Last year in the range
        
    Returns
    -------
    str
        Standardized file stem for combined files
    """
    return f"hmda_lenders_combined_{min_year}-{max_year}"


def _find_year_file(folder: Path, year: int, pattern: str) -> Path:
    """Return the first file matching a year-specific pattern.
    
    Parameters
    ----------
    folder : Path
        Directory to search
    year : int
        Year to match
    pattern : str
        Glob pattern with {year} placeholder
        
    Returns
    -------
    Path
        Path to the matching file
        
    Raises
    ------
    FileNotFoundError
        If no files match the pattern for the specified year
    """
    matches = list(folder.glob(pattern.format(year=year)))
    if not matches:
        raise FileNotFoundError(
            f"No files found for pattern '{pattern}' in {folder} for year {year}."
        )
    return matches[0]


def combine_lenders_panel_ts_post2018(
    panel_folder: Path,
    ts_folder: Path,
    save_folder: Path,
    min_year: int = 2018,
    max_year: int = 2023,
) -> None:
    """
    Combine Transmittal Series and Panel data for lenders between 2018 and later years.

    This function merges modern HMDA lender data using the LEI (Legal Entity Identifier)
    as the primary key. The result includes comprehensive lender information from both
    panel registrations and transmittal series submissions.

    Parameters
    ----------
    panel_folder : Path
        Folder where processed panel parquet files are stored
    ts_folder : Path
        Folder where processed transmittal series parquet files are stored
    save_folder : Path
        Folder where combined data will be saved
    min_year : int, optional
        First year of data to include. Default is 2018.
    max_year : int, optional
        Last year of data to include. Default is 2023.

    Returns
    -------
    None
        Combined files are saved as both CSV (pipe-delimited) and Parquet formats.

    Examples
    --------
    >>> from hmda_data_manager.utils.summary import combine_lenders_panel_ts_post2018
    >>> combine_lenders_panel_ts_post2018(
    ...     panel_folder=Path("data/clean/panel"),
    ...     ts_folder=Path("data/clean/transmissal_series"),
    ...     save_folder=Path("data/combined"),
    ...     min_year=2020,
    ...     max_year=2024
    ... )

    Notes
    -----
    Post-2018 data uses LEI as the primary identifier for lenders, which provides
    better consistency across years compared to the pre-2018 system.
    """
    panel_folder = Path(panel_folder)
    ts_folder = Path(ts_folder)
    save_folder = Path(save_folder)
    save_folder.mkdir(parents=True, exist_ok=True)
    years = range(min_year, max_year + 1)

    logger.info("Loading panel data for years %s-%s", min_year, max_year)
    df_panel = _load_parquet_series(panel_folder, years)
    
    logger.info("Loading transmittal series data for years %s-%s", min_year, max_year)
    df_ts = _load_parquet_series(ts_folder, years)
    
    logger.info("Merging panel and TS data")
    df = _merge_panel_ts_post2018(df_panel, df_ts)

    file_stem = _combined_file_stem(min_year, max_year)
    csv_path = save_folder / f"{file_stem}.csv"
    parquet_path = save_folder / f"{file_stem}.parquet"
    
    logger.info("Saving combined data to %s", csv_path)
    df.to_csv(csv_path, index=False, sep="|")
    df.to_parquet(parquet_path, index=False)
    
    logger.info("Successfully created combined lender file with %s records", len(df))


def combine_lenders_panel_ts_pre2018(
    panel_folder: Path,
    ts_folder: Path,
    save_folder: Path,
    min_year: int = 2007,
    max_year: int = 2017,
) -> None:
    """
    Combine Transmittal Series and Panel data for lenders between 2007 and 2017.

    This function merges legacy HMDA lender data using the combination of Respondent ID
    and Agency Code as the composite key. The data requires more extensive cleaning
    due to inconsistent formatting in the pre-2018 period.

    Parameters
    ----------
    panel_folder : Path
        Folder where processed panel CSV files are stored
    ts_folder : Path
        Folder where processed transmittal series CSV files are stored
    save_folder : Path
        Folder where combined data will be saved
    min_year : int, optional
        First year of data to include. Default is 2007.
    max_year : int, optional
        Last year of data to include. Default is 2017.

    Returns
    -------
    None
        Combined file is saved as pipe-delimited CSV.

    Examples
    --------
    >>> from hmda_data_manager.utils.summary import combine_lenders_panel_ts_pre2018
    >>> combine_lenders_panel_ts_pre2018(
    ...     panel_folder=Path("data/clean/panel"),
    ...     ts_folder=Path("data/clean/transmissal_series"),
    ...     save_folder=Path("data/combined"),
    ...     min_year=2010,
    ...     max_year=2017
    ... )

    Notes
    -----
    Pre-2018 data uses a composite key of Activity Year, Respondent ID, and Agency Code.
    The data also requires more extensive cleaning due to inconsistent whitespace
    and missing value indicators.
    """
    panel_folder = Path(panel_folder)
    ts_folder = Path(ts_folder)
    save_folder = Path(save_folder)
    save_folder.mkdir(parents=True, exist_ok=True)
    years = range(min_year, max_year + 1)

    logger.info("Loading transmittal series data for years %s-%s", min_year, max_year)
    df_ts = _load_ts_pre2018(ts_folder, years)
    
    logger.info("Loading panel data for years %s-%s", min_year, max_year)
    df_panel = _load_panel_pre2018(panel_folder, years)
    
    logger.info("Merging panel and TS data")
    df = _merge_panel_ts_pre2018(df_panel, df_ts)

    csv_path = save_folder / f"{_combined_file_stem(min_year, max_year)}.csv"
    logger.info("Saving combined data to %s", csv_path)
    df.to_csv(csv_path, index=False, sep="|")
    
    logger.info("Successfully created combined lender file with %s records", len(df))
