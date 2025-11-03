"""
HMDA Summary and Lender Data Functions
=======================================

This module provides functions for creating summary files and combining
HMDA lender data across different time periods. It handles merging panel
and transmittal series (TS) data to create comprehensive lender datasets.

Key Features:
- Combine panel and TS data for different time periods (pre-2018)
- Handle different data formats and column structures across HMDA vintages
- Create combined lender files spanning multiple years
- Data cleaning and standardization

Main Functions:
  (moved to core/lenders)

Notes:
- Pre-2018 and post2018 data have different schemas and merge keys
- Pre-2018 uses combination of 'Respondent ID' and 'Agency Code'
"""

import logging
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd


logger = logging.getLogger(__name__)


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
