"""
Post-2018 Lender Combination Utilities
=====================================

Functions to combine Panel and Transmittal Series (TS) data for post-2018
datasets using LEI and activity_year as keys.
"""

import logging
from pathlib import Path
from typing import Iterable

import pandas as pd


logger = logging.getLogger(__name__)


def _load_parquet_series(folder: Path, years: Iterable[int]) -> pd.DataFrame:
    """Concatenate per-year Parquet files from a folder.

    Expects files that include the year in the filename.
    """
    frames = [
        pd.read_parquet(_find_year_file(folder, year, pattern="*{year}*.parquet"))
        for year in years
    ]
    return pd.concat(frames, ignore_index=True)


def _combined_file_stem(min_year: int, max_year: int) -> str:
    return f"hmda_lenders_combined_{min_year}-{max_year}"


def _find_year_file(folder: Path, year: int, pattern: str) -> Path:
    matches = list(folder.glob(pattern.format(year=year)))
    if not matches:
        raise FileNotFoundError(
            f"No files found for pattern '{pattern}' in {folder} for year {year}."
        )
    return matches[0]


def _merge_panel_ts_post2018(panel: pd.DataFrame, ts: pd.DataFrame) -> pd.DataFrame:
    """Merge panel and TS using ['activity_year', 'lei'] with outer join."""
    df = panel.merge(
        ts, on=["activity_year", "lei"], how="outer", suffixes=("_panel", "_ts")
    )
    return df[df.columns.sort_values()]


def combine_lenders_panel_ts_post2018(
    panel_folder: Path,
    ts_folder: Path,
    save_folder: Path,
    min_year: int = 2018,
    max_year: int = 2023,
) -> None:
    """Combine Panel and TS parquet files for post-2018 lenders and save outputs.

    - Loads per-year parquet files for panel and TS
    - Merges on ['activity_year', 'lei']
    - Writes CSV (pipe-delimited) and Parquet to save_folder
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

    logger.info("Merging panel and TS data (post-2018)")
    df = _merge_panel_ts_post2018(df_panel, df_ts)

    file_stem = _combined_file_stem(min_year, max_year)
    csv_path = save_folder / f"{file_stem}.csv"
    parquet_path = save_folder / f"{file_stem}.parquet"

    logger.info("Saving combined data to %s", csv_path)
    df.to_csv(csv_path, index=False, sep="|")
    df.to_parquet(parquet_path, index=False)

    logger.info("Successfully created combined lender file with %s records", len(df))


__all__ = [
    "combine_lenders_panel_ts_post2018",
]


