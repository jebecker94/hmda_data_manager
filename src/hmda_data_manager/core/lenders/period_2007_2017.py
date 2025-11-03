"""
Lender Combination Utilities for 2007-2017 Period
=================================================

Functions to combine Panel and Transmittal Series (TS) data for the
2007-2017 period where keys differ from post-2018.
"""

import logging
from pathlib import Path
from typing import Iterable

import pandas as pd


logger = logging.getLogger(__name__)


# Columns to drop from TS that duplicate panel fields (legacy files)
TS_DROP_COLUMNS_2007_2017 = [
    "Respondent Name (Panel)",
    "Respondent City (Panel)",
    "Respondent State (Panel)",
]


def _strip_whitespace_and_replace_missing(df: pd.DataFrame) -> pd.DataFrame:
    for column in df.columns:
        df[column] = [value.strip() if isinstance(value, str) else value for value in df[column]]
        df.loc[df[column].isin([pd.NA, ""]) , column] = None
    return df


def _find_year_file(folder: Path, year: int, pattern: str) -> Path:
    matches = list(folder.glob(pattern.format(year=year)))
    if not matches:
        raise FileNotFoundError(
            f"No files found for pattern '{pattern}' in {folder} for year {year}."
        )
    return matches[0]


def _load_ts_2007_2017(ts_folder: Path, years: Iterable[int]) -> pd.DataFrame:
    frames = []
    for year in years:
        file = _find_year_file(ts_folder, year, "*{year}*.csv")
        df_year = pd.read_csv(file, low_memory=False)
        df_year.columns = [column.strip() for column in df_year.columns]
        df_year = df_year.drop(columns=TS_DROP_COLUMNS_2007_2017, errors="ignore")
        frames.append(df_year)
    return pd.concat(frames, ignore_index=True)


def _load_panel_2007_2017(panel_folder: Path, years: Iterable[int]) -> pd.DataFrame:
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


def _merge_panel_ts_2007_2017(panel: pd.DataFrame, ts: pd.DataFrame) -> pd.DataFrame:
    df = panel.merge(
        ts,
        on=["Activity Year", "Respondent ID", "Agency Code"],
        how="outer",
        suffixes=(" Panel", " TS"),
    )
    df = df[df.columns.sort_values()]
    return _strip_whitespace_and_replace_missing(df)


def combine_lenders_panel_ts_period_2007_2017(
    panel_folder: Path,
    ts_folder: Path,
    save_folder: Path,
    min_year: int = 2007,
    max_year: int = 2017,
) -> None:
    """Combine Panel and TS CSV files for the 2007-2017 period and write CSV.

    - Loads CSVs for panel and TS per year
    - Harmonizes column names for panel files
    - Merges on ["Activity Year", "Respondent ID", "Agency Code"]
    - Writes pipe-delimited CSV to save_folder
    """
    panel_folder = Path(panel_folder)
    ts_folder = Path(ts_folder)
    save_folder = Path(save_folder)
    save_folder.mkdir(parents=True, exist_ok=True)

    years = range(min_year, max_year + 1)
    logger.info("Loading TS data for %s-%s", min_year, max_year)
    df_ts = _load_ts_2007_2017(ts_folder, years)

    logger.info("Loading panel data for %s-%s", min_year, max_year)
    df_panel = _load_panel_2007_2017(panel_folder, years)

    logger.info("Merging panel and TS data (2007-2017)")
    df = _merge_panel_ts_2007_2017(df_panel, df_ts)

    csv_path = save_folder / f"hmda_lenders_combined_{min_year}-{max_year}.csv"
    logger.info("Saving combined data to %s", csv_path)
    df.to_csv(csv_path, index=False, sep="|")


__all__ = [
    "combine_lenders_panel_ts_period_2007_2017",
]


