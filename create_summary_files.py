# Import required libraries
from pathlib import Path


#%% Helper functions
def _merge_panel_ts_post2018(panel: pd.DataFrame, ts: pd.DataFrame) -> pd.DataFrame:
    """Merge panel and TS data for post-2018 files."""

    df = panel.merge(
        ts, on=["activity_year", "lei"], how="outer", suffixes=("_panel", "_ts")
    )
    return df[df.columns.sort_values()]


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


def _strip_whitespace_and_replace_missing(df: pd.DataFrame) -> pd.DataFrame:
    """Trim whitespace and normalise missing indicators."""

    for column in df.columns:
        df[column] = [
            value.strip() if isinstance(value, str) else value for value in df[column]
        ]
        df.loc[df[column].isin([np.nan, ""]), column] = None
    return df


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

#%% Combine Files
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

## Main routine
if __name__ == "__main__":
    pass
