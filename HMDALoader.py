# -*- coding: utf-8 -*-
"""
Created on Friday Jul 19 10:20:24 2024
Updated On: Wednesday May 21 10:00:00 2025
@author: Jonathan E. Becker
"""

# Import Packages
import polars as pl
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
import config
from typing import Union
import re

# Set Folder Paths
DATA_DIR = config.DATA_DIR


# Get List of HMDA Files
def get_hmda_files(
    data_folder: Path = DATA_DIR,
    file_type: str = "lar",
    min_year: int | None = None,
    max_year: int | None = None,
    version_type: str | None = None,
    extension: str | None = None,
) -> list[Path]:
    """Return the most recent HMDA files matching the provided filters.

    Parameters
    ----------
    data_folder : Path, optional
        Directory containing the HMDA file list. Defaults to ``DATA_DIR``.
    file_type : str, optional
        HMDA file type to return, e.g. ``"lar"``. Defaults to ``"lar"``.
    min_year : int | None, optional
        Minimum activity year of files to include.
    max_year : int | None, optional
        Maximum activity year of files to include.
    version_type : str | None, optional
        HMDA version identifier to filter by, such as ``"MLAR"``.
    extension : str | None, optional
        Desired file extension (``"parquet"``, ``"csv.gz"``, ``"dta"``). If
        provided, only files with the extension are returned.

    Returns
    -------
    list[Path]
        Ordered list of HMDA file paths.
    """

    # Path of File List
    data_folder = Path(data_folder)
    list_file = data_folder / "file_list_hmda.csv"

    # Load File List
    df = pl.read_csv(list_file)

    # Filter by FileType
    df = df.filter(pl.col("FileType").str.to_lowercase() == file_type.lower())

    # Filter by Years
    if min_year:
        df = df.filter(pl.col("Year") >= min_year)
    if max_year:
        df = df.filter(pl.col("Year") <= max_year)

    # Filter by Extension Type
    if extension:
        if extension.lower() == "parquet":
            df = df.filter(pl.col("FileParquet") == 1)
        if extension.lower() == "csv.gz":
            df = df.filter(pl.col("FileCSVGZ") == 1)
        if extension.lower() == "dta":
            df = df.filter(pl.col("FileDTA") == 1)

    # Filter by Version Type
    if version_type:
        df = df.filter(pl.col("VersionType") == version_type)

    # Keep Most Recent File For Each Year
    df = df.unique(subset=["Year"], keep="first")

    # Sort by Year
    df = df.sort("Year")

    # Get File Names And Add Extensions
    folders = [Path(x) for x in df["FolderName"].to_list()]
    prefixes = df["FilePrefix"].to_list()
    files = [folder / prefix for folder, prefix in zip(folders, prefixes)]
    if extension:
        files = [file.with_suffix(f".{extension}") for file in files]

    # Return File List
    return files


# Load HMDA Files
def load_hmda_file(
    data_folder: Path = DATA_DIR,
    file_type: str = "lar",
    min_year: int = 2018,
    max_year: int = 2023,
    columns: list[str] | None = None,
    filters: list | None = None,
    verbose: bool = False,
    engine: str = "pandas",
    **kwargs,
) -> Union[pd.DataFrame, pl.LazyFrame, pl.DataFrame, pa.Table]:
    """Load a range of HMDA files into a single DataFrame.

    The function currently supports only Parquet files for efficiency.

    Parameters
    ----------
    data_folder : Path, optional
        Directory where HMDA files are stored. Defaults to ``DATA_DIR``.
    file_type : str, optional
        HMDA file type to load (e.g. ``"lar"``). Defaults to ``"lar"``.
    min_year, max_year : int, optional
        Inclusive range of activity years to load. Defaults to 2018--2023.
    columns : list[str] | None, optional
        Subset of columns to read from the files.
    filters : list | None, optional
        Row filters passed to the underlying Parquet reader.
    verbose : bool, optional
        If ``True`` print progress messages. Defaults to ``False``.
    engine : str, optional
        Backend to use for reading files: ``"pandas"``, ``"polars"`` or
        ``"pyarrow"``. Defaults to ``"pandas"``.
    **kwargs : Any
        Additional keyword arguments forwarded to the backend reader.

    Returns
    -------
    Union[pd.DataFrame, pl.LazyFrame, pl.DataFrame, pa.Table]
        Concatenated HMDA dataset.
    """

    # Get HMDA Files
    files = get_hmda_files(
        data_folder=data_folder,
        file_type=file_type,
        min_year=min_year,
        max_year=max_year,
        extension="parquet",
    )

    # Load File
    df = []
    if engine == "pandas":
        for file in files:
            if verbose:
                print("Adding data from file:", file)
            df_a = pd.read_parquet(
                file, columns=columns, filters=filters, **kwargs
            )  # Note: Filters must be passed in pyarrow/pandas format
            df.append(df_a)
        df = pd.concat(df)
    if engine == "pyarrow":
        for file in files:
            if verbose:
                print("Adding data from file:", file)
            df_a = pq.read_table(
                file, columns=columns, filters=filters, **kwargs
            )  # Note: Filters must be passed in pyarrow/pandas format
            df.append(df_a)
        df = pa.concat_tables(df)
    if engine == "polars":
        for file in files:
            if verbose:
                print("Adding data from file:", file)
            df_a = pl.scan_parquet(
                file, **kwargs
            )  # Note: We'll default to lazy loading when using polars
            # df_a = df_a.filter(filters) # Note: Filters must be passed in polars format
            # df_a = df_a.select(columns)
            df.append(df_a)
        df = pl.concat(df)

    # Return DataFrame
    return df


# %% File Management
# Extract Year From FileNames
def extract_years_from_strings(strings: list[str]) -> list[int]:
    """Extract four-digit years from a list of strings.

    Parameters
    ----------
    strings : list[str]
        Strings that contain four-digit year substrings.

    Returns
    -------
    list[int]
        Extracted years in the order they appear in ``strings``.
    """
    years: list[int] = []
    for string in strings:
        found_year = int(re.findall(r"\d{4}", string)[0])
        years.append(found_year)
    return years


# Update File List
def update_file_list(data_folder: Path) -> None:
    """Create ``file_list_hmda.csv`` cataloging available HMDA files.

    Parameters
    ----------
    data_folder : Path
        Directory where cleaned HMDA files are stored.

    Returns
    -------
    None
        This function writes a CSV file in ``data_folder`` and has no return
        value.
    """

    # Get List of Files and Drop Folders
    data_folder = Path(data_folder)
    files = [f for f in data_folder.rglob("*") if f.is_file()]
    files = [f for f in files if "file_list_hmda" not in f.name]

    # Create DataFrame and Get Prefix
    df = pl.DataFrame({"FileName": [str(f) for f in files]})
    df = df.with_columns(
        pl.col("FileName")
        .map_elements(lambda x: Path(x).stem, return_dtype=pl.String)
        .alias("FilePrefix")
    )

    # Get File Types by Prefix
    df = df.with_columns(
        [
            pl.col("FileName")
            .str.to_lowercase()
            .str.ends_with(".parquet")
            .cast(pl.Int8)
            .alias("FileParquet"),
            pl.col("FileName")
            .str.to_lowercase()
            .str.ends_with(".csv.gz")
            .cast(pl.Int8)
            .alias("FileCSVGZ"),
            pl.col("FileName")
            .str.to_lowercase()
            .str.ends_with(".dta")
            .cast(pl.Int8)
            .alias("FileDTA"),
        ]
    )
    df = df.with_columns(
        [
            pl.max("FileParquet").over("FilePrefix").alias("FileParquet"),
            pl.max("FileCSVGZ").over("FilePrefix").alias("FileCSVGZ"),
            pl.max("FileDTA").over("FilePrefix").alias("FileDTA"),
        ]
    )

    # Get File Type
    fp_lower = pl.col("FilePrefix").str.to_lowercase()
    df = df.with_columns(
        pl.when(fp_lower.str.contains("lar"))
        .then("lar")
        .when(fp_lower.str.contains("panel"))
        .then("panel")
        .when(fp_lower.str.contains("ts"))
        .then("ts")
        .otherwise("")
        .alias("FileType")
    )

    # Clean Up
    df = df.with_columns(
        pl.col("FileName")
        .map_elements(lambda x: str(Path(x).parent), return_dtype=pl.String)
        .alias("FolderName")
    )
    df = df.drop("FileName").unique()

    # Get Years
    df = df.with_columns(
        pl.col("FilePrefix")
        .map_elements(lambda x: int(re.findall(r"\d{4}", x)[0]), return_dtype=pl.Int16)
        .alias("Year")
    )

    # Get Version Types from Prefixes
    fp_lower = pl.col("FilePrefix").str.to_lowercase()
    df = df.with_columns(pl.lit("LAR").alias("VersionType"))
    df = df.with_columns(
        pl.when(fp_lower.str.contains("mlar"))
        .then("MLAR")
        .when(fp_lower.str.contains("nationwide"))
        .then("NARC")
        .when(fp_lower.str.contains("public_lar"))
        .then("SNAP")
        .when(fp_lower.str.contains("public_panel"))
        .then("SNAP")
        .when(fp_lower.str.contains("public_ts"))
        .then("SNAP")
        .when(fp_lower.str.contains("one_year"))
        .then("YEAR1")
        .when(fp_lower.str.contains("three_year"))
        .then("YEAR3")
        .otherwise(pl.col("VersionType"))
        .alias("VersionType")
    )

    # Create Master Indicator
    df = df.sort(["FileType", "Year", "VersionType"])
    df = df.with_columns(
        pl.col("VersionType").rank("dense").over("Year").alias("VersionRank")
    )
    df = df.with_columns((pl.col("VersionRank") == 1).cast(pl.Int8).alias("i_Master"))
    df = df.drop("VersionRank")

    # Re-order Variables and Save
    df = df.select(
        [
            "FileType",
            "Year",
            "FilePrefix",
            "VersionType",
            "i_Master",
            "FileParquet",
            "FileCSVGZ",
            "FileDTA",
            "FolderName",
        ]
    )
    df.write_csv(data_folder / "file_list_hmda.csv")


# Get File Type
def get_file_type_code(file_name: Path | str) -> str:
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
    file_type_code: str | None = None
    if "mlar" in base_name.lower():
        file_type_code = "e"
    if "nationwide" in base_name.lower():
        file_type_code = "d"
    if "public_lar" in base_name.lower():
        file_type_code = "c"
    if "public_panel" in base_name.lower():
        file_type_code = "c"
    if "public_ts" in base_name.lower():
        file_type_code = "c"
    if "one_year" in base_name.lower():
        file_type_code = "b"
    if "three_year" in base_name.lower():
        file_type_code = "a"
    if not file_type_code:
        raise ValueError("Cannot parse the HMDA file type from the provided file name.")

    # Return Type Code
    return file_type_code
