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
import config
from typing import Union
import glob
import os
import re

# Set Folder Paths
DATA_DIR = config.DATA_DIR


# Get List of HMDA Files
def get_hmda_files(
    data_folder=DATA_DIR,
    file_type="lar",
    min_year=None,
    max_year=None,
    version_type=None,
    extension=None,
):
    """
    Gets the list of most up-to-date HMDA files.

    Parameters
    ----------
    data_folder : str
        The folder where the HMDA files are stored.
    min_year : int, optional
        The first year of HMDA files to return. The default is None.
    max_year : int, optional
        The last year of HMDA files to return. The default is None.
    version_type : str, optional
        The version type of HMDA files to return. The default is None.
    extension : str, optional
        The file extension of HMDA files to return. The default is None.

    Returns
    -------
    files : list
        List of HMDA files.

    """

    # Path of File List
    list_file = f"{data_folder}/file_list_hmda.csv"

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
    folders = df["FolderName"].to_list()
    files = df["FilePrefix"].to_list()
    if extension:
        files = [f"{x}/{y}.{extension}" for x, y in zip(folders, files)]

    # Return File List
    return files


# Load HMDA Files
def load_hmda_file(
    data_folder=DATA_DIR,
    file_type="lar",
    min_year=2018,
    max_year=2023,
    columns=None,
    filters=None,
    verbose=False,
    engine="pandas",
    **kwargs,
) -> Union[pd.DataFrame, pl.LazyFrame, pl.DataFrame, pa.Table]:
    """
    Load HMDA files.

    Note that in orrder to load files efficiently, we use only parquet formats. Other formats are not supported at this time, but may be implemented later.

    Parameters
    ----------
    data_folder : str
        The folder where the HMDA files are stored.
    file_type : str, optional
        The type of HMDA file to load. The default is 'lar'.
    min_year : int, optional
            The first year of HMDA files to load. The default is 2018.
    max_year : int, optional
            The last year of HMDA files to load. The default is 2023.
    columns : list, optional
        The columns to load. The default is None.
    filters : list, optional
        The filters to apply. The default is None.
    verbose : bool, optional
        Whether to print progress messages. The default is False.
    engine : str, optional
        The engine to use for loading the data, either 'pandas', 'polars', or 'pyarrow'. The default is 'pandas'.
    **kwargs : optional
        Additional arguments to pass to pd.read_parquet.

    Returns
    -------
    df : DataFrame
        The loaded HMDA file.

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
def extract_years_from_strings(strings):
    years = []
    for string in strings:
        found_year = re.findall(r"\d{4}", string)[0]
        years.append(found_year)
    return years


# Update File List
def update_file_list(data_folder):
    """
    Creates a CSV list of all HMDA files. Helps to standardize future work by
    keeping track of which version of a file we should be using.

    Parameters
    ----------
    data_folder : str
        Folder where cleaned HMDA data is stored.

    Returns
    -------
    None.

    """

    # Get List of Files and Drop Folders
    files = glob.glob(f"{data_folder}/**", recursive=True)
    files = [x for x in files if os.path.isfile(x)]
    files = [x for x in files if "file_list_hmda" not in x]

    # Create DataFrame and Get Prefix
    df = pl.DataFrame({"FileName": files})
    df = df.with_columns(
        pl.col("FileName")
        .map_elements(
            lambda x: os.path.basename(x).split(".")[0], return_dtype=pl.String
        )
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
        .map_elements(lambda x: os.path.dirname(x), return_dtype=pl.String)
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
    df.write_csv(f"{data_folder}/file_list_hmda.csv")


# Get File Type
def get_file_type_code(file_name):
    # Get Base Name of File
    base_name = os.path.basename(file_name).split(".")[0]

    # Get Version Types from Prefixes
    file_type_code = None
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
