"""
Common utilities and constants shared across HMDA import modules.

This module contains helper functions and constants used by all time-period
specific import modules. These utilities handle file processing, schema
management, and data transformations that are consistent across HMDA formats.
"""

import logging
import time
from pathlib import Path
import polars as pl
from ...utils.support import get_delimiter, get_file_schema, unzip_hmda_file


logger = logging.getLogger(__name__)


# Shared constants used across all time periods
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
    raw_file: Path, delimiter: str, schema: dict[str, pl.DataType]
) -> dict[str, pl.DataType]:
    """Restrict a schema to the columns available in a delimited file.
    
    Parameters
    ----------
    raw_file : Path
        Path to the raw CSV/delimited file
    delimiter : str
        Column delimiter character
    schema : dict[str, pl.DataType]
        Full schema dictionary
        
    Returns
    -------
    dict[str, pl.DataType]
        Schema restricted to available columns
    """
    csv_columns = pl.read_csv(raw_file, separator=delimiter, n_rows=0, ignore_errors=True).columns
    logger.debug("CSV columns: %s", csv_columns)
    return {column: schema[column] for column in csv_columns if column in schema}


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


def format_census_tract_lazy(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Standardize the census tract column within a lazy frame.
    
    Parameters
    ----------
    lf : pl.LazyFrame
        Input lazy frame with census tract column
        
    Returns
    -------
    pl.LazyFrame
        Lazy frame with standardized census tract column
    """
    return (
        lf.cast({CENSUS_TRACT_COLUMN: pl.Float64}, strict=False)
        .cast({CENSUS_TRACT_COLUMN: pl.Int64}, strict=False)
        .cast({CENSUS_TRACT_COLUMN: pl.String}, strict=False)
        .with_columns(
            pl.col(CENSUS_TRACT_COLUMN).str.zfill(11).alias(CENSUS_TRACT_COLUMN)
        )
    )


def build_hmda_lazyframe(
    raw_file: Path,
    delimiter: str,
    schema: dict[str, pl.DataType],
    year: int,
    add_hmda_index: bool,
    archive_path: Path,
    add_file_type: bool,
) -> pl.LazyFrame:
    """Create a ``polars`` lazy frame for a raw HMDA delimited file.
    
    Parameters
    ----------
    raw_file : Path
        Path to the extracted delimited file
    delimiter : str
        Column delimiter character
    schema : dict[str, pl.DataType]
        Column schema dictionary
    year : int
        Data year
    add_hmda_index : bool
        Whether to add HMDA index column
    archive_path : Path
        Path to the original archive file
    add_file_type : bool
        Whether to add file type column
        
    Returns
    -------
    pl.LazyFrame
        Configured lazy frame for the HMDA file
    """
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
    file_type_code = get_file_type_code(archive_path)
    if add_file_type:
        lf = lf.with_columns(pl.lit(file_type_code).alias("file_type"))
    if add_hmda_index:
        # Import locally to avoid circular imports
        from .post2018 import append_hmda_index
        lf = append_hmda_index(lf, year, file_type_code)

    return lf


def process_hmda_archive(
    archive_path: Path,
    save_path: Path,
    schema_file: Path,
    year: int,
    remove_raw_file: bool,
    add_hmda_index: bool,
    add_file_type: bool,
) -> None:
    """Read, clean and persist a single HMDA archive.
    
    Parameters
    ----------
    archive_path : Path
        Path to the zip archive
    save_path : Path
        Path where processed file will be saved
    schema_file : Path
        Path to the HTML schema file
    year : int
        Data year
    remove_raw_file : bool
        Whether to remove extracted file after processing
    add_hmda_index : bool
        Whether to add HMDA index column
    add_file_type : bool
        Whether to add file type column
    """
    raw_file_path = Path(unzip_hmda_file(archive_path, archive_path.parent))
    try:
        delimiter = get_delimiter(raw_file_path, bytes=16000)
        schema = get_file_schema(schema_file=schema_file, schema_type="polars")
        limited_schema = limit_schema_to_available_columns(
            raw_file_path,
            delimiter,
            schema,
        )
        lf = build_hmda_lazyframe(
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


def save_to_dataset(
    data_folder: Path,
    save_folder: Path,
    min_year: int = 2018,
    max_year: int = 2024,
):
    """Save HMDA data to dataset with Hive Partitioning.
    
    Parameters
    ----------
    data_folder : Path
        Folder containing processed parquet files
    save_folder : Path
        Target folder for partitioned dataset
    min_year : int, optional
        First year to include. Default is 2018.
    max_year : int, optional
        Last year to include. Default is 2024.
    """
    data_folder = Path(data_folder)
    save_folder = Path(save_folder)

    df = []
    years = range(min_year, max_year + 1)
    for year in years:
        for file in data_folder.glob(f"*{year}*.parquet"):
            df_a = pl.scan_parquet(file)
            df.append(df_a)
    df = pl.concat(df, how='diagonal_relaxed')
    year_column = 'activity_year' if 'activity_year' in df.columns else 'as_of_year'
    df.sink_parquet(
        pl.PartitionByKey(
            save_folder,
            by=[pl.col(year_column), pl.col('file_type')],
            include_key=True,
        ),
        mkdir=True,
    )
