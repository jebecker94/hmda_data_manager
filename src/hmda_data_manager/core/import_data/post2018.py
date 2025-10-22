"""
HMDA Import Functions for Post-2018 Data (2018+)

This module handles import and processing of modern HMDA data files from 2018 onward.
This period introduced significant changes including the HMDAIndex, expanded demographic
fields, and more complex data structures.

Key Characteristics:
- HMDAIndex unique identifier system
- File type codes (a, b, c, d, e)
- Derived demographic columns
- Extensive tract variables
- More complex schema with many optional fields

This module may continue to evolve as CFPB updates the HMDA data format.
"""

import logging
import shutil
from pathlib import Path
import polars as pl
from ...utils.support import destring_hmda_cols_after_2018
from .common import (
    HMDA_INDEX_COLUMN,
    DERIVED_COLUMNS,
    POST_2017_TRACT_COLUMNS,
    normalized_file_stem,
    should_process_output,
    process_hmda_archive,
    format_census_tract_lazy,
)


logger = logging.getLogger(__name__)


def append_hmda_index(
    lf: pl.LazyFrame, year: int, file_type_code: str
) -> pl.LazyFrame:
    """Format the HMDA index values as strings with a consistent prefix.
    
    The HMDAIndex provides a unique identifier for each HMDA record consisting of:
    - Activity year (4 digits)  
    - File type code (1 character: a, b, c, d, e)
    - Row number (9 digits, zero-padded)
    
    For example: 2018a_000000001
    
    Parameters
    ----------
    lf : pl.LazyFrame
        Input lazy frame with row index
    year : int
        Activity year
    file_type_code : str
        Single character file type code
        
    Returns
    -------
    pl.LazyFrame
        Lazy frame with formatted HMDAIndex column
    """
    prefix = f"{year}{file_type_code}_"
    return (
        lf.cast({HMDA_INDEX_COLUMN: pl.String}, strict=False)
        .with_columns(pl.col(HMDA_INDEX_COLUMN).str.zfill(9).alias(HMDA_INDEX_COLUMN))
        .with_columns(
            (pl.lit(prefix) + pl.col(HMDA_INDEX_COLUMN)).alias(HMDA_INDEX_COLUMN)
        )
    )


def rename_columns_post2018(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Rename columns for post-2018 files to standardize naming.
    
    Parameters
    ----------
    lf : pl.LazyFrame
        Input lazy frame
        
    Returns
    -------
    pl.LazyFrame
        Lazy frame with renamed columns
    """
    rename_dict = {
        "loan_to_value_ratio": "combined_loan_to_value_ratio",
    }
    return lf.rename(rename_dict, strict=False)


def import_hmda_post_2018(
    data_folder: Path,
    save_folder: Path,
    schema_file: Path,
    min_year: int = 2018,
    max_year: int = 2024,
    replace: bool = False,
    remove_raw_file: bool = True,
    add_hmda_index: bool = True,
    add_file_type: bool = True,
) -> None:
    """
    Import and clean HMDA data for 2018 onward.

    This function processes modern HMDA zip archives and adds the HMDAIndex unique
    identifier system. The HMDAIndex is a unique identifier for each loan application
    record consisting of:
    - Activity year (4 digits)
    - File type code (1 character)
    - Row number (9 digits, zero padded)
    For example: 2018a_000000001

    Parameters
    ----------
    data_folder : Path
        Folder where raw zip archives are stored.
    save_folder : Path
        Folder where cleaned parquet files will be saved.
    schema_file : Path
        Path to the HTML schema file for post-2018 data.
    min_year : int, optional
        First year of data to include. Default is 2018.
    max_year : int, optional
        Last year of data to include. Default is 2024.
    replace : bool, optional
        Whether to replace existing files. Default is False.
    remove_raw_file : bool, optional
        Whether to remove the extracted raw file after processing. Default is True.
    add_hmda_index : bool, optional
        Whether to add the HMDAIndex to the data. Default is True.
    add_file_type : bool, optional
        Whether to add the file type column to the data. Default is True.

    Returns
    -------
    None
        Files are written to save_folder as parquet files.

    Examples
    --------
    >>> from hmda_data_manager.core.import_data import import_hmda_post_2018
    >>> import_hmda_post_2018(
    ...     data_folder=Path("data/raw/loans"),
    ...     save_folder=Path("data/clean/loans"),
    ...     schema_file=Path("schemas/hmda_lar_schema_post2018.html"),
    ...     min_year=2020,
    ...     max_year=2024
    ... )
    """
    data_folder = Path(data_folder)
    save_folder = Path(save_folder)
    save_folder.mkdir(parents=True, exist_ok=True)
    schema_file = Path(schema_file)

    for year in range(min_year, max_year + 1):
        archives_found = list(data_folder.glob(f"*{year}*.zip"))
        
        if not archives_found:
            logger.debug("No archives found for year %s", year)
            continue
            
        for archive in archives_found:
            file_name = normalized_file_stem(archive.stem)
            save_file = save_folder / f"{file_name}.parquet"

            if not should_process_output(save_file, replace):
                logger.debug("Skipping existing file: %s", save_file)
                continue

            logger.info("Processing archive: %s", archive)
            process_hmda_archive(
                archive_path=archive,
                save_path=save_file,
                schema_file=schema_file,
                year=year,
                remove_raw_file=remove_raw_file,
                add_hmda_index=add_hmda_index,
                add_file_type=add_file_type,
            )


def clean_hmda_post_2017(
    data_folder: Path,
    min_year: int = 2018,
    max_year: int = 2024,
    overwrite: bool = False,
    year_column: str = 'activity_year',
) -> None:
    """
    Clean HMDA data for 2018 onward.

    This function performs additional cleaning on already-imported post-2018 HMDA data,
    including:
    - Removing derived columns (calculated by CFPB)
    - Removing tract variables (demographic data)
    - Column renaming for consistency
    - Numeric type conversion
    - Census tract formatting

    Parameters
    ----------
    data_folder : Path
        Folder where parquet files are stored.
    min_year : int, optional
        First year of data to include. Default is 2018.
    max_year : int, optional
        Last year of data to include. Default is 2024.
    overwrite : bool, optional
        Whether to overwrite existing files. Default is False.
    year_column : str, optional
        Name of the year column. Default is 'activity_year'.

    Returns
    -------
    None
        Cleaned files are saved with '_clean' suffix or overwrite originals.

    Examples
    --------
    >>> from hmda_data_manager.core.import_data import clean_hmda_post_2017
    >>> clean_hmda_post_2017(
    ...     data_folder=Path("data/clean/loans"),
    ...     min_year=2020,
    ...     max_year=2024,
    ...     overwrite=True
    ... )
    """
    data_folder = Path(data_folder)

    for year in range(min_year, max_year + 1):
        files_found = list(data_folder.glob(f"*{year}*.parquet"))
        
        if not files_found:
            logger.debug("No files found for year %s", year)
            continue
            
        for file in files_found:
            save_file_parquet = file.with_name(f"{file.stem}_clean.parquet")

            if not should_process_output(save_file_parquet, overwrite):
                logger.debug("Skipping existing cleaned file: %s", save_file_parquet)
                continue

            logger.info("Cleaning file: %s", file)
            
            try:
                # Load and clean file
                lf = pl.scan_parquet(file, low_memory=True)
                
                # Remove derived columns (calculated by CFPB)
                lf = lf.drop(DERIVED_COLUMNS, strict=False)
                
                # Remove tract variables (demographic data)
                lf = lf.drop(POST_2017_TRACT_COLUMNS, strict=False)
                
                # Rename columns for consistency
                lf = rename_columns_post2018(lf)
                
                # Convert string columns to numeric types
                lf = destring_hmda_cols_after_2018(lf)
                
                # Format census tract column
                lf = format_census_tract_lazy(lf)
                
                # Save cleaned file
                lf.sink_parquet(save_file_parquet)

                # Move cleaned file to original file if overwriting
                if overwrite:
                    shutil.move(save_file_parquet, file)
                    logger.debug("Overwritten original file: %s", file)
                else:
                    logger.debug("Saved cleaned file: %s", save_file_parquet)
                    
            except Exception as e:
                logger.error("Error cleaning file %s: %s", file, str(e))
                # Clean up partial file if it exists
                if save_file_parquet.exists():
                    save_file_parquet.unlink()
                raise
