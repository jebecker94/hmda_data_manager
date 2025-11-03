"""
HMDA Import Functions for Pre-2007 Data (1981-2006)

This module handles import and processing of legacy HMDA data files from 1981-2006.
These files have a simpler format and different column structure compared to later
HMDA data releases.

Key Characteristics:
- Simple delimited text format
- Basic loan application fields
- No derived columns or tract variables
- Different column naming conventions

This module is relatively stable since the legacy data format is unlikely to change.
"""

import logging
from pathlib import Path
import polars as pl
from ...utils.support import (
    get_delimiter,
    rename_hmda_columns,
    destring_hmda_cols_pre2007,
)


logger = logging.getLogger(__name__)


def import_hmda_pre_2007(
    data_folder: Path,
    save_folder: Path,
    min_year: int = 1981,
    max_year: int = 2006,
    contains_string: str = "HMDA_LAR",
) -> None:
    """
    Import and clean HMDA data before 2007.

    This function processes legacy HMDA files from 1981-2006, which have a simpler
    format than later HMDA releases. The processing includes:
    - Column name standardization
    - Numeric type conversion
    - Export to parquet format

    Parameters
    ----------
    data_folder : Path
        Folder containing raw HMDA text files.
    save_folder : Path
        Directory where cleaned files will be written.
    min_year : int, optional
        First year of data to process. Default is 1981.
    max_year : int, optional
        Last year of data to process. Default is 2006.
    contains_string : str, optional
        Substring used to identify HMDA files to process. Default is 'HMDA_LAR'.

    Returns
    -------
    None
        Files are written to save_folder as parquet files.

    Examples
    --------
    >>> from hmda_data_manager.core.import_data import import_hmda_pre_2007
    >>> import_hmda_pre_2007(
    ...     data_folder=Path("data/raw/loans"),
    ...     save_folder=Path("data/clean/loans"),
    ...     min_year=1990,
    ...     max_year=2006
    ... )
    """
    data_folder = Path(data_folder)
    save_folder = Path(save_folder)
    save_folder.mkdir(parents=True, exist_ok=True)

    # Loop Over Years
    for year in range(min_year, max_year + 1):
        # Get Files for this year
        files_found = list(data_folder.glob(f"*{year}*.txt"))
        
        if not files_found:
            logger.debug("No files found for year %s", year)
            continue
            
        for file in files_found:
            # Get File Name
            file_name = file.stem
            save_file_parquet = save_folder / f"{file_name}.parquet"

            # Skip if file already exists
            if save_file_parquet.exists():
                logger.debug("File already exists, skipping: %s", save_file_parquet)
                continue

            # Load and process raw data
            logger.info("Processing file: %s", file)
            
            try:
                # Read delimited file
                df = pl.read_csv(
                    file, 
                    separator=get_delimiter(file, bytes=16000), 
                    ignore_errors=True
                )

                # Rename columns to standard format
                df = rename_hmda_columns(df, df_type="polars")
                
                # Ensure we have a polars DataFrame
                if not isinstance(df, pl.DataFrame):
                    df = pl.from_pandas(df) if hasattr(df, 'to_pandas') else pl.DataFrame(df)

                
                # Ensure we have a polars DataFrame
                if not isinstance(df, pl.DataFrame):
                    df = pl.from_pandas(df) if hasattr(df, 'to_pandas') else pl.DataFrame(df)

                # Convert string columns to numeric types
                df = destring_hmda_cols_pre2007(df)

                # Save to Parquet
                df.write_parquet(save_file_parquet)
                logger.debug("Saved processed file: %s", save_file_parquet)
                
            except Exception as e:
                logger.error("Error processing file %s: %s", file, str(e))
                # Clean up partial file if it exists
                if save_file_parquet.exists():
                    save_file_parquet.unlink()
                raise
