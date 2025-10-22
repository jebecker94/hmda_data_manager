"""
HMDA Import Functions for 2007-2017 Data

This module handles import and processing of HMDA data files from 2007-2017.
This period represents the standardized HMDA format before the major changes
introduced in 2018.

Key Characteristics:
- Standardized zip archive format
- Consistent schema across years
- No HMDA index (introduced in 2018)
- Uses destring_hmda_cols_2007_2017 processing
- Includes tract variables that need to be handled

This module is relatively stable since the 2007-2017 data format is finalized.
"""

import logging
from pathlib import Path
from ...utils.support import destring_hmda_cols_2007_2017
from .common import (
    normalized_file_stem,
    should_process_output,
    process_hmda_archive,
    PRE2018_TS_DROP_COLUMNS,
)


logger = logging.getLogger(__name__)


def import_hmda_2007_2017(
    data_folder: Path,
    save_folder: Path,
    schema_file: Path,
    min_year: int = 2007,
    max_year: int = 2017,
    replace: bool = False,
    remove_raw_file: bool = True,
) -> None:
    """
    Import and clean HMDA data for 2007-2017.

    This function processes HMDA zip archives from the standardized period (2007-2017).
    Unlike post-2018 data, these files do not receive HMDA index values or file type codes.

    Parameters
    ----------
    data_folder : Path
        Folder where raw zip archives are stored.
    save_folder : Path
        Folder where cleaned parquet files will be saved.
    schema_file : Path
        Path to the HTML schema file for this period.
    min_year : int, optional
        First year of data to include. Default is 2007.
    max_year : int, optional
        Last year of data to include. Default is 2017.
    replace : bool, optional
        Whether to replace existing files. Default is False.
    remove_raw_file : bool, optional
        Whether to remove the extracted raw file after processing. Default is True.

    Returns
    -------
    None
        Files are written to save_folder as parquet files.

    Examples
    --------
    >>> from hmda_data_manager.core.import_data import import_hmda_2007_2017
    >>> import_hmda_2007_2017(
    ...     data_folder=Path("data/raw/loans"),
    ...     save_folder=Path("data/clean/loans"),
    ...     schema_file=Path("schemas/hmda_lar_schema_2007-2017.html"),
    ...     min_year=2010,
    ...     max_year=2017
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
                add_hmda_index=False,  # No HMDA index for 2007-2017
                add_file_type=False,   # No file type for 2007-2017
            )


def clean_hmda_2007_2017(
    data_folder: Path, 
    min_year: int = 2007, 
    max_year: int = 2017, 
    replace: bool = False
) -> None:
    """
    Clean HMDA data for 2007-2017.

    This function performs additional cleaning on already-imported 2007-2017 HMDA data,
    including numeric type conversion and standardization.

    Parameters
    ----------
    data_folder : Path
        Folder containing HMDA parquet files.
    min_year : int, optional
        First year to process. Default is 2007.
    max_year : int, optional
        Last year to process. Default is 2017.
    replace : bool, optional
        Overwrite existing cleaned files if True. Default is False.

    Returns
    -------
    None
        Cleaned files are saved with '_clean' suffix.

    Note
    ----
    This function is currently incomplete - the _clean_2007_2017_file helper
    function needs to be implemented.
    
    Examples
    --------
    >>> from hmda_data_manager.core.import_data import clean_hmda_2007_2017
    >>> clean_hmda_2007_2017(
    ...     data_folder=Path("data/clean/loans"),
    ...     min_year=2010,
    ...     max_year=2017,
    ...     replace=True
    ... )
    """
    data_folder = Path(data_folder)

    for year in range(min_year, max_year + 1):
        # Look for files from this year
        files = list(data_folder.glob(f"*{year}*records*.parquet")) + list(
            data_folder.glob(f"*{year}*public*.parquet")
        )
        
        if not files:
            logger.debug("No files found for year %s", year)
            continue
            
        for file in files:
            save_file_parquet = file.with_name(f"{file.stem}_clean.parquet")

            if not should_process_output(save_file_parquet, replace):
                logger.debug("Skipping existing cleaned file: %s", save_file_parquet)
                continue

            logger.info("Cleaning file: %s", file)
            _clean_2007_2017_file(file, save_file_parquet, year)


def _clean_2007_2017_file(input_file: Path, output_file: Path, year: int) -> None:
    """
    Clean a single 2007-2017 HMDA file.
    
    This is a placeholder implementation that needs to be completed.
    The cleaning should include:
    - Applying destring_hmda_cols_2007_2017
    - Dropping unnecessary columns (PRE2018_TS_DROP_COLUMNS)
    - Standardizing data types
    
    Parameters
    ----------
    input_file : Path
        Path to input parquet file
    output_file : Path  
        Path where cleaned file will be saved
    year : int
        Data year
        
    TODO
    ----
    This function needs to be implemented with the actual cleaning logic.
    """
    logger.warning(
        "The _clean_2007_2017_file function is not yet implemented. "
        "File %s was not processed.", input_file
    )
    # TODO: Implement the actual cleaning logic
    # This should read the parquet file, apply cleaning transformations,
    # and save the result
    pass
