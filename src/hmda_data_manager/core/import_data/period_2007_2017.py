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
import time
from collections.abc import Sequence
from pathlib import Path
import polars as pl
from ...utils.support import destring_hmda_cols_2007_2017, get_delimiter, get_file_schema, unzip_hmda_file


logger = logging.getLogger(__name__)


# Constants specific to 2007-2017 data
PRE2018_TS_DROP_COLUMNS = [
    "Respondent Name (Panel)",
    "Respondent City (Panel)",
    "Respondent State (Panel)",
]

FLOAT_DTYPES = {pl.Float32, pl.Float64}
INTEGER_DTYPES = {
    pl.Int8,
    pl.Int16,
    pl.Int32,
    pl.Int64,
    pl.UInt8,
    pl.UInt16,
    pl.UInt32,
    pl.UInt64,
}


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
    raw_file: Path, delimiter: str, schema: dict[str, pl.DataType | str]
) -> dict[str, pl.DataType | str]:
    """Restrict a schema to the columns available in a delimited file.

    Parameters
    ----------
    raw_file : Path
        Path to the raw CSV/delimited file
    delimiter : str
        Column delimiter character
    schema : dict[str, pl.DataType | str]
        Full schema dictionary

    Returns
    -------
    dict[str, pl.DataType | str]
        Schema restricted to available columns
    """
    csv_columns = pl.read_csv(
        raw_file, separator=delimiter, n_rows=0, ignore_errors=True
    ).columns
    logger.debug("CSV columns: %s", csv_columns)
    return {column: schema[column] for column in csv_columns if column in schema}


def _integer_like_float_columns(
    lf: pl.LazyFrame, float_columns: Sequence[str]
) -> list[str]:
    """Return float columns whose non-null values are all integers."""

    if not float_columns:
        return []

    checks = [
        (
            pl.when(pl.col(column).is_null())
            .then(True)
            .otherwise(
                pl.when(pl.col(column).is_finite())
                .then(pl.col(column).round(0) == pl.col(column))
                .otherwise(False)
            )
            .all()
            .alias(column)
        )
        for column in float_columns
    ]

    if not checks:
        return []

    result = lf.select(checks).collect()
    return [column for column in float_columns if bool(result[column][0])]


def cast_integer_like_floats(
    lf: pl.LazyFrame, schema: dict[str, pl.DataType | str]
) -> pl.LazyFrame:
    """Cast float columns with integer-only values to integer dtypes."""

    float_columns = [
        column for column, dtype in lf.schema.items() if dtype in FLOAT_DTYPES
    ]
    integer_like_columns = _integer_like_float_columns(lf, float_columns)
    if not integer_like_columns:
        return lf

    casts = []
    for column in integer_like_columns:
        target_dtype = schema.get(column, pl.Int64)
        if target_dtype not in INTEGER_DTYPES:
            target_dtype = pl.Int64
        casts.append(pl.col(column).cast(target_dtype, strict=False).alias(column))  # type: ignore[arg-type]

    return lf.with_columns(casts)


def build_hmda_lazyframe(
    raw_file: Path,
    delimiter: str,
    schema: dict[str, pl.DataType | str],
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
        Whether to add HMDA index column (not used for 2007-2017)
    archive_path : Path
        Path to the original archive file
    add_file_type : bool
        Whether to add file type column (not used for 2007-2017)

    Returns
    -------
    pl.LazyFrame
        Configured lazy frame for the HMDA file
    """
    # For 2007-2017, we don't add HMDA index or file type
    lf = pl.scan_csv(raw_file, separator=delimiter, low_memory=True, schema=schema)  # type: ignore[arg-type]
    return cast_integer_like_floats(lf, schema)


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
        Whether to add HMDA index column (ignored for 2007-2017)
    add_file_type : bool
        Whether to add file type column (ignored for 2007-2017)
    """
    raw_file_path = Path(unzip_hmda_file(archive_path, archive_path.parent))
    try:
        delimiter = get_delimiter(raw_file_path, bytes=16000)
        schema = get_file_schema(schema_file=schema_file, schema_type="polars")
        if not isinstance(schema, dict):
            raise ValueError(f"Expected dict schema, got {type(schema)}")
        limited_schema = limit_schema_to_available_columns(
            raw_file_path,
            delimiter,
            schema,  # type: ignore[arg-type]
        )
        lf = build_hmda_lazyframe(
            raw_file=raw_file_path,
            delimiter=delimiter,
            schema=limited_schema,
            year=year,
            add_hmda_index=False,  # Always False for 2007-2017
            add_file_type=False,   # Always False for 2007-2017
            archive_path=archive_path,
        )
        lf.sink_parquet(save_path)
    finally:
        if remove_raw_file:
            time.sleep(1)
            raw_file_path.unlink(missing_ok=True)


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
    Unlike post2018 data, these files do not receive HMDA index values or file type codes.

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
