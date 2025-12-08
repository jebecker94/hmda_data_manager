#!/usr/bin/env python
"""
Inspect 2006 pre-2007 HMDA data directly.

Run this from the project root:
    python inspect_pre2007_2006.py
"""

import logging
import zipfile
from pathlib import Path
import polars as pl

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

# Import utilities
from hmda_data_manager.utils.io import get_delimiter
from hmda_data_manager.core.config import RAW_DIR


def load_2006_data() -> pl.DataFrame:
    """Load 2006 data from ZIP archive for inspection."""

    raw_folder = RAW_DIR / "loans"
    archive = raw_folder / "HMDA_LAR_2006.zip"

    if not archive.exists():
        raise FileNotFoundError(f"Archive not found: {archive}")

    logger.info("Opening archive: %s", archive)

    # Extract TXT file from ZIP
    with zipfile.ZipFile(archive) as z:
        txt_files = [f for f in z.namelist() if f.endswith('.txt') and '/' not in f]
        if not txt_files:
            raise ValueError(f"No TXT file found in {archive}")

        txt_file = txt_files[0]
        logger.info("Extracting file: %s", txt_file)

        # Extract to temporary location
        temp_path = raw_folder / txt_file
        z.extract(txt_file, path=raw_folder)

    try:
        # Detect delimiter
        delimiter = get_delimiter(temp_path, bytes=16000)
        logger.info("Detected delimiter: %r", delimiter)

        # Load data
        logger.info("Loading data with Polars...")
        df = pl.read_csv(
            temp_path,
            separator=delimiter,
            ignore_errors=True,
            infer_schema_length=10000,
        )

        logger.info("Loaded %d rows, %d columns", len(df), len(df.columns))

        return df

    finally:
        # Clean up extracted file
        if temp_path.exists():
            temp_path.unlink()
            logger.debug("Cleaned up temporary file: %s", temp_path)


if __name__ == "__main__":
    # Load the data
    df = load_2006_data()

    print("\n" + "="*80)
    print("DATAFRAME INFO")
    print("="*80)
    print(f"Shape: {df.shape}")
    print(f"Rows: {len(df):,}")
    print(f"Columns: {len(df.columns)}")

    print("\n" + "="*80)
    print("COLUMN NAMES")
    print("="*80)
    for i, col in enumerate(df.columns, 1):
        print(f"{i:2d}. {col}")

    print("\n" + "="*80)
    print("SCHEMA (DTYPES)")
    print("="*80)
    for col, dtype in df.schema.items():
        print(f"{col:40s} {dtype}")

    print("\n" + "="*80)
    print("FIRST 5 ROWS")
    print("="*80)
    print(df.head())

    print("\n" + "="*80)
    print("SAMPLE OF KEY COLUMNS (first 10 rows)")
    print("="*80)
    # Show first 10 columns
    print(df.select(df.columns[:10]).head(10))

    print("\n" + "="*80)
    print("NULL COUNTS BY COLUMN")
    print("="*80)
    null_counts = df.null_count()
    # Transpose to show as rows
    for col in df.columns:
        count = null_counts[col][0]
        pct = (count / len(df)) * 100 if len(df) > 0 else 0
        print(f"{col:40s} {count:10,} ({pct:5.2f}%)")

    print("\n" + "="*80)
    print("SAMPLE VALUES FOR EACH COLUMN (first unique values)")
    print("="*80)
    for col in df.columns:
        unique_vals = df[col].unique().head(5).to_list()
        print(f"{col:40s} {unique_vals}")
