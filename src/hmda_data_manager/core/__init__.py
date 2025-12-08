"""
Core HMDA Data Management Functionality
========================================

This module contains the primary functions for downloading and importing
HMDA data files.

Modules
-------
- import_data: Functions for importing LAR, panel, and TS files
- download: Functions for downloading HMDA files from CFPB
- config: Configuration and path management

The modules will be populated during migration from root-level scripts:
- import_hmda_data.py -> import_data/ (migrated - modular structure)
- download_hmda_data.py -> download.py
- config.py -> config.py (migrated)
"""

# Import configuration constants and data column lists
from .config import (
    # Path configuration
    PROJECT_DIR,
    DATA_DIR,
    RAW_DIR,
    CLEAN_DIR,
    BRONZE_DIR,
    SILVER_DIR,
    get_medallion_dir,
    # Post-2018 constants
    HMDA_INDEX_COLUMN,
    DERIVED_COLUMNS,
    POST2018_TRACT_COLUMNS,
    POST2018_FLOAT_COLUMNS,
    POST2018_INTEGER_COLUMNS,
    POST2018_EXEMPT_COLUMNS,
    # Pre-2007 constants
    PRE2007_INTEGER_COLUMNS,
    PRE2007_FLOAT_COLUMNS,
    # 2007-2017 constants
    PERIOD_2007_2017_TRACT_COLUMNS,
    PERIOD_2007_2017_INTEGER_COLUMNS,
    PERIOD_2007_2017_FLOAT_COLUMNS,
)

# Import data import functions
from .import_data import (
    build_bronze_pre2007,
    build_silver_pre2007,
    build_bronze_period_2007_2017,
    build_silver_period_2007_2017,
    build_bronze_post2018,
    build_silver_post2018,
)

# Import download functions
from .download import (
    download_zip_files_from_url,
    download_hmda_files,
    determine_raw_subfolder,
)

__all__ = [
    # Path configuration
    "PROJECT_DIR",
    "DATA_DIR",
    "RAW_DIR",
    "CLEAN_DIR",
    "BRONZE_DIR",
    "SILVER_DIR",
    "get_medallion_dir",
    # Post-2018 constants
    "HMDA_INDEX_COLUMN",
    "DERIVED_COLUMNS",
    "POST2018_TRACT_COLUMNS",
    "POST2018_FLOAT_COLUMNS",
    "POST2018_INTEGER_COLUMNS",
    "POST2018_EXEMPT_COLUMNS",
    # Pre-2007 constants
    "PRE2007_INTEGER_COLUMNS",
    "PRE2007_FLOAT_COLUMNS",
    # 2007-2017 constants
    "PERIOD_2007_2017_TRACT_COLUMNS",
    "PERIOD_2007_2017_INTEGER_COLUMNS",
    "PERIOD_2007_2017_FLOAT_COLUMNS",
    # Import functions
    "build_bronze_pre2007",
    "build_silver_pre2007",
    "build_bronze_period_2007_2017",
    "build_silver_period_2007_2017",
    "build_bronze_post2018",
    "build_silver_post2018",
    # Download functions
    "download_zip_files_from_url",
    "download_hmda_files",
    "determine_raw_subfolder",
]

