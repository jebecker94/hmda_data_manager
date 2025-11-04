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

# Import configuration constants
from .config import (
    PROJECT_DIR,
    DATA_DIR,
    RAW_DIR,
    CLEAN_DIR,
    BRONZE_DIR,
    SILVER_DIR,
    get_medallion_dir,
)

# Import data import functions
from .import_data import (
    import_hmda_pre_2007,
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
    # Configuration
    "PROJECT_DIR",
    "DATA_DIR", 
    "RAW_DIR",
    "CLEAN_DIR",
    "BRONZE_DIR",
    "SILVER_DIR",
    "get_medallion_dir",
    
    # Import functions
    "import_hmda_pre_2007",
    "build_bronze_period_2007_2017",
    "build_silver_period_2007_2017",
    "build_bronze_post2018",
    "build_silver_post2018",
    
    # Download functions
    "download_zip_files_from_url",
    "download_hmda_files",
    "determine_raw_subfolder",
]

