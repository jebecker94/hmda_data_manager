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
from .config import PROJECT_DIR, DATA_DIR, RAW_DIR, CLEAN_DIR

# Import data import functions
from .import_data import (
    import_hmda_pre_2007,
    import_hmda_2007_2017,
    import_hmda_post2018,
    save_to_dataset,
    import_hmda_data,
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
    
    # Import functions
    "import_hmda_pre_2007",
    "import_hmda_2007_2017",
    "import_hmda_post2018",
    "save_to_dataset",
    "import_hmda_data",
    
    # Download functions
    "download_zip_files_from_url",
    "download_hmda_files",
    "determine_raw_subfolder",
]

