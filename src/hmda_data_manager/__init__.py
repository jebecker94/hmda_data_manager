"""
HMDA Data Manager
=================

Tools for managing CFPB's Home Mortgage Disclosure Act (HMDA) data for research.

This package provides functionality for:
- Downloading HMDA data files from CFPB
- Importing and processing loan-level (LAR) data
- Importing panel and transmittal sheet data
- Creating summary statistics and reports
- Data cleaning and validation utilities

Main Modules
------------
- core: Core data import and download functionality
- utils: Utility functions for data processing and schema handling
- schemas: HTML schema documentation for HMDA data files

Example Usage
-------------
>>> # Import and download functions
>>> from hmda_data_manager.core import download_hmda_files
>>> from hmda_data_manager.utils import get_file_schema
>>> from hmda_data_manager.core.lenders.post2018 import combine_lenders_panel_ts_post2018
>>> from hmda_data_manager.schemas import get_schema_path

>>> # Download data for recent years
>>> download_hmda_files(range(2020, 2025))

>>> # Import data using schema
>>> schema_file = get_schema_path("hmda_lar_schema_post2018")
>>> # Use build_bronze_post2018/build_silver_post2018 for post-2018 processing

For detailed examples, see the examples/ directory in the repository.

Notes
-----
The HMDAIndex variable is automatically created for post2018 data to provide
The HMDAIndex variable is automatically created for post2018 data to provide
unique identifiers across HMDA releases. Format: YYYYt_######### where YYYY is
the year, t is the file type code, and # is the zero-padded row number.

For loan matching functionality (matching originations to purchases), see the
separate hmda-matching project.
"""

__version__ = "0.2.0"
__author__ = "Jonathan E. Becker"

# Public API will be populated as we migrate modules
# For now, this serves as the package entry point

__all__ = [
    "__version__",
    "__author__",
]

