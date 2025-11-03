"""
Utility Functions for HMDA Data Processing
===========================================

This module contains helper functions for data processing, schema handling,
file manipulation, and data transformations.

Modules
-------
- support: Core support functions (schema, column renaming, destringing, etc.)
- summary: Functions for creating summary files and statistics

The modules will be populated during migration from root-level scripts:
- import_support_functions.py -> support.py (migrated)
- create_summary_files.py -> summary.py (migrated)
"""

# Import key functions from support module
from .support import (
    get_file_schema,
    get_delimiter,
    unzip_hmda_file,
    rename_hmda_columns,
    replace_csv_column_names,
    destring_hmda_cols_pre2007,
    destring_hmda_cols_2007_2017,
    clean_hmda,
    clean_hmda_polars,
    add_identity_keys,
    add_identity_keys_polars,
    replace_na_like_values,
    replace_na_like_values_polars,
    standardize_schema,
    standardize_schema_polars,
    save_file_to_stata,
)
 

__all__ = [
    # Schema and file handling
    "get_file_schema",
    "get_delimiter",
    "unzip_hmda_file",
    "replace_csv_column_names",
    
    # Data transformation
    "rename_hmda_columns",
    "destring_hmda_cols_pre2007",
    "destring_hmda_cols_2007_2017", 
    
    # Data cleaning pipelines
    "clean_hmda",
    "clean_hmda_polars",
    
    # Key utilities
    "add_identity_keys",
    "add_identity_keys_polars",
    "replace_na_like_values",
    "replace_na_like_values_polars",
    "standardize_schema",
    "standardize_schema_polars",
    
    # Export functions
    "save_file_to_stata",
]

