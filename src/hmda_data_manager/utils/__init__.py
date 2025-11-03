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
    clean_hmda,
    add_identity_keys,
    replace_na_like_values,
    standardize_schema,
)
from .export import (
    save_file_to_stata,
    prepare_hmda_for_stata,
)
 

__all__ = [
    # Schema and file handling
    "get_file_schema",
    "get_delimiter",
    "unzip_hmda_file",
    "replace_csv_column_names",
    
    # Data transformation
    "rename_hmda_columns",
    
    # Data cleaning pipelines
    "clean_hmda",
    
    # Key utilities
    "add_identity_keys",
    "replace_na_like_values",
    "standardize_schema",
    
    # Export functions
    "save_file_to_stata",
    "prepare_hmda_for_stata",
]

