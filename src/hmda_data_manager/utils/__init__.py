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
from .schema import (
    rename_hmda_columns,
)
from .io import (
    get_delimiter,
    unzip_hmda_file,
    replace_csv_column_names,
)
from .cleaning import (
    clean_hmda,
    replace_na_like_values,
    standardize_schema,
    harmonize_census_tract,
    apply_plausibility_filters,
    clean_rate_spread,
    flag_outliers_basic,
    coerce_numeric_columns,
    downcast_hmda_variables,
)
from .identity import (
    add_identity_keys,
    deduplicate_records,
)
from .export import (
    save_file_to_stata,
    prepare_hmda_for_stata,
)
from .geo import (
    split_and_save_tract_variables,
)

__all__ = [
    # Schema and file handling
    "get_delimiter",
    "unzip_hmda_file",
    "replace_csv_column_names",

    # Data transformation
    "rename_hmda_columns",
    "coerce_numeric_columns",

    # Data cleaning pipelines
    "clean_hmda",
    "harmonize_census_tract",
    "apply_plausibility_filters",
    "clean_rate_spread",
    "flag_outliers_basic",
    "downcast_hmda_variables"

    # Key utilities
    "add_identity_keys",
    "deduplicate_records",
    "replace_na_like_values",
    "standardize_schema",

    # Export functions
    "save_file_to_stata",
    "prepare_hmda_for_stata",
]

