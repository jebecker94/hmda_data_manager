"""
HMDA Data Import Functions
==========================

This module provides specialized import functions for different HMDA data periods,
each with their own data formats, schemas, and processing requirements.

Time Period Structure
---------------------
- **Pre-2007** (1981-2006): Basic loan application data, simpler format
- **2007-2017**: Standardized format with additional fields
- **Post-2018**: Modern format with extensive demographic and loan details

Modules
-------
- common: Shared utilities and constants used across all periods
- pre2007: Import functions for legacy HMDA data (1981-2006)
- period_2007_2017: Import functions for standardized period data (2007-2017)
- post2018: Import functions for modern HMDA data (2018+)

Main Functions
--------------
- import_hmda_pre_2007: Import legacy format data
- import_hmda_2007_2017: Import standardized format data
 

Example Usage
-------------
>>> from hmda_data_manager.core.import_data import build_bronze_post2018, build_silver_post2018
>>> build_bronze_post2018("loans", min_year=2020, max_year=2024)
>>> build_silver_post2018("loans", min_year=2020, max_year=2024)
"""

# Import main functions from each module
from .pre2007 import import_hmda_pre_2007
from .period_2007_2017 import import_hmda_2007_2017
from .post2018 import (
    build_bronze_post2018,
    build_silver_post2018,
)

__all__ = [
    # Main import functions
    "import_hmda_pre_2007",
    "import_hmda_2007_2017", 
    # Post-processing functions
    "build_bronze_post2018",
    "build_silver_post2018",
]
