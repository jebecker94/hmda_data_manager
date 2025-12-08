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
- build_bronze_pre2007: Create bronze layer for pre-2007 data
- build_silver_pre2007: Create silver layer for pre-2007 data
- build_bronze_period_2007_2017: Create bronze layer for 2007-2017 data
- build_silver_period_2007_2017: Create silver layer for 2007-2017 data
- build_bronze_post2018: Create bronze layer for post-2018 data
- build_silver_post2018: Create silver layer for post-2018 data

Example Usage
-------------
>>> from hmda_data_manager.core.import_data import build_bronze_post2018, build_silver_post2018
>>> build_bronze_post2018("loans", min_year=2020, max_year=2024)
>>> build_silver_post2018("loans", min_year=2020, max_year=2024)
"""

# Import main functions from each module
from .pre2007 import (
    build_bronze_pre2007,
    build_silver_pre2007,
)
from .period_2007_2017 import (
    build_bronze_period_2007_2017,
    build_silver_period_2007_2017,
)
from .post2018 import (
    build_bronze_post2018,
    build_silver_post2018,
)

__all__ = [
    # Pre-2007 medallion builders
    "build_bronze_pre2007",
    "build_silver_pre2007",
    # 2007-2017 medallion builders
    "build_bronze_period_2007_2017",
    "build_silver_period_2007_2017",
    # Post-2018 medallion builders
    "build_bronze_post2018",
    "build_silver_post2018",
]
