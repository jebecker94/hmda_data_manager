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
- import_hmda_post_2018: Import modern format data
- clean_hmda_post_2017: Clean and standardize post-2017 data
- save_to_dataset: Save processed data to partitioned datasets

Example Usage
-------------
>>> from hmda_data_manager.core.import_data import import_hmda_post_2018
>>> import_hmda_post_2018(raw_folder, clean_folder, schema_file, 2020, 2024)

>>> from hmda_data_manager.core.import_data import import_hmda_data
>>> import_hmda_data(year_range=(2018, 2024), data_folder=raw_folder)
"""

# Import main functions from each module
from .pre2007 import import_hmda_pre_2007
from .period_2007_2017 import import_hmda_2007_2017
from .post2018 import import_hmda_post_2018, clean_hmda_post_2017
from .common import save_to_dataset

__all__ = [
    # Main import functions
    "import_hmda_pre_2007",
    "import_hmda_2007_2017", 
    "import_hmda_post_2018",
    
    # Post-processing functions
    "clean_hmda_post_2017",
    "save_to_dataset",
    
    # Convenience function
    "import_hmda_data",
]


def import_hmda_data(year_range: tuple[int, int], **kwargs):
    """
    Convenience function that routes to appropriate import function based on year range.
    
    Parameters
    ----------
    year_range : tuple[int, int]
        (min_year, max_year) tuple defining the years to import
    **kwargs
        Additional arguments passed to the specific import function
        
    Returns
    -------
    None
        
    Examples
    --------
    >>> # Import modern data
    >>> import_hmda_data((2018, 2024), data_folder=raw_dir, save_folder=clean_dir)
    
    >>> # Import legacy data  
    >>> import_hmda_data((1990, 2006), data_folder=raw_dir, save_folder=clean_dir)
    """
    min_year, max_year = year_range
    
    if max_year <= 2006:
        return import_hmda_pre_2007(min_year=min_year, max_year=max_year, **kwargs)
    elif min_year >= 2007 and max_year <= 2017:
        return import_hmda_2007_2017(min_year=min_year, max_year=max_year, **kwargs)  
    elif min_year >= 2018:
        return import_hmda_post_2018(min_year=min_year, max_year=max_year, **kwargs)
    else:
        raise ValueError(
            f"Year range {year_range} spans multiple data format periods. "
            "Please use period-specific import functions or split into separate ranges."
        )
