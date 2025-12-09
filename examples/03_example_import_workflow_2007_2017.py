"""
HMDA Data Import Workflow Example - 2007-2017 Data
===================================================

This example demonstrates how to use the hmda_data_manager package to import
and process HMDA data for years 2007-2017 using the modular import functions.

The workflow includes:
1. Setting up paths and configuration
2. Building Bronze (per dataset)
3. Building Silver (hive-partitioned, per file)

"""

import logging
from pathlib import Path

# Import the modular functions
from hmda_data_manager.core import (
    RAW_DIR,
    BRONZE_DIR,
    SILVER_DIR,
    build_bronze_period_2007_2017,
    build_silver_period_2007_2017,
)

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

def main():
    """Main workflow for importing 2007-2017 HMDA data."""

    logger.info("Starting HMDA 2007-2017 Data Import Workflow")
    logger.info("=" * 60)

    # =============================================================================
    # 1. Configuration and Setup
    # =============================================================================

    logger.info("1. Setting up configuration...")

    # Define years to process (adjust as needed)
    min_year = 2007
    max_year = 2017

    # Define folders
    raw_folder = RAW_DIR
    bronze_folder = BRONZE_DIR
    silver_folder = SILVER_DIR

    logger.info(f"  Raw data folder: {raw_folder}")
    logger.info(f"  Bronze data folder: {bronze_folder}")
    logger.info(f"  Silver data folder: {silver_folder}")
    logger.info(f"  Processing years: {min_year} to {max_year}")

    # Ensure bronze/silver directories exist for loans
    # Note: Only loans dataset is available for 2007-2017 period
    (bronze_folder / "loans" / "period_2007_2017").mkdir(parents=True, exist_ok=True)
    (silver_folder / "loans" / "period_2007_2017").mkdir(parents=True, exist_ok=True)

    # =============================================================================
    # 2. Build Bronze (2007-2017)
    # =============================================================================
    logger.info("2. Building Bronze (per dataset)...")

    try:
        build_bronze_period_2007_2017("loans", min_year=min_year, max_year=max_year)
        logger.info("  ✅ Bronze build completed")
    except Exception as e:
        logger.error(f"  ❌ Error building bronze: {e}")
        return False

    # =============================================================================
    # 3. Build Silver (Hive-partitioned)
    # =============================================================================

    logger.info("3. Building Silver (hive-partitioned) for loans...")

    try:
        # drop_tract_vars=True removes bulky census tract summary statistics
        build_silver_period_2007_2017(
            "loans",
            min_year=min_year,
            max_year=max_year,
            replace=True,
            drop_tract_vars=True,
        )
        logger.info("  ✅ Silver build completed")
    except Exception as e:
        logger.error(f"  ❌ Error building silver: {e}")
        return False

    return True

if __name__ == "__main__":
    """
    Run the import workflow.

    Before running:
    1. Ensure you have downloaded HMDA data files to data/raw/loans/
       Expected files: hmda_YYYY_nationwide_*_records_codes.zip
    2. Adjust the min_year and max_year variables above as needed
    3. Ensure you have sufficient disk space for the processed files

    Note: The 2007-2017 period only includes loan-level data (LAR).
          Panel and transmittal series data are not available for this period.

    Usage:
        python examples/03_example_import_workflow_2007_2017.py
    """

    success = main()

    if success:
        print("\n✅ Import workflow completed successfully!")
        print("Check the logs above for detailed information about the imported data.")
    else:
        print("\n❌ Import workflow failed!")
        print("Check the error messages above for troubleshooting information.")
