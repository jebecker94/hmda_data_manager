"""
HMDA Data Import Workflow Example - Pre-2007 Data
==================================================

This example demonstrates how to use the hmda_data_manager package to import
and process HMDA data for years 1990-2006 using the modular import functions.

The workflow includes:
1. Setting up paths and configuration
2. Building Bronze (per dataset)
3. Building Silver (hive-partitioned, per file)

Data Source:
-----------
Forrester, Andrew. Historical Home Mortgage Disclosure Act (HMDA) Data.
Ann Arbor, MI: Inter-university Consortium for Political and Social Research
[distributor], V1 (2021). https://doi.org/10.3886/E151921V1

Note: Pre-2007 data uses fixed-width format converted to delimited files.
      1981-1989 data is excluded (different schema and data structure).

"""

import logging
from pathlib import Path

# Import the modular functions
from hmda_data_manager.core import (
    RAW_DIR,
    BRONZE_DIR,
    SILVER_DIR,
)
from hmda_data_manager.core.import_data.pre2007 import (
    build_bronze_pre2007,
    build_silver_pre2007,
)

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

def main():
    """Main workflow for importing pre-2007 HMDA data."""

    logger.info("Starting HMDA Pre-2007 Data Import Workflow")
    logger.info("=" * 60)

    # =============================================================================
    # 1. Configuration and Setup
    # =============================================================================

    logger.info("1. Setting up configuration...")

    # Define years to process (adjust as needed)
    # Note: 1981-1989 excluded due to different schema
    min_year = 1990
    max_year = 2006

    # Define folders
    raw_folder = RAW_DIR
    bronze_folder = BRONZE_DIR
    silver_folder = SILVER_DIR

    logger.info(f"  Raw data folder: {raw_folder}")
    logger.info(f"  Bronze data folder: {bronze_folder}")
    logger.info(f"  Silver data folder: {silver_folder}")
    logger.info(f"  Processing years: {min_year} to {max_year}")

    # Ensure bronze/silver directories exist for datasets
    for subdir in ["loans", "panel", "transmissal_series"]:
        (bronze_folder / subdir / "pre2007").mkdir(parents=True, exist_ok=True)
        (silver_folder / subdir / "pre2007").mkdir(parents=True, exist_ok=True)

    # =============================================================================
    # 2. Build Bronze (Pre-2007)
    # =============================================================================
    logger.info("2. Building Bronze (per dataset)...")

    try:
        build_bronze_pre2007("loans", min_year=min_year, max_year=max_year)
        build_bronze_pre2007("panel", min_year=min_year, max_year=max_year)
        build_bronze_pre2007("transmissal_series", min_year=min_year, max_year=max_year)
        logger.info("  ✅ Bronze build completed")
    except Exception as e:
        logger.error(f"  ❌ Error building bronze: {e}")
        return False

    # =============================================================================
    # 3. Build Silver (Hive-partitioned)
    # =============================================================================

    logger.info("3. Building Silver (hive-partitioned) for loans/panel/ts...")

    try:
        build_silver_pre2007("loans", min_year=min_year, max_year=max_year, replace=True)
        build_silver_pre2007("panel", min_year=min_year, max_year=max_year, replace=True)
        build_silver_pre2007("transmissal_series", min_year=min_year, max_year=max_year, replace=True)
        logger.info("  ✅ Silver build completed")
    except Exception as e:
        logger.error(f"  ❌ Error building silver: {e}")
        return False

    return True

if __name__ == "__main__":
    """
    Run the import workflow.

    Before running:
    1. Download raw data from openICPSR Project 151921
       https://www.openicpsr.org/openicpsr/project/151921/version/V1/view
    2. Place ZIP files in data/raw/{loans,panel,transmissal_series}/
       Expected files: HMDA_YYYY.zip (e.g., HMDA_2006.zip)
    3. Adjust the min_year and max_year variables above as needed
    4. Ensure you have sufficient disk space for the processed files

    Schema Evolution:
    - 1990-2003: 23 columns (basic format)
    - 2004-2006: 38 columns (expanded with race, ethnicity, rate spread)

    Usage:
        python examples/04_example_import_workflow_pre2007.py
    """

    success = main()

    if success:
        print("\n✅ Import workflow completed successfully!")
        print("Check the logs above for detailed information about the imported data.")
    else:
        print("\n❌ Import workflow failed!")
        print("Check the error messages above for troubleshooting information.")
