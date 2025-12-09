"""
HMDA Data Import Workflow Example - Post-2018 Data
==================================================

This example demonstrates how to use the hmda_data_manager package to import
and process HMDA data for years 2018 and later using the modular import functions.

The workflow includes:
1. Setting up paths and configuration
2. Building Bronze (per dataset)
3. Building Silver (hive-partitioned, per file)

"""

import logging
from pathlib import Path

# Import the new modular functions
from hmda_data_manager.core import (
    RAW_DIR,
    BRONZE_DIR,
    SILVER_DIR,
    build_bronze_post2018,
    build_silver_post2018,
)

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

def main():
    """Main workflow for importing post-2018 HMDA data."""
    
    logger.info("Starting HMDA Post-2018 Data Import Workflow")
    logger.info("=" * 60)
    
    # =============================================================================
    # 1. Configuration and Setup
    # =============================================================================
    
    logger.info("1. Setting up configuration...")
    
    # Define years to process (adjust as needed)
    min_year = 2018
    max_year = 2024
    
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
        (bronze_folder / subdir / "post2018").mkdir(parents=True, exist_ok=True)
        (silver_folder / subdir / "post2018").mkdir(parents=True, exist_ok=True)

    # =============================================================================
    # 2. Build Bronze (Post-2018)
    # =============================================================================
    logger.info("2. Building Bronze (per dataset)...")

    try:
        build_bronze_post2018("loans", min_year=min_year, max_year=max_year)
        build_bronze_post2018("panel", min_year=min_year, max_year=max_year)
        build_bronze_post2018("transmissal_series", min_year=min_year, max_year=max_year)
        logger.info("  ✅ Bronze build completed")
    except Exception as e:
        logger.error(f"  ❌ Error building bronze: {e}")
        return False

    # =============================================================================
    # 3. Build Silver (Hive-partitioned)
    # =============================================================================

    logger.info("3. Building Silver (hive-partitioned) for loans/panel/ts...")

    try:
        build_silver_post2018("loans", min_year=min_year, max_year=max_year, replace=True)
        build_silver_post2018("panel", min_year=min_year, max_year=max_year, replace=False)
        build_silver_post2018("transmissal_series", min_year=min_year, max_year=max_year, replace=False)
        logger.info("  ✅ Silver build completed")
    except Exception as e:
        logger.error(f"  ❌ Error building silver: {e}")
        return False

    return True

if __name__ == "__main__":
    """
    Run the import workflow.
    
    Before running:
    1. Ensure you have downloaded HMDA data using 01_example_download_hmda_data.py
    2. Adjust the min_year and max_year variables above as needed
    3. Ensure you have sufficient disk space for the processed files
    
    Usage:
        python examples/02_example_import_workflow_post2018.py
    """
    
    success = main()
    
    if success:
        print("\n✅ Import workflow completed successfully!")
        print("Check the logs above for detailed information about the imported data.")
    else:
        print("\n❌ Import workflow failed!")
        print("Check the error messages above for troubleshooting information.")
