"""
Example: Import 2007-2017 HMDA Data
====================================

This example demonstrates how to use the import_2007_2017_workflow function
to import and process HMDA data for years 2007-2017.

The workflow includes:
1. Building Bronze layer (per dataset, one parquet file per archive)
2. Building Silver layer (Hive-partitioned by activity_year and file_type)

Note: Only loans dataset is available for 2007-2017 period.
      Panel and transmittal series data are not available for this period.

Before running:
1. Ensure you have downloaded HMDA data files to data/raw/loans/
   Expected files: hmda_YYYY_nationwide_*_records_codes.zip
2. Adjust the min_year and max_year variables below as needed
3. Ensure you have sufficient disk space for the processed files

Usage:
    python examples/03_example_import_workflow_2007_2017.py

Alternatively, you can use the CLI:
    hmda import 2007-2017 --min-year 2007 --max-year 2017 --drop-tract-vars

Author: Jonathan E. Becker
"""

import logging

# Import the workflow function
from hmda_data_manager import import_2007_2017_workflow


def main():
    """Main workflow for importing 2007-2017 HMDA data."""

    # Set up logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )

    logger = logging.getLogger(__name__)
    logger.info("Starting HMDA 2007-2017 Data Import Workflow")

    # Define years to process (adjust as needed)
    min_year = 2007
    max_year = 2017

    # Execute import workflow
    # This will build both bronze and silver layers for the loans dataset
    success = import_2007_2017_workflow(
        min_year=min_year,
        max_year=max_year,
        drop_tract_vars=True,  # True = drop bulky tract vars (recommended)
        replace=False,  # False = skip existing files, True = replace all
    )

    return success


if __name__ == "__main__":
    success = main()

    print("\n" + "="*60)
    if success:
        print("✅ Import workflow completed successfully!")
        print("="*60)
        print("\nData locations:")
        print("  - Bronze: data/bronze/loans/period_2007_2017/")
        print("  - Silver: data/silver/loans/period_2007_2017/activity_year=YYYY/")
        print("\nNext steps:")
        print("  - Query the silver data using Polars or DuckDB")
        print("  - See other examples in the examples/ directory")
    else:
        print("❌ Import workflow failed!")
        print("="*60)
        print("Check the error messages above for troubleshooting information.")
