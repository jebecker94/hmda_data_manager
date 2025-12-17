"""
Example: Import Pre-2007 HMDA Data
===================================

This example demonstrates how to use the import_pre2007_workflow function
to import and process HMDA data for years 1990-2006.

The workflow includes:
1. Building Bronze layer (per dataset, one parquet file per archive)
2. Building Silver layer (Hive-partitioned by activity_year)

Data Source:
-----------
Forrester, Andrew. Historical Home Mortgage Disclosure Act (HMDA) Data.
Ann Arbor, MI: Inter-university Consortium for Political and Social Research
[distributor], V1 (2021). https://doi.org/10.3886/E151921V1

Note: Pre-2007 data uses fixed-width format converted to delimited files.
      1981-1989 data is excluded (different schema and data structure).

Before running:
1. Download raw data from openICPSR Project 151921
   https://www.openicpsr.org/openicpsr/project/151921/version/V1/view
2. Place ZIP files in data/raw/{loans,panel,transmissal_series}/
   Expected files: HMDA_YYYY.zip (e.g., HMDA_2006.zip)
3. Adjust the min_year and max_year variables below as needed
4. Ensure you have sufficient disk space for the processed files

Schema Evolution:
- 1990-2003: 23 columns (basic format)
- 2004-2006: 38 columns (expanded with race, ethnicity, rate spread)

Usage:
    python examples/04_example_import_workflow_pre2007.py

Alternatively, you can use the CLI:
    hmda import pre2007 --min-year 1990 --max-year 2006

Author: Jonathan E. Becker
"""

import logging

# Import the workflow function
from hmda_data_manager import import_pre2007_workflow


def main():
    """Main workflow for importing pre-2007 HMDA data."""

    # Set up logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )

    logger = logging.getLogger(__name__)
    logger.info("Starting HMDA Pre-2007 Data Import Workflow")

    # Define years to process (adjust as needed)
    # Note: 1981-1989 excluded due to different schema
    min_year = 1990
    max_year = 2006

    # Execute import workflow
    # This will build both bronze and silver layers for all three datasets:
    # - loans
    # - panel
    # - transmissal_series
    results = import_pre2007_workflow(
        min_year=min_year,
        max_year=max_year,
        datasets=None,  # None = process all datasets
        replace=False,  # False = skip existing files, True = replace all
    )

    # Report results
    print("\n" + "="*60)
    print("Import Workflow Summary")
    print("="*60)
    for dataset, success in results.items():
        status = "✅" if success else "❌"
        result = "Success" if success else "Failed"
        print(f"{status} {dataset}: {result}")
    print("")

    # Return overall success
    return all(results.values())


if __name__ == "__main__":
    success = main()

    if success:
        print("✅ Import workflow completed successfully!")
        print("\nData locations:")
        print("  - Bronze: data/bronze/{dataset}/pre2007/")
        print("  - Silver: data/silver/{dataset}/pre2007/activity_year=YYYY/")
        print("\nNext steps:")
        print("  - Query the silver data using Polars or DuckDB")
        print("  - See other examples in the examples/ directory")
    else:
        print("❌ Import workflow failed!")
        print("Check the error messages above for troubleshooting information.")
