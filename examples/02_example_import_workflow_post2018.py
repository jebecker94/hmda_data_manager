"""
Example: Import Post-2018 HMDA Data
====================================

This example demonstrates how to use the import_post2018_workflow function
to import and process HMDA data for years 2018 and later.

The workflow includes:
1. Building Bronze layer (per dataset, one parquet file per archive)
2. Building Silver layer (Hive-partitioned by activity_year and file_type)

Before running:
1. Ensure you have downloaded HMDA data using 01_example_download_hmda_data.py
2. Adjust the min_year and max_year variables below as needed
3. Ensure you have sufficient disk space for the processed files

Usage:
    python examples/02_example_import_workflow_post2018.py

Alternatively, you can use the CLI:
    hmda import post2018 --min-year 2018 --max-year 2024

Author: Jonathan E. Becker
"""

import logging

# Import the workflow function
from hmda_data_manager import import_post2018_workflow


def main():
    """Main workflow for importing post-2018 HMDA data."""

    # Set up logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )

    logger = logging.getLogger(__name__)
    logger.info("Starting HMDA Post-2018 Data Import Workflow")

    # Define years to process (adjust as needed)
    min_year = 2018
    max_year = 2024

    # Execute import workflow
    # This will build both bronze and silver layers for all three datasets:
    # - loans
    # - panel
    # - transmissal_series
    results = import_post2018_workflow(
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
        print("  - Bronze: data/bronze/{dataset}/post2018/")
        print("  - Silver: data/silver/{dataset}/post2018/activity_year=YYYY/file_type=X/")
        print("\nNext steps:")
        print("  - Query the silver data using Polars or DuckDB")
        print("  - See other examples in the examples/ directory")
    else:
        print("❌ Import workflow failed!")
        print("Check the error messages above for troubleshooting information.")
