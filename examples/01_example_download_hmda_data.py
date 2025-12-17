#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Example: Download HMDA Data Files
==================================

This example demonstrates how to use the download_workflow function to download
HMDA files from the CFPB website.

The workflow downloads:
- Snapshot datasets (current year data)
- One-year datasets
- Three-year datasets
- Optionally: Modified LAR (MLAR) files
- Optionally: Historical files (2007-2017)

Files are automatically organized into subdirectories:
- loans/ (LAR files)
- panel/ (panel files)
- transmissal_series/ (TS files)
- msamd/ (geography files)
- misc/ (other files)

Usage:
    python examples/01_example_download_hmda_data.py

Alternatively, you can use the CLI:
    hmda download --years 2018-2024

Author: Jonathan E. Becker
"""

import logging
from pathlib import Path

# Import the workflow function
from hmda_data_manager import download_workflow


def main():
    """Main download routine using the download_workflow function."""

    # Set up logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )

    logger = logging.getLogger(__name__)
    logger.info("Starting HMDA data download using workflow function")

    # Download parameters
    min_year = 2018
    max_year = 2024
    download_mlar = False  # Set to True to download Modified LAR files
    download_historical = False  # Set to True to download 2007-2017 files
    download_folder = "./data/raw"  # Destination folder for downloads

    # Ensure download folder exists
    Path(download_folder).mkdir(parents=True, exist_ok=True)

    # Execute download workflow
    try:
        download_workflow(
            years=range(min_year, max_year + 1),
            destination_folder=download_folder,
            include_mlar=download_mlar,
            include_historical=download_historical,
            # Additional options you can customize:
            # pause_length=5,        # Seconds between downloads
            # wait_time=10,          # Seconds to wait for JS to load
            # overwrite_mode="skip", # "skip", "always", "if_newer", "if_size_diff"
        )

        logger.info("Download completed successfully!")

    except Exception as e:
        logger.error(f"Error during download: {e}")
        return False

    return True


if __name__ == "__main__":
    success = main()

    print("\n" + "="*60)
    if success:
        print("✅ Download script completed successfully!")
    else:
        print("❌ Download script failed!")
    print("="*60)

    if success:
        print("\nFiles have been downloaded to ./data/raw/ and organized into subdirectories:")
        print("  - loans/              (LAR files)")
        print("  - panel/              (Panel files)")
        print("  - transmissal_series/ (TS files)")
        print("  - msamd/              (Geography files)")
        print("  - misc/               (Other files)")
        print("\nNext steps:")
        print("  1. Run import examples to process the downloaded files")
        print("  2. See 02_example_import_workflow_post2018.py")
        print("\nAlternatively, use the CLI:")
        print("  hmda import post2018 --min-year 2018 --max-year 2024")
