#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Example: Download HMDA Data Files
==================================

This example replicates the functionality of the original download_hmda_data.py script
using the new hmda_data_manager package structure. It downloads HMDA files from the
CFPB website and organizes them into appropriate subdirectories.

The script downloads:
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
    python examples/example_download_hmda_data.py

Author: Jonathan E. Becker
"""

import logging
from pathlib import Path

# Import from the new package structure
from hmda_data_manager.core import download_hmda_files, download_zip_files_from_url


def main():
    """Main download routine that replicates the original script functionality."""
    
    # Set up logging (same format as original)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )
    
    logger = logging.getLogger(__name__)
    logger.info("Starting HMDA data download using hmda_data_manager package")

    # Download parameters (same as original script)
    download_mlar = False  # Set to True to download Modified LAR files
    download_historical = False  # Set to True to download 2007-2017 files
    min_static_year = 2018
    max_static_year = 2024
    download_folder = "./data/raw"  # Destination folder for downloads
    
    # Ensure download folder exists
    Path(download_folder).mkdir(parents=True, exist_ok=True)
    
    logger.info(f"Download parameters:")
    logger.info(f"  Years: {min_static_year}-{max_static_year}")
    logger.info(f"  Destination: {download_folder}")
    logger.info(f"  Include MLAR: {download_mlar}")
    logger.info(f"  Include Historical: {download_historical}")

    # Method 1: Use the new convenience function (recommended)
    logger.info("Using convenience function download_hmda_files()...")
    try:
        download_hmda_files(
            years=range(min_static_year, max_static_year + 1),
            destination_folder=download_folder,
            include_mlar=download_mlar,
            include_historical=download_historical,
            # Additional options you can customize:
            # pause_length=5,        # Seconds between downloads
            # wait_time=10,          # Seconds to wait for JS to load
            # overwrite_mode="skip", # "skip", "always", "if_newer", "if_size_diff"
        )
        logger.info("Download completed successfully using convenience function!")
        
    except Exception as e:
        logger.error(f"Error during download: {e}")
        logger.info("Falling back to manual download method...")
        manual_download_method(min_static_year, max_static_year, download_folder, 
                              download_mlar, download_historical)


def manual_download_method(min_year: int, max_year: int, download_folder: str,
                          download_mlar: bool, download_historical: bool):
    """
    Alternative method that replicates the exact logic from the original script.
    
    This method shows how to use the lower-level download_zip_files_from_url function
    to have more control over the download process.
    """
    
    logger = logging.getLogger(__name__)
    logger.info("Using manual download method (exact replica of original script)...")
    
    # Base URLs (same as original script)
    mlar_base_url = "https://ffiec.cfpb.gov/data-publication/modified-lar"
    snapshot_base_url = "https://ffiec.cfpb.gov/data-publication/snapshot-national-loan-level-dataset"
    one_year_base_url = "https://ffiec.cfpb.gov/data-publication/one-year-national-loan-level-dataset"
    three_year_base_url = "https://ffiec.cfpb.gov/data-publication/three-year-national-loan-level-dataset"
    historical_url = "https://www.consumerfinance.gov/data-research/hmda/historic-data/?geo=nationwide&records=all-records&field_descriptions=codes"

    # Download static files (exact replica of original loop)
    logger.info("Downloading static files...")
    for year in range(min_year, max_year + 1):
        logger.info(f"Processing year {year}...")
        for base_url in [snapshot_base_url, one_year_base_url, three_year_base_url]:
            target_url = base_url + f"/{year}"
            logger.info(f"  Downloading from: {target_url}")
            try:
                download_zip_files_from_url(target_url, download_folder)
            except Exception as e:
                logger.error(f"  Error downloading from {target_url}: {e}")

    # Download MLAR files if requested (exact replica of original logic)
    if download_mlar:
        logger.info("Downloading MLAR files...")
        for year in range(min_year, max_year + 1):
            target_url = mlar_base_url + f"/{year}"
            logger.info(f"  Downloading MLAR for {year}: {target_url}")
            try:
                download_zip_files_from_url(target_url, download_folder, download_all=True)
            except Exception as e:
                logger.error(f"  Error downloading MLAR for {year}: {e}")

    # Download historical files if requested (exact replica of original logic)
    if download_historical:
        logger.info("Downloading historical files (2007–2017)...")
        try:
            download_zip_files_from_url(historical_url, download_folder, download_all=True)
        except Exception as e:
            logger.error(f"  Error downloading historical files: {e}")

    logger.info("Manual download method completed!")


def demonstrate_advanced_options():
    """
    Demonstrate advanced download options available in the new package.
    
    This shows capabilities that weren't in the original script but are now available.
    """
    
    logger = logging.getLogger(__name__)
    logger.info("Demonstrating advanced download options...")
    
    # Example: Download with conditional overwrite
    logger.info("Example: Download only if server files are newer...")
    try:
        download_zip_files_from_url(
            "https://ffiec.cfpb.gov/data-publication/snapshot-national-loan-level-dataset/2024",
            "./data/raw",
            overwrite_mode="if_newer",  # Only download if server file is newer
            pause_length=2,             # Shorter pause between downloads
            wait_time=15,               # Longer wait for JavaScript loading
        )
    except Exception as e:
        logger.warning(f"Advanced options demo failed: {e}")

    # Example: Download only pipe-delimited files
    logger.info("Example: Download only pipe-delimited files...")
    try:
        download_zip_files_from_url(
            "https://ffiec.cfpb.gov/data-publication/one-year-national-loan-level-dataset/2024",
            "./data/raw",
            download_csvs=False,        # Don't download CSV format
            download_pipes=True,        # Download pipe-delimited format
            download_all=False,         # Don't download everything
        )
    except Exception as e:
        logger.warning(f"Pipe-delimited demo failed: {e}")


if __name__ == "__main__":
    # Run the main download function
    main()
    
    # Uncomment the line below to see advanced options demonstration
    # demonstrate_advanced_options()
    
    print("\n" + "="*60)
    print("Download script completed!")
    print("="*60)
    print("\nFiles have been downloaded to ./data/raw/ and organized into subdirectories:")
    print("  - loans/              (LAR files)")
    print("  - panel/              (Panel files)")  
    print("  - transmissal_series/ (TS files)")
    print("  - msamd/              (Geography files)")
    print("  - misc/               (Other files)")
    print("\nNext steps:")
    print("  1. Run import examples to process the downloaded files")
    print("  2. See other examples in the examples/ directory")
    print("  3. Check the README.md for more information")
