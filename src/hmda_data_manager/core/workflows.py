"""
HMDA Data Manager Workflows
============================

High-level orchestration functions for common HMDA data workflows.

This module provides convenient wrapper functions that combine download, bronze,
and silver layer operations into complete workflows. These functions are designed
to be used both programmatically and via the CLI.

Functions
---------
- download_workflow: Download HMDA data files from CFPB
- import_post2018_workflow: Complete post-2018 import pipeline (bronze + silver)
- import_2007_2017_workflow: Complete 2007-2017 import pipeline (bronze + silver)
- import_pre2007_workflow: Complete pre-2007 import pipeline (bronze + silver)

Example Usage
-------------
>>> from hmda_data_manager.core.workflows import (
...     download_workflow,
...     import_post2018_workflow,
... )

>>> # Download data
>>> download_workflow(years=range(2018, 2025))

>>> # Import and process
>>> results = import_post2018_workflow(min_year=2018, max_year=2024)
>>> print(results)
{'loans': True, 'panel': True, 'transmissal_series': True}
"""

import logging
from pathlib import Path
from typing import Any

from .config import RAW_DIR, BRONZE_DIR, SILVER_DIR
from .download import download_hmda_files
from .import_data import (
    build_bronze_post2018,
    build_silver_post2018,
    build_bronze_period_2007_2017,
    build_silver_period_2007_2017,
)
from .import_data.pre2007 import (
    build_bronze_pre2007,
    build_silver_pre2007,
)

logger = logging.getLogger(__name__)


def download_workflow(
    years: range,
    destination_folder: Path | str | None = None,
    include_mlar: bool = False,
    include_historical: bool = False,
    **kwargs: Any,
) -> None:
    """
    Download HMDA data files from CFPB website.

    This is a convenience wrapper around download_hmda_files that provides
    sensible defaults and logging for the download workflow.

    Parameters
    ----------
    years : range
        Range of years to download (e.g., range(2018, 2025))
    destination_folder : Path | str | None, optional
        Destination folder for downloads. If None, uses configured RAW_DIR
    include_mlar : bool, default False
        Whether to download Modified LAR (MLAR) files
    include_historical : bool, default False
        Whether to download historical 2007-2017 files
    **kwargs : Any
        Additional keyword arguments to pass to download_hmda_files:
        - pause_length: Seconds between downloads (default: 5)
        - wait_time: Seconds to wait for JavaScript to load (default: 10)
        - overwrite_mode: "skip", "always", "if_newer", "if_size_diff" (default: "skip")
        - download_csvs: Whether to download CSV format (default: False)
        - download_pipes: Whether to download pipe-delimited format (default: True)

    Returns
    -------
    None

    Examples
    --------
    >>> # Download post-2018 data
    >>> download_workflow(years=range(2018, 2025))

    >>> # Download with MLAR files
    >>> download_workflow(
    ...     years=range(2020, 2025),
    ...     include_mlar=True,
    ... )

    >>> # Download to custom location
    >>> download_workflow(
    ...     years=range(2018, 2025),
    ...     destination_folder="./my_data",
    ... )

    Notes
    -----
    - Files are automatically organized into subdirectories:
      loans/, panel/, transmissal_series/, msamd/, misc/
    - Download progress is logged to INFO level
    - Existing files are skipped by default (use overwrite_mode to change)
    """
    if destination_folder is None:
        destination_folder = RAW_DIR

    dest_path = Path(destination_folder)
    dest_path.mkdir(parents=True, exist_ok=True)

    logger.info("=" * 60)
    logger.info("HMDA Download Workflow")
    logger.info("=" * 60)
    logger.info(f"Years: {min(years)}-{max(years)}")
    logger.info(f"Destination: {dest_path}")
    logger.info(f"Include MLAR: {include_mlar}")
    logger.info(f"Include Historical: {include_historical}")
    logger.info("")

    try:
        download_hmda_files(
            years=years,
            destination_folder=str(dest_path),
            include_mlar=include_mlar,
            include_historical=include_historical,
            **kwargs,
        )
        logger.info("")
        logger.info("✅ Download workflow completed successfully!")

    except Exception as e:
        logger.error(f"❌ Download workflow failed: {e}")
        raise


def import_post2018_workflow(
    min_year: int = 2018,
    max_year: int = 2024,
    datasets: list[str] | None = None,
    replace: bool = False,
) -> dict[str, bool]:
    """
    Complete post-2018 HMDA import workflow (bronze + silver layers).

    This workflow:
    1. Builds bronze layer (one parquet file per archive)
    2. Builds silver layer (Hive-partitioned by activity_year and file_type)

    Parameters
    ----------
    min_year : int, default 2018
        Minimum year to process
    max_year : int, default 2024
        Maximum year to process
    datasets : list[str] | None, optional
        List of datasets to process. If None, processes all three:
        ["loans", "panel", "transmissal_series"]
    replace : bool, default False
        Whether to replace existing files (True) or skip them (False)

    Returns
    -------
    dict[str, bool]
        Dictionary mapping dataset name to success status (True/False)

    Examples
    --------
    >>> # Process all datasets for 2018-2024
    >>> results = import_post2018_workflow(min_year=2018, max_year=2024)
    >>> print(results)
    {'loans': True, 'panel': True, 'transmissal_series': True}

    >>> # Process only loans dataset
    >>> results = import_post2018_workflow(
    ...     min_year=2020,
    ...     max_year=2024,
    ...     datasets=["loans"],
    ... )

    >>> # Replace existing files
    >>> results = import_post2018_workflow(
    ...     min_year=2018,
    ...     max_year=2024,
    ...     replace=True,
    ... )

    Notes
    -----
    - Bronze layer: data/bronze/{dataset}/post2018/
    - Silver layer: data/silver/{dataset}/post2018/activity_year=YYYY/file_type=X/
    - HMDAIndex unique identifier is automatically created
    - Processing continues even if individual datasets fail
    """
    if datasets is None:
        datasets = ["loans", "panel", "transmissal_series"]

    logger.info("=" * 60)
    logger.info("HMDA Post-2018 Import Workflow")
    logger.info("=" * 60)
    logger.info(f"Years: {min_year}-{max_year}")
    logger.info(f"Datasets: {', '.join(datasets)}")
    logger.info(f"Replace existing: {replace}")
    logger.info("")

    results = {}

    # Ensure directories exist
    for dataset in datasets:
        (BRONZE_DIR / dataset / "post2018").mkdir(parents=True, exist_ok=True)
        (SILVER_DIR / dataset / "post2018").mkdir(parents=True, exist_ok=True)

    # Build Bronze Layer
    logger.info("Step 1: Building Bronze Layer")
    logger.info("-" * 60)
    for dataset in datasets:
        try:
            logger.info(f"Processing {dataset}...")
            build_bronze_post2018(dataset, min_year=min_year, max_year=max_year)
            logger.info(f"✅ Bronze {dataset} completed")
        except Exception as e:
            logger.error(f"❌ Bronze {dataset} failed: {e}")
            results[dataset] = False
            continue

    logger.info("")

    # Build Silver Layer
    logger.info("Step 2: Building Silver Layer (Hive-partitioned)")
    logger.info("-" * 60)
    for dataset in datasets:
        if dataset in results and not results[dataset]:
            logger.info(f"⏭️  Skipping {dataset} (bronze build failed)")
            continue

        try:
            logger.info(f"Processing {dataset}...")
            build_silver_post2018(
                dataset,
                min_year=min_year,
                max_year=max_year,
                replace=replace,
            )
            logger.info(f"✅ Silver {dataset} completed")
            results[dataset] = True
        except Exception as e:
            logger.error(f"❌ Silver {dataset} failed: {e}")
            results[dataset] = False

    logger.info("")
    logger.info("=" * 60)
    logger.info("Workflow Summary")
    logger.info("=" * 60)
    for dataset, success in results.items():
        status = "✅" if success else "❌"
        logger.info(f"{status} {dataset}: {'Success' if success else 'Failed'}")
    logger.info("")

    return results


def import_2007_2017_workflow(
    min_year: int = 2007,
    max_year: int = 2017,
    drop_tract_vars: bool = True,
    replace: bool = False,
) -> bool:
    """
    Complete 2007-2017 HMDA import workflow (bronze + silver layers).

    This workflow:
    1. Builds bronze layer (one parquet file per archive)
    2. Builds silver layer (Hive-partitioned by activity_year and file_type)

    Parameters
    ----------
    min_year : int, default 2007
        Minimum year to process
    max_year : int, default 2017
        Maximum year to process
    drop_tract_vars : bool, default True
        Whether to drop bulky census tract summary statistics.
        Recommended: True (significantly reduces file size)
    replace : bool, default False
        Whether to replace existing files (True) or skip them (False)

    Returns
    -------
    bool
        True if workflow completed successfully, False otherwise

    Examples
    --------
    >>> # Process all years with default settings
    >>> success = import_2007_2017_workflow(min_year=2007, max_year=2017)

    >>> # Keep tract variables (larger files)
    >>> success = import_2007_2017_workflow(
    ...     min_year=2007,
    ...     max_year=2017,
    ...     drop_tract_vars=False,
    ... )

    >>> # Process subset of years
    >>> success = import_2007_2017_workflow(min_year=2010, max_year=2015)

    Notes
    -----
    - Only loans dataset is available for 2007-2017 period
    - Panel and transmittal series data not available for this period
    - Bronze layer: data/bronze/loans/period_2007_2017/
    - Silver layer: data/silver/loans/period_2007_2017/activity_year=YYYY/
    """
    dataset = "loans"  # Only loans available for this period

    logger.info("=" * 60)
    logger.info("HMDA 2007-2017 Import Workflow")
    logger.info("=" * 60)
    logger.info(f"Years: {min_year}-{max_year}")
    logger.info(f"Dataset: {dataset}")
    logger.info(f"Drop tract variables: {drop_tract_vars}")
    logger.info(f"Replace existing: {replace}")
    logger.info("")

    # Ensure directories exist
    (BRONZE_DIR / dataset / "period_2007_2017").mkdir(parents=True, exist_ok=True)
    (SILVER_DIR / dataset / "period_2007_2017").mkdir(parents=True, exist_ok=True)

    # Build Bronze Layer
    logger.info("Step 1: Building Bronze Layer")
    logger.info("-" * 60)
    try:
        logger.info(f"Processing {dataset}...")
        build_bronze_period_2007_2017(dataset, min_year=min_year, max_year=max_year)
        logger.info(f"✅ Bronze {dataset} completed")
    except Exception as e:
        logger.error(f"❌ Bronze {dataset} failed: {e}")
        logger.info("")
        return False

    logger.info("")

    # Build Silver Layer
    logger.info("Step 2: Building Silver Layer (Hive-partitioned)")
    logger.info("-" * 60)
    try:
        logger.info(f"Processing {dataset}...")
        build_silver_period_2007_2017(
            dataset,
            min_year=min_year,
            max_year=max_year,
            replace=replace,
            drop_tract_vars=drop_tract_vars,
        )
        logger.info(f"✅ Silver {dataset} completed")
    except Exception as e:
        logger.error(f"❌ Silver {dataset} failed: {e}")
        logger.info("")
        return False

    logger.info("")
    logger.info("=" * 60)
    logger.info("✅ Workflow completed successfully!")
    logger.info("=" * 60)
    logger.info("")

    return True


def import_pre2007_workflow(
    min_year: int = 1990,
    max_year: int = 2006,
    datasets: list[str] | None = None,
    replace: bool = False,
) -> dict[str, bool]:
    """
    Complete pre-2007 HMDA import workflow (bronze + silver layers).

    This workflow:
    1. Builds bronze layer (one parquet file per archive)
    2. Builds silver layer (Hive-partitioned by activity_year)

    Parameters
    ----------
    min_year : int, default 1990
        Minimum year to process (must be >= 1990)
    max_year : int, default 2006
        Maximum year to process (must be <= 2006)
    datasets : list[str] | None, optional
        List of datasets to process. If None, processes all three:
        ["loans", "panel", "transmissal_series"]
    replace : bool, default False
        Whether to replace existing files (True) or skip them (False)

    Returns
    -------
    dict[str, bool]
        Dictionary mapping dataset name to success status (True/False)

    Examples
    --------
    >>> # Process all datasets for 1990-2006
    >>> results = import_pre2007_workflow(min_year=1990, max_year=2006)

    >>> # Process only loans dataset
    >>> results = import_pre2007_workflow(
    ...     min_year=1990,
    ...     max_year=2006,
    ...     datasets=["loans"],
    ... )

    >>> # Process subset of years
    >>> results = import_pre2007_workflow(min_year=2000, max_year=2006)

    Notes
    -----
    - Data source: openICPSR Project 151921
    - 1981-1989 excluded due to different schema
    - Bronze layer: data/bronze/{dataset}/pre2007/
    - Silver layer: data/silver/{dataset}/pre2007/activity_year=YYYY/
    - Schema changes: 1990-2003 (23 columns), 2004-2006 (38 columns)
    - Processing continues even if individual datasets fail
    """
    if datasets is None:
        datasets = ["loans", "panel", "transmissal_series"]

    logger.info("=" * 60)
    logger.info("HMDA Pre-2007 Import Workflow")
    logger.info("=" * 60)
    logger.info(f"Years: {min_year}-{max_year}")
    logger.info(f"Datasets: {', '.join(datasets)}")
    logger.info(f"Replace existing: {replace}")
    logger.info("")

    results = {}

    # Ensure directories exist
    for dataset in datasets:
        (BRONZE_DIR / dataset / "pre2007").mkdir(parents=True, exist_ok=True)
        (SILVER_DIR / dataset / "pre2007").mkdir(parents=True, exist_ok=True)

    # Build Bronze Layer
    logger.info("Step 1: Building Bronze Layer")
    logger.info("-" * 60)
    for dataset in datasets:
        try:
            logger.info(f"Processing {dataset}...")
            build_bronze_pre2007(dataset, min_year=min_year, max_year=max_year)
            logger.info(f"✅ Bronze {dataset} completed")
        except Exception as e:
            logger.error(f"❌ Bronze {dataset} failed: {e}")
            results[dataset] = False
            continue

    logger.info("")

    # Build Silver Layer
    logger.info("Step 2: Building Silver Layer (Hive-partitioned)")
    logger.info("-" * 60)
    for dataset in datasets:
        if dataset in results and not results[dataset]:
            logger.info(f"⏭️  Skipping {dataset} (bronze build failed)")
            continue

        try:
            logger.info(f"Processing {dataset}...")
            build_silver_pre2007(
                dataset,
                min_year=min_year,
                max_year=max_year,
                replace=replace,
            )
            logger.info(f"✅ Silver {dataset} completed")
            results[dataset] = True
        except Exception as e:
            logger.error(f"❌ Silver {dataset} failed: {e}")
            results[dataset] = False

    logger.info("")
    logger.info("=" * 60)
    logger.info("Workflow Summary")
    logger.info("=" * 60)
    for dataset, success in results.items():
        status = "✅" if success else "❌"
        logger.info(f"{status} {dataset}: {'Success' if success else 'Failed'}")
    logger.info("")

    return results


__all__ = [
    "download_workflow",
    "import_post2018_workflow",
    "import_2007_2017_workflow",
    "import_pre2007_workflow",
]
