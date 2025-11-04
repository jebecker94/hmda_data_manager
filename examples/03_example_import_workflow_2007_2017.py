"""
Example: Import HMDA loans for 2007–2017 using the medallion workflow.

This script builds the bronze and silver layers for the 2007–2017 period.
It assumes you have already downloaded the raw ZIP archives into
``data/raw/loans`` (e.g., files matching "hmda_2010_nationwide_*_records_codes.zip").

Outputs:
- Bronze: ``data/bronze/loans/period_2007_2017/*.parquet`` (one per archive)
- Silver: ``data/silver/loans/period_2007_2017/activity_year=YYYY/file_type=X/*.parquet``
"""

from __future__ import annotations

from pathlib import Path
import polars as pl

from hmda_data_manager.core import (
    build_bronze_period_2007_2017,
    build_silver_period_2007_2017,
    get_medallion_dir,
)

def main(min_year: int = 2007, max_year: int = 2017) -> None:
    # Build bronze (minimal transforms, one parquet per archive)
    build_bronze_period_2007_2017(
        "loans",
        min_year=min_year,
        max_year=max_year,
        replace=False,
    )

    # Build silver (partitioned by activity_year, file_type)
    build_silver_period_2007_2017(
        "loans",
        min_year=min_year,
        max_year=max_year,
        replace=False,
        drop_tract_vars=True,  # Drop tract summary variables like post-2018
    )

    # Quick verification: scan the full silver folder and show a small sample
    silver_dir = get_medallion_dir("silver", "loans", "period_2007_2017")
    lf = pl.scan_parquet(silver_dir)
    try:
        cnt = lf.select(pl.len()).collect().item()
    except Exception:
        cnt = None

    print(f"Silver folder: {silver_dir}")
    print(f"Row count: {cnt}")
    print("Sample rows:")
    print(lf.head(5).collect())

    # Print value counts of year and file_type
    print(lf.select(pl.col("activity_year"), pl.col("file_type")).unique().collect())

    # Print value counts of activity_year and purchaser
    print(lf.select(pl.col("activity_year"), pl.col("purchaser_type")).unique().collect())


if __name__ == "__main__":
    main()


