# -*- coding: utf-8 -*-
"""
Example: Sampling Silver HMDA Data

This example demonstrates how to efficiently load and sample from the hive-partitioned
HMDA silver database using Polars lazy evaluation.

Key Features:
- Lazily loads the full silver dataset (no memory pressure)
- Takes a configurable percentage sample (default 2%)
- Uses Polars' efficient sampling with optional seed for reproducibility
- Demonstrates sampling from post-2018 loans data

Prerequisites:
1. Run 02_example_import_workflow_post2018.py first to create the silver database
2. Ensure you have post-2018 HMDA data available

Usage:
    python examples/99_example_sample_silver_data.py

"""

# Import Packages
import polars as pl
from hmda_data_manager.core import SILVER_DIR

# Configuration
SAMPLE_FRACTION = 0.001 # 1% sample by default

# Lazily load the full post-2018 loans database
# This scans all partitions but doesn't load into memory yet
loans_lf = pl.scan_parquet(SILVER_DIR / "loans" / "post2018" / "**/*.parquet")

print(f"Sampling {SAMPLE_FRACTION * 100:.1f}% of the silver loans database...")
print("Original length: ", loans_lf.select(pl.len()).collect().item())

# Take a sample using row_index and modulo filtering (lazy operation)
# This approach works on LazyFrames without collecting the full dataset
# For a 2% sample, keep every 50th row (1/0.02 = 50)
sample_step = int(1 / SAMPLE_FRACTION)

sampled_df = (
    loans_lf
    .with_row_index("row_num")
    .filter(pl.col("row_num") % sample_step == 0)
    .drop("row_num")
    .collect()
)

# Display sample information
print(f"\nSample collected successfully!")
print(f"Sample size: {len(sampled_df):,} rows")
print(f"Number of columns: {len(sampled_df.columns)}")
print(f"\nFirst few rows:")
print(sampled_df.head())

# Show distribution of activity_year and file_type in sample
print("\nSample distribution by year and file type:")
distribution = (
    sampled_df.group_by(["activity_year", "file_type"])
    .agg(pl.len().alias("count"))
    .sort(["activity_year", "file_type"])
)
print(distribution)

# Example: Filter sample to specific conditions
# Only collect what you need after sampling
print("\nExample: DC purchases from the sample")
dc_purchases = sampled_df.filter(
    (pl.col("state_code") == "DC") & (pl.col("action_taken") == 6)
)
print(f"DC purchases in sample: {len(dc_purchases):,} rows")
