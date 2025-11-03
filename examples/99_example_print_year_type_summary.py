# -*- coding: utf-8 -*-
"""
Example: Loading DC Purchase Data from Hive Database

This example demonstrates how to efficiently query the hive-partitioned HMDA 
database using Polars SQL interface.

Prerequisites:
1. Run example_import_workflow_post2018.py first to create the database
2. Ensure you have post-2018 HMDA data available

"""

# Import Packages
import polars as pl
from hmda_data_manager.core import (
    SILVER_DIR,
)

# Read from hive-partitioned database (created by example_import_workflow_post2018.py)
# This demonstrates efficient querying of the partitioned dataset
df = pl.scan_parquet(SILVER_DIR / "loans" / "post2018")

# Print year and file type combinations
print(df.select(pl.col("activity_year"), pl.col("file_type")).unique().sort(by=["activity_year", "file_type"]).collect())
