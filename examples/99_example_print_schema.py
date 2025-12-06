"""
99_example_print_schema.py
Displays the schema of the post-2018 HMDA loan data in the silver folder.
"""

# Import required libraries
import polars as pl
from hmda_data_manager.core import (
    SILVER_DIR,
)

# Read from hive-partitioned database (created by example_import_workflow_post2018.py)
# This demonstrates efficient querying of the partitioned dataset
df = pl.scan_parquet(SILVER_DIR / "loans" / "post2018" / "**/*.parquet")

# Print schema
print("Silver folder schema:")
print(dict(df.collect_schema()))

# Loop and display
for column, dtype in df.collect_schema().items():
    print(f"{column}: {dtype}")

