"""
99_example_print_schema.py
Displays the schema of the post-2018 HMDA loan data in the silver folder.
"""

# Import required libraries
import polars as pl
from hmda_data_manager.core import (
    SILVER_DIR,
)

# Print schemas period-by-period
print("Silver folder schemas:")
print('Post-2018 schema:')
df = pl.scan_parquet(SILVER_DIR / "loans" / "post2018" / "**/*.parquet")
for column, dtype in df.collect_schema().items():
    print(f"{column}: {dtype}")

print('Period 2007-2017 schema:')
df = pl.scan_parquet(SILVER_DIR / "loans" / "period_2007_2017" / "**/*.parquet")
for column, dtype in df.collect_schema().items():
    print(f"{column}: {dtype}")

print('Pre-2007 schema:')
df = pl.scan_parquet(SILVER_DIR / "loans" / "pre2007" / "**/*.parquet")
for column, dtype in df.collect_schema().items():
    print(f"{column}: {dtype}")
