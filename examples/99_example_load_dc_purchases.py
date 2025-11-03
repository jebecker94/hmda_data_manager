# -*- coding: utf-8 -*-
"""
Example: Loading DC Purchase Data from Hive Database

This example demonstrates how to efficiently query the hive-partitioned HMDA 
database using Polars SQL interface.

For all of the file types, we want to load the data for the year and file type combination that is the most recently updated.
Examples of how to do this with the polars sql interface are provided below.
Conditional table expressions in SQL are used to filter the data to the most recently updated file type for each year.

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
df = df.sql('''
WITH alias AS (
    SELECT *, MIN(file_type) OVER (PARTITION BY activity_year) AS min_file_type
    FROM loans
    WHERE state_code = 'DC'
    AND action_taken = 6
)
SELECT * FROM alias
WHERE min_file_type = file_type
''',
table_name = 'loans')

# Summarize by county, state, and year
df_county = df.sql('''
SELECT DISTINCT activity_year, state_code, county_code, COUNT(*) AS count
FROM loans
WHERE HMDAIndex LIKE '%a_%'
GROUP BY activity_year, state_code, county_code
ORDER BY activity_year, state_code, county_code
''',
table_name = 'loans')

# Show the results
df = df.collect()
print(df)

# Print year and file type combinations
print(df.select(pl.col("activity_year"), pl.col("file_type")).unique().sort(by=["activity_year", "file_type"]))


df_ts = pl.scan_parquet(
    SILVER_DIR / "transmissal_series" / "post2018",
    cast_options=pl.ScanCastOptions(integer_cast='upcast'),
)
df_ts = df_ts.sql('''
WITH alias AS (
    SELECT *, MIN(file_type) OVER (PARTITION BY activity_year) AS min_file_type
    FROM ts
)
SELECT * FROM alias
WHERE min_file_type = file_type
''',
table_name = 'ts')
df_ts = df_ts.collect()
print(df_ts)

df_panel = pl.scan_parquet(
    SILVER_DIR / "panel" / "post2018",
    cast_options=pl.ScanCastOptions(integer_cast='upcast'),
    extra_columns='ignore'
)

df_panel = df_panel.sql('''
WITH alias AS (
    SELECT *, MIN(file_type) OVER (PARTITION BY activity_year) AS min_file_type
    FROM panel
)
SELECT * FROM alias
WHERE min_file_type = file_type
''',
table_name = 'panel')
df_panel = df_panel.collect()
print(df_panel)
