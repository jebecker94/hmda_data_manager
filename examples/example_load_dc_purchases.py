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
from hmda_data_manager.core import DATA_DIR

# Read from hive-partitioned database (created by example_import_workflow_post2018.py)
# This demonstrates efficient querying of the partitioned dataset
df = pl.scan_parquet(DATA_DIR / "database/loans/post2018")

# Select DC Purchases with SQL
df = df.sql('''
SELECT *
FROM loans
WHERE state_code = 'DC' AND action_taken = 6
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
