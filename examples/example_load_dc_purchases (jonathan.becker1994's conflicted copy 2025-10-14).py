# -*- coding: utf-8 -*-
"""
Created on Tue Oct 07 07:00:00 2025
@author: Jonathan E. Becker
"""

# Import Packages
import sys
from pathlib import Path
import polars as pl
sys.path.append(str(Path(__file__).resolve().parents[1]))
import config

# Set Data Directory
DATA_DIR = config.DATA_DIR

# Read from database
df = pl.scan_parquet(DATA_DIR / "database/loans")

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