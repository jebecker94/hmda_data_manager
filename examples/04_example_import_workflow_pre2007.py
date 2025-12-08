#!/usr/bin/env python
"""
Example: Pre-2007 HMDA Data Import Workflow

Complete workflow for importing pre-2007 HMDA data (1990-2006).

This example demonstrates the full medallion architecture for legacy HMDA data:
- Bronze layer: Raw data extracted from ZIPs, all columns as strings
- Silver layer: Type-converted, cleaned, analysis-ready data

Prerequisites:
1. Download raw data from openICPSR Project 151921
2. Place ZIP files in data/raw/

Note: 1981-1989 data is excluded by default (1981 contains aggregate census
tract data rather than individual loans, and 1982-1989 have a different schema)

Data Source:
Forrester, Andrew. Converted Home Mortgage Disclosure Act Data, 1981-2006.
Inter-university Consortium for Political and Social Research [distributor],
2021-08-30. https://doi.org/10.3886/E151921V1

Schema Evolution:
- 1990-2003: 23 columns (basic format)
- 2004-2006: 38 columns (expanded with multiple race fields, ethnicity, rate spread)
"""

from hmda_data_manager.core.import_data.pre2007 import (
    build_bronze_pre2007,
    build_silver_pre2007,
)

print("="*80)
print("PRE-2007 HMDA DATA IMPORT WORKFLOW")
print("="*80)

# ==============================================================================
# BUILD BRONZE LAYER (RAW → BRONZE)
# ==============================================================================

print("\n" + "="*80)
print("STEP 1: Building Bronze Layer (1990-2006)")
print("="*80)
print("\nExtracting from ZIP archives and saving as parquet...")
print("All columns are kept as strings for maximum data preservation.\n")

# Build bronze for loans (LAR)
print("Building bronze for LOANS (LAR)...")
build_bronze_pre2007("loans", min_year=1990, max_year=2006)

# Build bronze for panel (lender information)
print("\nBuilding bronze for PANEL (lender information)...")
build_bronze_pre2007("panel", min_year=1990, max_year=2006)

# Build bronze for transmittal series (submission metadata)
print("\nBuilding bronze for TRANSMITTAL SERIES (submission metadata)...")
build_bronze_pre2007("transmissal_series", min_year=1990, max_year=2006)

print("\n" + "="*80)
print("BRONZE LAYER COMPLETE")
print("="*80)
print("\nBronze files saved to:")
print("  - data/bronze/loans/pre2007/")
print("  - data/bronze/panel/pre2007/")
print("  - data/bronze/transmissal_series/pre2007/")
print("\nEach year is saved as a separate parquet file (e.g., loans_2006.parquet)")

# ==============================================================================
# BUILD SILVER LAYER (BRONZE → SILVER)
# ==============================================================================

print("\n" + "="*80)
print("STEP 2: Building Silver Layer (1990-2006)")
print("="*80)
print("\nApplying schema harmonization and geographic standardization...")
print("Transformations:")
print("  - Type conversions: 31 integer columns, 1 float column")
print("  - Geographic codes: state (2-digit), county (5-digit), tract (11-digit)")
print("  - Dollar amounts: loan_amount and income multiplied by 1000")
print("  - Column renaming: occupancy -> occupancy_type, msamd -> msa_md\n")

# Build silver for loans (LAR)
print("Building silver for LOANS (LAR)...")
build_silver_pre2007("loans", min_year=1990, max_year=2006)

# Build silver for panel (lender information)
print("\nBuilding silver for PANEL (lender information)...")
build_silver_pre2007("panel", min_year=1990, max_year=2006)

# Build silver for transmittal series (submission metadata)
print("\nBuilding silver for TRANSMITTAL SERIES (submission metadata)...")
build_silver_pre2007("transmissal_series", min_year=1990, max_year=2006)

print("\n" + "="*80)
print("SILVER LAYER COMPLETE")
print("="*80)
print("\nSilver files saved to:")
print("  - data/silver/loans/pre2007/activity_year=YYYY/")
print("  - data/silver/panel/pre2007/activity_year=YYYY/")
print("  - data/silver/transmissal_series/pre2007/activity_year=YYYY/")
print("\nData is Hive-partitioned by activity_year for efficient querying")

print("\n" + "="*80)
print("QUERY EXAMPLES")
print("="*80)
print("\n# Load all years lazily:")
print("import polars as pl")
print("lf = pl.scan_parquet('data/silver/loans/pre2007/**/*.parquet')")
print()
print("# Load specific year:")
print("lf = pl.scan_parquet('data/silver/loans/pre2007/activity_year=2006/*.parquet')")
print()
print("# Collect to DataFrame:")
print("df = lf.collect()")

print("\n" + "="*80)
print("WORKFLOW COMPLETE")
print("="*80)
