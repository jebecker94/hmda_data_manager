#!/usr/bin/env python
"""
Inspect bronze layer data for any time period.

The bronze layer stores all columns as strings to preserve raw values and
enable inspection/validation before silver layer type conversions.

This script loads a single bronze file and displays:
- Schema information (all columns should be String type)
- Geographic code samples
- Value distributions
- Data quality checks

Usage:
    python examples/inspect_bronze.py

Then select the period, dataset (if applicable), and specific file interactively.
"""

import sys
import polars as pl
from pathlib import Path
from hmda_data_manager.core.config import get_medallion_dir

# Configuration
AVAILABLE_PERIODS = {
    "pre2007": {"years": range(1990, 2007), "datasets": ["loans"]},
    "period_2007_2017": {"years": range(2007, 2018), "datasets": ["loans"]},
    "post2018": {"years": range(2018, 2025), "datasets": ["loans", "panel", "transmissal_series"]},
}


def get_user_selection():
    """Prompt user to select period and dataset."""
    print("="*80)
    print("BRONZE LAYER INSPECTION TOOL")
    print("="*80)
    print("\nAvailable time periods:")
    print("  1. pre2007 (1990-2006)")
    print("  2. period_2007_2017 (2007-2017)")
    print("  3. post2018 (2018-2024)")

    while True:
        choice = input("\nSelect period (1-3): ").strip()
        if choice == "1":
            period = "pre2007"
            break
        elif choice == "2":
            period = "period_2007_2017"
            break
        elif choice == "3":
            period = "post2018"
            break
        else:
            print("Invalid choice. Please enter 1, 2, or 3.")

    # Get available datasets for this period
    available_datasets = AVAILABLE_PERIODS[period]["datasets"]

    if len(available_datasets) == 1:
        dataset = available_datasets[0]
        print(f"\nDataset: {dataset}")
    else:
        print(f"\nAvailable datasets for {period}:")
        for i, ds in enumerate(available_datasets, 1):
            print(f"  {i}. {ds}")

        while True:
            choice = input(f"\nSelect dataset (1-{len(available_datasets)}): ").strip()
            try:
                idx = int(choice) - 1
                if 0 <= idx < len(available_datasets):
                    dataset = available_datasets[idx]
                    break
                else:
                    print(f"Invalid choice. Please enter 1-{len(available_datasets)}.")
            except ValueError:
                print(f"Invalid choice. Please enter 1-{len(available_datasets)}.")

    return period, dataset


def select_bronze_file(period: str, dataset: str) -> Path | None:
    """List all bronze files and let user select one."""
    bronze_folder = get_medallion_dir("bronze", dataset, period)

    # Get all parquet files
    candidates = sorted(bronze_folder.glob("*.parquet"))

    if not candidates:
        return None
    elif len(candidates) == 1:
        print(f"\nFound bronze file: {candidates[0].name}")
        return candidates[0]
    else:
        # Multiple files found - let user choose
        print(f"\nAvailable bronze files ({len(candidates)} files):")
        for i, f in enumerate(candidates, 1):
            size_mb = f.stat().st_size / 1024 / 1024
            print(f"  {i:2d}. {f.name:50s} ({size_mb:>8.2f} MB)")

        while True:
            choice = input(f"\nSelect file (1-{len(candidates)}): ").strip()
            try:
                idx = int(choice) - 1
                if 0 <= idx < len(candidates):
                    return candidates[idx]
                else:
                    print(f"Invalid choice. Please enter 1-{len(candidates)}.")
            except ValueError:
                print(f"Invalid choice. Please enter 1-{len(candidates)}.")


def inspect_bronze_data(bronze_file: Path, period: str, dataset: str):
    """Inspect bronze layer data."""
    print("\n" + "="*80)
    print(f"LOADING BRONZE FILE")
    print("="*80)
    print(f"File: {bronze_file}")
    print(f"Size: {bronze_file.stat().st_size / 1024 / 1024:.2f} MB")

    # Load data
    df = pl.read_parquet(bronze_file)

    print("\n" + "="*80)
    print("DATAFRAME INFO")
    print("="*80)
    print(f"Shape: {df.shape}")
    print(f"Rows: {len(df):,}")
    print(f"Columns: {len(df.columns)}")

    print("\n" + "="*80)
    print("COLUMN NAMES AND TYPES")
    print("="*80)
    print("\n⚠️  NOTE: Bronze layer should have all String columns (except metadata)")
    if period == "post2018":
        print("         Post-2018 bronze adds: file_type (String), HMDAIndex (String)\n")
    else:
        print()

    # Count non-string columns
    non_string_cols = [col for col, dtype in df.schema.items() if dtype != pl.String]
    if non_string_cols:
        print(f"⚠️  WARNING: Found {len(non_string_cols)} non-string columns: {non_string_cols}\n")
    else:
        print("✅ All columns are String type (as expected for bronze layer)\n")

    for i, (col, dtype) in enumerate(df.schema.items(), 1):
        marker = "  " if dtype == pl.String else "⚠️ "
        print(f"{marker}{i:2d}. {col:40s} {dtype}")

    # Period-specific key columns
    if period == "pre2007":
        key_cols = ['activity_year', 'respondent_id', 'agency_code', 'loan_amount',
                   'action_taken', 'state_code', 'county_code', 'msa_md']
    elif period == "period_2007_2017":
        key_cols = ['activity_year', 'respondent_id', 'agency_code', 'loan_amount',
                   'action_taken', 'state_code', 'county_code', 'msa_md']
    else:  # post2018
        key_cols = ['activity_year', 'lei', 'loan_amount', 'action_taken',
                   'state_code', 'county_code', 'census_tract', 'file_type']
        if dataset == "loans":
            key_cols.insert(0, 'HMDAIndex')

    print("\n" + "="*80)
    print("SAMPLE OF KEY COLUMNS - FIRST 10 ROWS")
    print("="*80)
    available_key_cols = [c for c in key_cols if c in df.columns]
    if available_key_cols:
        print(df.select(available_key_cols).head(10))
    else:
        print("Key columns not found - showing first 5 columns instead")
        print(df.select(df.columns[:5]).head(10))

    # Geographic codes
    geo_cols = ['state_code', 'county_code', 'census_tract']
    available_geo_cols = [c for c in geo_cols if c in df.columns]

    if available_geo_cols:
        print("\n" + "="*80)
        print("GEOGRAPHIC CODES - FIRST 10 ROWS")
        print("="*80)
        print(df.select(available_geo_cols).head(10))

        print("\n" + "="*80)
        print("GEOGRAPHIC CODE STRING LENGTHS (Bronze = Raw Values)")
        print("="*80)

        for geo_col in available_geo_cols:
            if geo_col in df.columns:
                non_null = df.filter(pl.col(geo_col).is_not_null())
                if len(non_null) > 0:
                    lengths = non_null[geo_col].str.len_chars().unique().sort().to_list()
                    print(f"\n{geo_col}:")
                    print(f"  Unique string lengths: {lengths}")
                    print(f"  Sample values: {non_null[geo_col].head(5).to_list()}")

                    # Note: Bronze has raw values; silver will standardize these
                    if geo_col == 'state_code':
                        print("  Note: Silver layer will standardize to 2-digit strings")
                    elif geo_col == 'county_code':
                        print("  Note: Silver layer will standardize to 5-digit strings")
                    elif geo_col == 'census_tract':
                        print("  Note: Silver layer will standardize to 11-digit strings")

    print("\n" + "="*80)
    print("NULL COUNTS BY COLUMN (Top 20)")
    print("="*80)
    null_counts = df.null_count()
    for col in df.columns[:20]:
        count = null_counts[col][0]
        pct = (count / len(df)) * 100 if len(df) > 0 else 0
        print(f"{col:40s} {count:10,} ({pct:5.2f}%)")

    print("\n" + "="*80)
    print("VALUE DISTRIBUTIONS FOR KEY COLUMNS (Top 10 values)")
    print("="*80)

    # Show value distributions for categorical columns
    if period == "post2018":
        inspect_cols = ['action_taken', 'loan_type', 'loan_purpose', 'occupancy_type']
        if 'file_type' in df.columns:
            inspect_cols.insert(0, 'file_type')
    else:
        inspect_cols = ['action_taken', 'loan_type', 'loan_purpose', 'occupancy_type', 'agency_code']

    for col in inspect_cols:
        if col in df.columns:
            print(f"\n{col}:")
            value_counts = (
                df.select(pl.col(col))
                .group_by(col)
                .agg(pl.len().alias("count"))
                .sort("count", descending=True)
                .head(10)
            )
            for row in value_counts.iter_rows(named=True):
                value = row[col]
                count = row["count"]
                display_val = "NULL" if value is None else str(value)
                pct = (count / len(df)) * 100 if len(df) > 0 else 0
                print(f"  {display_val:20s} {count:>12,} ({pct:5.2f}%)")

    print("\n" + "="*80)
    print("NUMERIC FIELD VALIDATION (Bronze = String Values)")
    print("="*80)

    if "loan_amount" in df.columns:
        print("\nloan_amount (stored as String in bronze):")
        non_null = df.filter(pl.col("loan_amount").is_not_null())
        if len(non_null) > 0:
            print(f"  Sample values: {non_null['loan_amount'].head(10).to_list()}")

            # Check if all non-null values are numeric strings
            sample = non_null['loan_amount'].head(1000).to_list()
            all_numeric = all(
                v.lstrip('-').replace('.', '', 1).isdigit() if isinstance(v, str) else False
                for v in sample
            )
            if all_numeric:
                print("  ✅ Sample values are all numeric strings (can convert to Int64 in silver)")
            else:
                non_numeric = [v for v in sample if not (isinstance(v, str) and v.lstrip('-').replace('.', '', 1).isdigit())][:5]
                print(f"  ⚠️  Found non-numeric values: {non_numeric}")

    print("\n" + "="*80)
    print("INSPECTION COMPLETE")
    print("="*80)
    print("\nNext steps:")
    print("  - Bronze layer preserves raw string values")
    print("  - Silver layer applies type conversions and standardization")
    print("  - Use tabulate_pre2007_values.py for deeper value inspection")


def main():
    """Main entry point."""
    # Get user selections
    period, dataset = get_user_selection()

    # Select bronze file
    bronze_file = select_bronze_file(period, dataset)

    if bronze_file is None:
        print(f"\n❌ Error: No bronze files found for {period} / {dataset}")
        print(f"\nPlease run the import workflow first:")
        if period == "pre2007":
            print(f"  python examples/04_example_import_workflow_pre2007.py")
        elif period == "period_2007_2017":
            print(f"  python examples/03_example_import_workflow_2007_2017.py")
        else:
            print(f"  python examples/02_example_import_workflow_post2018.py")
        sys.exit(1)

    # Inspect the data
    inspect_bronze_data(bronze_file, period, dataset)


if __name__ == "__main__":
    main()
