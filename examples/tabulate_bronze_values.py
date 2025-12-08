#!/usr/bin/env python
"""
Tabulate unique values across bronze layer data for any time period.

This script lazily loads bronze data and shows unique values for each column
to help identify which columns can be safely cast to specific types in the
silver layer.

The bronze layer stores all columns as strings, so this script helps verify:
- Which columns contain only numeric values (candidates for Int64/Float64)
- Which columns contain categorical codes
- Which columns have mixed or non-numeric values
- Presence of NULL values

Usage:
    python examples/tabulate_bronze_values.py

Then select the period and dataset interactively.
"""

import polars as pl
from pathlib import Path
from hmda_data_manager.core.config import get_medallion_dir

# Configuration
AVAILABLE_PERIODS = {
    "pre2007": {"years": range(1990, 2007), "datasets": ["loans"]},
    "period_2007_2017": {"years": range(2007, 2018), "datasets": ["loans"]},
    "post2018": {"years": range(2018, 2025), "datasets": ["loans", "panel", "transmissal_series"]},
}

SKIP_COLUMNS = [
    'HMDAIndex',
    'loan_to_value_ratio',
    'combined_loan_to_value_ratio',
    'rate_spread',
]


def get_user_selection():
    """Prompt user to select period and dataset."""
    print("="*80)
    print("BRONZE VALUE TABULATION TOOL")
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


def load_bronze_data(period: str, dataset: str) -> pl.LazyFrame | None:
    """Load all bronze files for the given period and dataset."""
    bronze_folder = get_medallion_dir("bronze", dataset, period)

    # Get all parquet files
    parquet_files = sorted(bronze_folder.glob("*.parquet"))

    if not parquet_files:
        return None

    print(f"\nFound {len(parquet_files)} bronze files in: {bronze_folder}")

    # Load all files as a single LazyFrame
    lf_list = [pl.scan_parquet(str(f)) for f in parquet_files]
    lf = pl.concat(lf_list, how="diagonal_relaxed")

    return lf


def tabulate_values(lf: pl.LazyFrame, period: str, dataset: str):
    """Tabulate unique values for each column."""
    # Get schema
    schema = lf.collect_schema()
    columns = schema.names()

    print(f"\nTotal columns: {len(columns)}")

    # For each column, collect unique values (limited to first 50 for display)
    print("\n" + "="*80)
    print("UNIQUE VALUES BY COLUMN")
    print("="*80)

    for col in columns:
        if col in SKIP_COLUMNS:
            continue
        print(f"\n{col}:")
        print("-" * 80)

        # Get unique values and counts
        unique_df = (
            lf.select(pl.col(col))
            .group_by(col)
            .agg(pl.len().alias("count"))
            .sort("count", descending=True)
            .head(50)  # Limit to top 50 values
            .collect()
        )

        n_unique = len(unique_df)
        total_count = unique_df["count"].sum()

        # Display values
        print(f"Unique values: {n_unique} (showing up to 50 most common)")
        print(f"Total rows for these values: {total_count:,}\n")

        # Format output
        for row in unique_df.iter_rows(named=True):
            value = row[col]
            count = row["count"]

            # Display value (handle None/null)
            if value is None:
                display_val = "NULL"
            else:
                display_val = str(value)

            print(f"  {display_val:30s} {count:>15,}")

        # Check if all non-null values are numeric strings (potential integer column)
        non_null_values = unique_df.filter(pl.col(col).is_not_null())[col].to_list()

        if non_null_values:
            # Check if all values are numeric (allowing leading zeros, negative signs, decimals)
            all_integer_like = all(
                v.lstrip('-').isdigit() if isinstance(v, str) else False
                for v in non_null_values
            )

            all_float_like = all(
                v.lstrip('-').replace('.', '', 1).replace('e', '', 1).replace('E', '', 1).replace('+', '', 1).isdigit()
                if isinstance(v, str) else False
                for v in non_null_values
            )

            if all_integer_like:
                print(f"\n  ✅ CANDIDATE FOR INTEGER CONVERSION (all non-null values are numeric)")
            elif all_float_like:
                print(f"\n  ✅ CANDIDATE FOR FLOAT CONVERSION (all non-null values are numeric/decimal)")
            else:
                # Show non-numeric examples
                non_numeric = [
                    v for v in non_null_values
                    if not (isinstance(v, str) and v.lstrip('-').replace('.', '', 1).isdigit())
                ][:5]
                if non_numeric:
                    print(f"\n  ❌ CONTAINS NON-NUMERIC VALUES: {non_numeric}")

    print("\n" + "="*80)
    print("TABULATION COMPLETE")
    print("="*80)
    print("\nNext steps:")
    print("  - Columns marked ✅ can be safely converted to Int64/Float64 in silver layer")
    print("  - Columns marked ❌ need special handling or should remain as String")
    print("  - Check config.py for existing column type lists")


def main():
    """Main entry point."""
    # Get user selections
    period, dataset = get_user_selection()

    # Load bronze data
    lf = load_bronze_data(period, dataset)

    if lf is None:
        print(f"\n❌ Error: No bronze files found for {period} / {dataset}")
        print(f"\nPlease run the import workflow first:")
        if period == "pre2007":
            print(f"  python examples/04_example_import_workflow_pre2007.py")
        elif period == "period_2007_2017":
            print(f"  python examples/03_example_import_workflow_2007_2017.py")
        else:
            print(f"  python examples/02_example_import_workflow_post2018.py")
        return

    print("\n" + "="*80)
    print(f"TABULATING VALUES FOR {period.upper()} / {dataset.upper()}")
    print("="*80)

    # Tabulate values
    tabulate_values(lf, period, dataset)


if __name__ == "__main__":
    main()
