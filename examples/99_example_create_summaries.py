"""
HMDA Lender Summary Creation Example
===================================

This example demonstrates how to create combined lender datasets by merging
Panel and Transmittal Sheet data for both pre-2018 and post-2018 HMDA data
using the hmda_data_manager package.

The summary creation includes:
1. Combining post-2018 Panel and Transmittal Sheet data 
2. Combining pre-2018 Panel and Transmittal Sheet data
3. Creating both CSV and Parquet outputs
4. Data validation and quality checks
5. Summary statistics and reporting

These combined datasets are useful for:
- Lender analysis across years
- Institution-level research
- Regulatory reporting
- Market concentration studies

Author: Jonathan E. Becker
Created: October 2025
"""

import logging
import polars as pl
from pathlib import Path

# Import the summary functions and configuration
from hmda_data_manager.core import DATA_DIR, CLEAN_DIR
from hmda_data_manager.utils import (
    combine_lenders_panel_ts_post2018,
    combine_lenders_panel_ts_pre2018
)

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

def main():
    """Main workflow for creating HMDA lender summary datasets."""
    
    logger.info("Starting HMDA Lender Summary Creation")
    logger.info("=" * 50)
    
    # =============================================================================
    # 1. Configuration and Setup
    # =============================================================================
    
    logger.info("1. Setting up configuration...")
    
    # Define folders
    clean_folder = CLEAN_DIR
    data_folder = DATA_DIR
    panel_folder = clean_folder / "panel"
    ts_folder = clean_folder / "transmissal_series"
    
    logger.info(f"  Panel data folder: {panel_folder}")
    logger.info(f"  Transmittal Sheet folder: {ts_folder}")
    logger.info(f"  Output folder: {data_folder}")
    
    # Check that required folders exist
    missing_folders = []
    for folder in [panel_folder, ts_folder]:
        if not folder.exists():
            missing_folders.append(str(folder))
    
    if missing_folders:
        logger.error("❌ Required folders not found:")
        for folder in missing_folders:
            logger.error(f"  - {folder}")
        logger.error("Run example_import_workflow_post2018.py first to create the necessary data files.")
        return False
    
    # Ensure output folder exists
    data_folder.mkdir(parents=True, exist_ok=True)
    
    # =============================================================================
    # 2. Create Post-2018 Combined Lender Dataset
    # =============================================================================
    
    logger.info("2. Creating post-2018 combined lender dataset...")
    
    try:
        # Define year range for post-2018 data
        post2018_min_year = 2018
        post2018_max_year = 2024
        
        # Check what files are available
        panel_files = list(panel_folder.glob("*_public_panel*.parquet"))
        ts_files = list(ts_folder.glob("*_public_ts*.parquet"))
        
        logger.info(f"  Found {len(panel_files)} panel files")
        logger.info(f"  Found {len(ts_files)} transmittal sheet files")
        
        if not panel_files or not ts_files:
            logger.warning("  ⚠️  No post-2018 files found, skipping post-2018 combination")
        else:
            # Use the utility function to combine post-2018 data
            logger.info(f"  Combining data for years {post2018_min_year}-{post2018_max_year}...")
            
            combine_lenders_panel_ts_post2018(
                panel_folder=panel_folder,
                ts_folder=ts_folder,
                save_folder=data_folder,
                min_year=post2018_min_year,
                max_year=post2018_max_year
            )
            
            logger.info("  ✅ Post-2018 combined dataset created successfully")
            
            # Check output files
            expected_stem = f"hmda_lenders_combined_{post2018_min_year}-{post2018_max_year}"
            csv_file = data_folder / f"{expected_stem}.csv"
            parquet_file = data_folder / f"{expected_stem}.parquet"
            
            if csv_file.exists():
                file_size_mb = csv_file.stat().st_size / (1024 * 1024)
                logger.info(f"  📁 CSV file: {csv_file.name} ({file_size_mb:.1f} MB)")
            
            if parquet_file.exists():
                file_size_mb = parquet_file.stat().st_size / (1024 * 1024)
                logger.info(f"  📁 Parquet file: {parquet_file.name} ({file_size_mb:.1f} MB)")
        
    except Exception as e:
        logger.error(f"  ❌ Error creating post-2018 combined dataset: {e}")
        return False
    
    # =============================================================================
    # 3. Create Pre-2018 Combined Lender Dataset (2007-2017)
    # =============================================================================
    
    logger.info("3. Creating pre-2018 combined lender dataset...")
    
    try:
        # Define year range for pre-2018 data
        pre2018_min_year = 2007
        pre2018_max_year = 2017
        
        # Check for CSV files (pre-2018 data is typically in CSV format)
        panel_csv_files = list(panel_folder.glob("*panel*.csv"))
        ts_csv_files = list(ts_folder.glob("*ts*.csv"))
        
        logger.info(f"  Found {len(panel_csv_files)} pre-2018 panel CSV files")
        logger.info(f"  Found {len(ts_csv_files)} pre-2018 transmittal sheet CSV files")
        
        if not panel_csv_files or not ts_csv_files:
            logger.warning("  ⚠️  No pre-2018 CSV files found, skipping pre-2018 combination")
            logger.info("  Note: Pre-2018 data requires CSV files, not Parquet files")
        else:
            # Use the utility function to combine pre-2018 data
            logger.info(f"  Combining data for years {pre2018_min_year}-{pre2018_max_year}...")
            
            combine_lenders_panel_ts_pre2018(
                panel_folder=panel_folder,
                ts_folder=ts_folder,
                save_folder=data_folder,
                min_year=pre2018_min_year,
                max_year=pre2018_max_year
            )
            
            logger.info("  ✅ Pre-2018 combined dataset created successfully")
            
            # Check output file
            expected_stem = f"hmda_lenders_combined_{pre2018_min_year}-{pre2018_max_year}"
            csv_file = data_folder / f"{expected_stem}.csv"
            
            if csv_file.exists():
                file_size_mb = csv_file.stat().st_size / (1024 * 1024)
                logger.info(f"  📁 CSV file: {csv_file.name} ({file_size_mb:.1f} MB)")
        
    except Exception as e:
        logger.error(f"  ❌ Error creating pre-2018 combined dataset: {e}")
        # This is not critical, so continue
    
    # =============================================================================
    # 4. Data Validation and Summary Statistics
    # =============================================================================
    
    logger.info("4. Validating created datasets and generating summary statistics...")
    
    try:
        # Find all created combined files
        combined_files = list(data_folder.glob("hmda_lenders_combined_*.parquet")) + \
                        list(data_folder.glob("hmda_lenders_combined_*.csv"))
        
        logger.info(f"  📊 Found {len(combined_files)} combined lender files:")
        
        for file in sorted(combined_files):
            logger.info(f"  Analyzing: {file.name}")
            
            try:
                # Load file appropriately based on format
                if file.suffix == '.parquet':
                    df = pl.scan_parquet(file)
                elif file.suffix == '.csv':
                    # Use pipe separator as that's what the functions create
                    df = pl.scan_csv(file, separator="|")
                else:
                    continue
                
                # Get basic statistics
                row_count = df.select(pl.len()).collect().item()
                columns = df.columns
                
                logger.info(f"    - Rows: {row_count:,}")
                logger.info(f"    - Columns: {len(columns)}")
                
                # Check for key columns based on data period
                if "2018" in file.name or "2019" in file.name or "202" in file.name:  # Post-2018
                    key_columns = ["activity_year", "lei"]
                    expected_years = list(range(2018, 2025))
                else:  # Pre-2018
                    key_columns = ["Activity Year", "Respondent ID"]
                    expected_years = list(range(2007, 2018))
                
                missing_key_cols = [col for col in key_columns if col not in columns]
                if missing_key_cols:
                    logger.warning(f"    ⚠️  Missing key columns: {missing_key_cols}")
                else:
                    logger.info(f"    ✅ Key columns present: {key_columns}")
                
                # Check year coverage if we have year column
                year_col = key_columns[0]  # First key column is usually the year
                if year_col in columns:
                    years_present = (
                        df.select(pl.col(year_col).unique().sort())
                        .collect()[year_col]
                        .to_list()
                    )
                    logger.info(f"    📅 Years covered: {min(years_present)} to {max(years_present)}")
                    
                    missing_years = [year for year in expected_years if year not in years_present]
                    if missing_years:
                        logger.warning(f"    ⚠️  Missing years: {missing_years}")
                
                # Check for duplicate lenders
                if len(key_columns) == 2:
                    duplicates = (
                        df.group_by(key_columns)
                        .agg(pl.len().alias("count"))
                        .filter(pl.col("count") > 1)
                        .select(pl.len())
                        .collect()
                        .item()
                    )
                    
                    if duplicates > 0:
                        logger.warning(f"    ⚠️  Found {duplicates} duplicate lender-year combinations")
                    else:
                        logger.info("    ✅ No duplicate lender-year combinations")
                
            except Exception as e:
                logger.warning(f"    Could not analyze {file.name}: {e}")
        
        logger.info("  ✅ Data validation completed")
        
    except Exception as e:
        logger.error(f"  ❌ Error during validation: {e}")
    
    # =============================================================================
    # 5. Summary and Next Steps
    # =============================================================================
    
    logger.info("5. Summary creation completed! 🎉")
    logger.info("=" * 50)
    
    # List all created files
    combined_files = list(data_folder.glob("hmda_lenders_combined_*"))
    
    if combined_files:
        logger.info("📁 Created combined lender files:")
        for file in sorted(combined_files):
            file_size_mb = file.stat().st_size / (1024 * 1024)
            logger.info(f"  - {file.name} ({file_size_mb:.1f} MB)")
    else:
        logger.warning("📁 No combined files were created")
        logger.info("Check that you have the required panel and transmittal sheet data files")
    
    logger.info("\n🚀 Next steps:")
    logger.info("  1. Use combined files for lender-level analysis:")
    logger.info("     import polars as pl")
    logger.info("     df = pl.read_parquet('data/hmda_lenders_combined_2018-2024.parquet')")
    logger.info("  2. Analyze lender characteristics over time:")
    logger.info("     df.group_by('activity_year').agg(pl.len().alias('lender_count'))")
    logger.info("  3. Study market concentration and competition")
    logger.info("  4. Create regulatory reports with comprehensive lender information")
    logger.info("  5. Join with LAR data for institution-level loan analysis")
    
    return True

def validate_requirements():
    """Check if required data files are available."""
    
    panel_folder = CLEAN_DIR / "panel"
    ts_folder = CLEAN_DIR / "transmissal_series"
    
    requirements_met = True
    
    # Check for post-2018 files
    post2018_panel = list(panel_folder.glob("*_public_panel*.parquet"))
    post2018_ts = list(ts_folder.glob("*_public_ts*.parquet"))
    
    if post2018_panel and post2018_ts:
        print("[OK] Post-2018 data files available")
    else:
        print("[ERROR] Post-2018 data files missing")
        print("   Run: python examples/example_import_workflow_post2018.py")
        requirements_met = False
    
    # Check for pre-2018 files
    pre2018_panel = list(panel_folder.glob("*panel*.csv"))
    pre2018_ts = list(ts_folder.glob("*ts*.csv"))
    
    if pre2018_panel and pre2018_ts:
        print("[OK] Pre-2018 data files available")
    elif not pre2018_panel and not pre2018_ts:
        print("[INFO] Pre-2018 data files not found (optional)")
    else:
        print("[WARNING] Partial pre-2018 data files found")
    
    return requirements_met

if __name__ == "__main__":
    """
    Run the summary creation workflow.
    
    Before running:
    1. Ensure you have imported HMDA Panel and Transmittal Sheet data
    2. For post-2018: Run example_import_workflow_post2018.py first
    3. For pre-2018: Import and process 2007-2017 data files
    
    Usage:
        # Check requirements
        python -c "from examples.example_create_summaries import validate_requirements; validate_requirements()"
        
        # Run the example
        python examples/example_create_summaries.py
    """
    
    print("HMDA Lender Summary Creation")
    print("=" * 30)
    
    # Check requirements first
    print("\nChecking requirements...")
    if not validate_requirements():
        print("\n[ERROR] Requirements not met. Please import the required data first.")
        exit(1)
    
    print("\n" + "=" * 30)
    
    # Run the main workflow
    success = main()
    
    if success:
        print("\n[SUCCESS] Summary creation completed successfully!")
        print("Check the logs above for detailed information about the created files.")
        print("Combined lender files are saved in the data/ directory.")
    else:
        print("\n[ERROR] Summary creation failed!")
        print("Check the error messages above for troubleshooting information.")
