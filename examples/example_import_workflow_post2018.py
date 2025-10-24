"""
HMDA Data Import Workflow Example - Post-2018 Data
==================================================

This example demonstrates how to use the hmda_data_manager package to import
and process HMDA data for years 2018 and later using the modular import functions.

The workflow includes:
1. Setting up paths and configuration
2. Importing LAR (loan-level) data 
3. Importing Panel data (lender information)
4. Importing Transmittal Sheet data
5. Combining datasets for analysis
6. Basic data validation and summary statistics
7. Creating hive-partitioned database for efficient querying

"""

import logging
import polars as pl
from pathlib import Path

# Import the new modular functions
from hmda_data_manager.core import (
    import_hmda_post2018, 
    save_to_dataset,
    DATA_DIR, 
    RAW_DIR, 
    CLEAN_DIR
)
from hmda_data_manager.schemas import get_schema_path

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

def main():
    """Main workflow for importing post-2018 HMDA data."""
    
    logger.info("Starting HMDA Post-2018 Data Import Workflow")
    logger.info("=" * 60)
    
    # =============================================================================
    # 1. Configuration and Setup
    # =============================================================================
    
    logger.info("1. Setting up configuration...")
    
    # Define years to process (adjust as needed)
    min_year = 2019
    max_year = 2019
    
    # Define folders
    raw_folder = RAW_DIR
    clean_folder = CLEAN_DIR
    
    logger.info(f"  Raw data folder: {raw_folder}")
    logger.info(f"  Clean data folder: {clean_folder}")
    logger.info(f"  Processing years: {min_year} to {max_year}")
    
    # Ensure clean directories exist
    for subdir in ["loans", "panel", "transmissal_series"]:
        (clean_folder / subdir).mkdir(parents=True, exist_ok=True)
    
    # =============================================================================
    # 2. Get Schema Information
    # =============================================================================
    
    logger.info("2. Loading schema information...")
    
    try:
        # Get schema files for post-2018 data
        lar_schema_path = get_schema_path("hmda_lar_schema_post2018")
        panel_schema_path = get_schema_path("hmda_panel_schema_post2018") 
        ts_schema_path = get_schema_path("hmda_ts_schema_post2018")
        
        logger.info(f"  LAR schema: {lar_schema_path}")
        logger.info(f"  Panel schema: {panel_schema_path}")
        logger.info(f"  TS schema: {ts_schema_path}")
        
    except Exception as e:
        logger.warning(f"Could not load schema files: {e}")
        logger.info("Proceeding without explicit schema validation...")
        lar_schema_path = panel_schema_path = ts_schema_path = None
    
    # =============================================================================
    # 3. Import LAR (Loan-Level) Data
    # =============================================================================
    logger.info("3. Importing LAR (loan-level) data...")
    
    try:
        # Import post2018 LAR data with cleaning enabled
        import_hmda_post2018(
            data_folder=raw_folder / "loans",
            save_folder=clean_folder / "loans",
            schema_file=lar_schema_path,
            min_year=min_year,
            max_year=max_year,
            add_hmda_index=True,
            add_file_type=True,
            clean=True,  # Enable cleaning transformations
        )
        
        logger.info("  ✅ LAR data import completed successfully")
        
        # Check what files were created
        lar_files = list((clean_folder / "loans").glob("*_public_lar*.parquet"))
        logger.info(f"  Created {len(lar_files)} LAR files:")
        for file in sorted(lar_files):
            logger.info(f"    - {file.name}")
        
    except Exception as e:
        logger.error(f"  ❌ Error importing LAR data: {e}")
        return False
    
    # =============================================================================
    # 4. Import Panel Data (Lender Information) 
    # =============================================================================
    stop
    
    logger.info("4. Importing Panel (lender) data...")
    
    try:
        # Import post2018 Panel data
        import_hmda_post2018(
            data_folder=raw_folder / "panel",
            save_folder=clean_folder / "panel", 
            schema_file=panel_schema_path,
            min_year=min_year,
            max_year=max_year,
            add_hmda_index=False,  # Panel doesn't need HMDA index
            add_file_type=True,
            clean=False,  # Panel data doesn't need cleaning
        )
        
        logger.info("  ✅ Panel data import completed successfully")
        
        # Check what files were created
        panel_files = list((clean_folder / "panel").glob("*_public_panel*.parquet"))
        logger.info(f"  Created {len(panel_files)} Panel files:")
        for file in sorted(panel_files):
            logger.info(f"    - {file.name}")
            
    except Exception as e:
        logger.error(f"  ❌ Error importing Panel data: {e}")
        return False
    
    # =============================================================================
    # 5. Import Transmittal Sheet Data
    # =============================================================================
    
    logger.info("5. Importing Transmittal Sheet data...")
    
    try:
        # Import post2018 TS data
        import_hmda_post2018(
            data_folder=raw_folder / "transmissal_series",
            save_folder=clean_folder / "transmissal_series",
            schema_file=ts_schema_path,
            min_year=min_year,
            max_year=max_year,
            add_hmda_index=False,  # TS doesn't need HMDA index
            add_file_type=True,
            clean=False,  # TS data doesn't need cleaning
        )
        
        logger.info("  ✅ Transmittal Sheet data import completed successfully")
        
        # Check what files were created
        ts_files = list((clean_folder / "transmissal_series").glob("*_public_ts*.parquet"))
        logger.info(f"  Created {len(ts_files)} TS files:")
        for file in sorted(ts_files):
            logger.info(f"    - {file.name}")
            
    except Exception as e:
        logger.error(f"  ❌ Error importing TS data: {e}")
        return False
    
    # =============================================================================
    # 6. Data Validation and Summary Statistics
    # =============================================================================
    
    logger.info("6. Performing data validation and generating summary statistics...")
    
    try:
        # Load and examine LAR data
        lar_files = list((clean_folder / "loans").glob("*_public_lar*.parquet"))
        
        if lar_files:
            # Load the first file for validation
            sample_file = lar_files[0]
            logger.info(f"  Examining sample file: {sample_file.name}")
            
            # Use Polars to scan the file efficiently
            df = pl.scan_parquet(sample_file)
            
            # Get basic info
            row_count = df.select(pl.len()).collect().item()
            column_names = df.columns
            
            logger.info(f"  📊 Sample file statistics:")
            logger.info(f"    - Row count: {row_count:,}")
            logger.info(f"    - Column count: {len(column_names)}")
            
            # Check for key columns
            key_columns = ["HMDAIndex", "lei", "activity_year", "action_taken", "loan_amount"]
            missing_columns = [col for col in key_columns if col not in column_names]
            
            if missing_columns:
                logger.warning(f"    ⚠️  Missing expected columns: {missing_columns}")
            else:
                logger.info(f"    ✅ All key columns present: {key_columns}")
            
            # Show action types distribution
            try:
                action_dist = (
                    df.group_by("action_taken")
                    .agg(pl.len().alias("count"))
                    .sort("action_taken")
                    .collect()
                )
                logger.info("    📈 Action taken distribution:")
                for row in action_dist.iter_rows():
                    action_code, count = row
                    logger.info(f"      Action {action_code}: {count:,} records")
                    
            except Exception as e:
                logger.warning(f"    Could not generate action distribution: {e}")
        
        # Load and examine Panel data
        panel_files = list((clean_folder / "panel").glob("*_public_panel*.parquet"))
        
        if panel_files:
            sample_panel = panel_files[0]
            logger.info(f"  Examining panel file: {sample_panel.name}")
            
            df_panel = pl.scan_parquet(sample_panel)
            panel_rows = df_panel.select(pl.len()).collect().item()
            
            logger.info(f"  📊 Panel file statistics:")
            logger.info(f"    - Lender count: {panel_rows:,}")
            
        logger.info("  ✅ Data validation completed")
        
    except Exception as e:
        logger.error(f"  ❌ Error during validation: {e}")
    
    # =============================================================================
    # 7. Create Hive-Partitioned Database (Optional)
    # =============================================================================
    
    logger.info("7. Creating hive-partitioned database...")
    
    try:
        # Create a hive-partitioned dataset from the imported LAR data
        database_folder = DATA_DIR / "database" / "loans" / "post2018"
        
        logger.info(f"  Creating database in: {database_folder}")
        
        # Use the save_to_dataset function to create partitioned data
        save_to_dataset(
            data_folder=clean_folder / "loans",
            save_folder=database_folder,
            min_year=min_year,
            max_year=max_year
        )
        
        logger.info("  ✅ Hive-partitioned database created successfully")
        logger.info(f"  📁 Database location: {database_folder}")
        logger.info("  🔍 Partitioned by: activity_year and file_type")
        
        # Check the created structure
        if database_folder.exists():
            partitions = list(database_folder.glob("**/"))
            logger.info(f"  📊 Created {len(partitions)} partition directories")
            
            # Show some example partition paths
            for partition in sorted(partitions)[:5]:  # Show first 5
                if partition != database_folder:  # Skip root directory
                    rel_path = partition.relative_to(database_folder)
                    logger.info(f"    - {rel_path}")
            if len(partitions) > 5:
                logger.info(f"    ... and {len(partitions) - 5} more")
        
    except Exception as e:
        logger.error(f"  ❌ Error creating hive database: {e}")
        logger.warning("  Database creation failed, but imported files are still available")
    
    # =============================================================================
    # 8. Summary and Next Steps
    # =============================================================================
    
    logger.info("8. Import workflow completed! 🎉")
    logger.info("=" * 60)
    
    logger.info("📁 Files created in the clean data directory:")
    for subdir in ["loans", "panel", "transmissal_series"]:
        files = list((clean_folder / subdir).glob("*.parquet"))
        if files:
            logger.info(f"  {subdir}/:")
            for file in sorted(files):
                file_size_mb = file.stat().st_size / (1024 * 1024)
                logger.info(f"    - {file.name} ({file_size_mb:.1f} MB)")
        else:
            logger.info(f"  {subdir}/: No files created")
    
    logger.info("\n🚀 Next steps:")
    logger.info("  1. Query the hive database for efficient analysis:")
    logger.info("     import polars as pl")
    logger.info("     df = pl.scan_parquet('data/database/loans/post2018')")
    logger.info("  2. Use SQL queries on the partitioned data:")
    logger.info("     df.sql('SELECT * FROM self WHERE activity_year = 2023')")
    logger.info("  3. Query specific partitions for faster performance:")
    logger.info("     df = pl.scan_parquet('data/database/loans/post2018/activity_year=2023/*')")
    logger.info("  4. Check examples/example_load_dc_purchases.py for SQL filtering examples")
    logger.info("  5. See examples/example_hmda_outlier_detection.py for analysis ideas")
    logger.info("  6. Partitioned data enables faster queries on large datasets")
    
    return True

if __name__ == "__main__":
    """
    Run the import workflow.
    
    Before running:
    1. Ensure you have downloaded HMDA data using example_download_hmda_data.py
    2. Adjust the min_year and max_year variables above as needed
    3. Ensure you have sufficient disk space for the processed files
    
    Usage:
        python examples/example_import_workflow_post2018.py
    """
    
    success = main()
    
    if success:
        print("\n✅ Import workflow completed successfully!")
        print("Check the logs above for detailed information about the imported data.")
    else:
        print("\n❌ Import workflow failed!")
        print("Check the error messages above for troubleshooting information.")
