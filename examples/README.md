# HMDA Data Manager Examples

This directory contains example scripts demonstrating how to use the `hmda_data_manager` package for various HMDA data tasks.

## Available Examples

### Core Functionality Examples

#### `01_example_download_hmda_data.py` 
Complete download workflow that replicates the original `download_hmda_data.py` script functionality using the new package structure.

**Features:**
- Downloads snapshot, one-year, and three-year datasets
- Optional MLAR (Modified LAR) files
- Optional historical files (2007-2017)
- Automatic file organization into subdirectories
- Error handling and logging
- Both convenience function and manual methods

**Usage:**
```bash
python examples/01_example_download_hmda_data.py
```

#### `02_example_import_workflow_post2018.py` ⭐ **NEW**
Comprehensive workflow for importing and processing post-2018 HMDA data using the new modular import functions. **Recommended starting point for new users.**

**Features:**
- Step-by-step import process for LAR, Panel, and Transmittal Sheet data
- Schema validation and data quality checks
- Combines datasets for integrated analysis
- Creates hive-partitioned database for efficient querying
- Detailed logging and progress tracking
- Data validation and summary statistics
- Focuses on post-2018 data format

**Usage:**
```bash
python examples/02_example_import_workflow_post2018.py
```

#### `99_example_create_summaries.py`
Creates combined lender datasets by merging Panel and Transmittal Sheet data for both pre-2018 and post-2018 periods. Essential for lender-level analysis.

**Features:**
- Combines post-2018 Panel and Transmittal Sheet data  
- Combines pre-2018 Panel and Transmittal Sheet data (2007-2017)
- Creates both CSV and Parquet outputs
- Data validation and quality checks
- Summary statistics and reporting
- Requirements checking functionality

**Prerequisites:** Run import workflows first to create Panel and TS data files.

**Usage:**
```bash
python examples/99_example_create_summaries.py
```

### Data Processing Examples

#### `99_example_load_dc_purchases.py`
Demonstrates loading and analyzing DC purchase data using SQL queries with Polars on hive-partitioned datasets.

**Prerequisites:** Run `02_example_import_workflow_post2018.py` first to create the silver dataset.

#### `99_example_print_year_type_summary.py`
Shows a simple summary by `activity_year` and `file_type` using Polars scans.

#### `99_examine_exempt_reporters.py`
Analyzes exempt reporters in HMDA data and identifies unusual reporting patterns.

#### `99_example_hmda_outlier_detection.py`
Demonstrates outlier detection techniques for HMDA loan data.

### Jupyter Notebooks

#### `99_isolation_forest_example.ipynb`
Interactive notebook showing isolation forest techniques for anomaly detection in HMDA data.

## Running Examples

### Prerequisites
Make sure the `hmda_data_manager` package is available in your Python path:

```bash
# Option 1: Install in development mode
pip install -e .

# Option 2: Add to Python path temporarily
export PYTHONPATH="${PYTHONPATH}:$(pwd)/src"
```

### Basic Usage
```bash
cd hmda_data_manager
python examples/01_example_download_hmda_data.py
```

### Customizing Examples
Most examples include configuration variables at the top that you can modify:

```python
# In 01_example_download_hmda_data.py
download_mlar = True          # Enable MLAR downloads
min_static_year = 2020        # Change year range
max_static_year = 2024
download_folder = "./my_data" # Change destination
```

## Example Workflow

Here's a typical workflow using the examples:

1. **Download data:**
   ```bash
   python examples/01_example_download_hmda_data.py
   ```

2. **Process data (build bronze/silver):**
   ```bash
   python examples/02_example_import_workflow_post2018.py
   ```

3. **Create lender summaries:**
   ```bash
   python examples/99_example_create_summaries.py
   ```

4. **Query the database efficiently:**
   ```bash
   python examples/99_example_load_dc_purchases.py
   ```

## Getting Help

- Check the docstrings in each example file for detailed explanations
- See the main README.md for package documentation
- Look at function documentation: `help(function_name)` in Python

## Contributing Examples

Have a useful HMDA analysis workflow? Consider contributing an example:

1. Follow the naming convention: `##_example_[task_description].py`
2. Include comprehensive docstrings and comments
3. Add configuration variables at the top for easy customization
4. Update this README.md with your example
