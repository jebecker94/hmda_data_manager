# HMDA Data Manager Examples

This directory contains example scripts demonstrating how to use the `hmda_data_manager` package for various HMDA data tasks.

## Available Examples

### Core Functionality Examples

#### `example_download_hmda_data.py` 
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
python examples/example_download_hmda_data.py
```

#### `example_import_workflow_post2018.py` ⭐ **NEW**
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
python examples/example_import_workflow_post2018.py
```

### Data Processing Examples

#### `example_load_dc_purchases.py`
Demonstrates loading and analyzing DC purchase data using SQL queries with Polars on hive-partitioned datasets.

**Prerequisites:** Run `example_import_workflow_post2018.py` first to create the database.

#### `example_save_partitioned_dataset.py`
Shows how to save HMDA data as partitioned datasets for efficient querying.

#### `examine_exempt_reporters.py`
Analyzes exempt reporters in HMDA data and identifies unusual reporting patterns.

#### `example_hmda_outlier_detection.py`
Demonstrates outlier detection techniques for HMDA loan data.

### Jupyter Notebooks

#### `isolation_forest_example.ipynb`
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
python examples/example_download_hmda_data.py
```

### Customizing Examples
Most examples include configuration variables at the top that you can modify:

```python
# In example_download_hmda_data.py
download_mlar = True          # Enable MLAR downloads
min_static_year = 2020        # Change year range
max_static_year = 2024
download_folder = "./my_data" # Change destination
```

## Example Workflow

Here's a typical workflow using the examples:

1. **Download data:**
   ```bash
   python examples/example_download_hmda_data.py
   ```

2. **Process data (creates hive database):**
   ```bash
   python examples/example_import_workflow_post2018.py
   ```

3. **Query the database efficiently:**
   ```bash
   python examples/example_load_dc_purchases.py
   ```

## Getting Help

- Check the docstrings in each example file for detailed explanations
- See the main README.md for package documentation
- Look at function documentation: `help(function_name)` in Python

## Contributing Examples

Have a useful HMDA analysis workflow? Consider contributing an example:

1. Follow the naming convention: `example_[task_description].py`
2. Include comprehensive docstrings and comments
3. Add configuration variables at the top for easy customization
4. Update this README.md with your example

## Migration Notes

If you're migrating from the old script-based approach:

| Old Script | New Example | Notes |
|------------|-------------|-------|
| `download_hmda_data.py` | `example_download_hmda_data.py` | ✅ Complete |
| `import_hmda_data.py` | `example_import_workflow_post2018.py` | ✅ Complete (post-2018) |
| `create_summary_files.py` | `example_create_summaries.py` | 🚧 Coming soon |

The new examples use the modular package structure and provide both beginner-friendly convenience functions and advanced customization options.
