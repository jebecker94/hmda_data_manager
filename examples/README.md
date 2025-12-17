# HMDA Data Manager Examples

This directory contains example scripts demonstrating how to use the `hmda_data_manager` package for various HMDA data tasks.

## Overview

The HMDA Data Manager provides **three ways** to work with HMDA data:

1. **Python API** - Import workflow functions directly in your code
2. **CLI Commands** - Use command-line interface for quick operations
3. **Example Scripts** - Copy and customize example workflows

All three methods use the same underlying workflow functions for consistency.

## Quick Start

### Method 1: Python API (Programmatic)

```python
from hmda_data_manager import (
    download_workflow,
    import_post2018_workflow,
)

# Download data
download_workflow(years=range(2018, 2025))

# Import and process
results = import_post2018_workflow(min_year=2018, max_year=2024)
```

### Method 2: CLI Commands

```bash
# Download data
hmda download --years 2018-2024

# Import post-2018 data
hmda import post2018 --min-year 2018 --max-year 2024

# Import 2007-2017 data
hmda import 2007-2017 --min-year 2007 --max-year 2017 --drop-tract-vars

# Import pre-2007 data
hmda import pre2007 --min-year 1990 --max-year 2006
```

### Method 3: Example Scripts

```bash
# Run example workflows
python examples/01_example_download_hmda_data.py
python examples/02_example_import_workflow_post2018.py
```

## Available Examples

### Core Workflow Examples (01-04)

These examples demonstrate the complete data pipeline from download to analysis-ready data.

#### `01_example_download_hmda_data.py`
Downloads HMDA files from CFPB website using the `download_workflow()` function.

**Features:**
- Downloads snapshot, one-year, and three-year datasets
- Optional MLAR (Modified LAR) files
- Optional historical files (2007-2017)
- Automatic file organization into subdirectories
- Error handling and logging

**Usage:**
```bash
python examples/01_example_download_hmda_data.py

# OR use CLI:
hmda download --years 2018-2024
```

#### `02_example_import_workflow_post2018.py` ⭐ **RECOMMENDED**
Imports and processes post-2018 HMDA data (2018-2024) using `import_post2018_workflow()`.

**Features:**
- Builds Bronze layer (one parquet file per archive)
- Builds Silver layer (Hive-partitioned by activity_year and file_type)
- Processes loans, panel, and transmissal_series datasets
- Creates HMDAIndex unique identifier
- Detailed logging and progress tracking

**Usage:**
```bash
python examples/02_example_import_workflow_post2018.py

# OR use CLI:
hmda import post2018 --min-year 2018 --max-year 2024

# Process only specific datasets:
hmda import post2018 --min-year 2020 --max-year 2024 --datasets loans panel
```

**Prerequisites:** Run `01_example_download_hmda_data.py` first.

#### `03_example_import_workflow_2007_2017.py`
Imports and processes 2007-2017 HMDA data using `import_2007_2017_workflow()`.

**Features:**
- Builds Bronze and Silver layers for historical data
- Only loans dataset available for this period
- Option to drop bulky census tract variables (recommended)

**Usage:**
```bash
python examples/03_example_import_workflow_2007_2017.py

# OR use CLI:
hmda import 2007-2017 --min-year 2007 --max-year 2017 --drop-tract-vars
```

**Prerequisites:** Download historical files from CFPB website.

#### `04_example_import_workflow_pre2007.py`
Imports and processes pre-2007 HMDA data (1990-2006) using `import_pre2007_workflow()`.

**Features:**
- Builds Bronze and Silver layers for historical data
- Processes loans, panel, and transmissal_series datasets
- Handles schema evolution (1990-2003: 23 cols, 2004-2006: 38 cols)

**Usage:**
```bash
python examples/04_example_import_workflow_pre2007.py

# OR use CLI:
hmda import pre2007 --min-year 1990 --max-year 2006
```

**Prerequisites:** Download data from openICPSR Project 151921.

### Analysis Examples (99_*)

These examples demonstrate specific analysis tasks and queries on processed data.

#### `99_example_create_summaries.py`
Creates combined lender datasets by merging Panel and Transmittal Sheet data.

**Prerequisites:** Run import workflows first to create Panel and TS data files.

#### `99_example_load_dc_purchases.py`
Demonstrates loading and analyzing DC purchase data using SQL queries with Polars.

**Prerequisites:** Run `02_example_import_workflow_post2018.py` first.

#### `99_example_print_year_type_summary.py`
Shows a simple summary by `activity_year` and `file_type` using Polars scans.

#### `99_examine_exempt_reporters.py`
Analyzes exempt reporters in HMDA data and identifies unusual reporting patterns.

#### `99_example_hmda_outlier_detection.py`
Demonstrates outlier detection techniques for HMDA loan data.

### Jupyter Notebooks

#### `99_isolation_forest_example.ipynb`
Interactive notebook showing isolation forest techniques for anomaly detection in HMDA data.

## Workflow Functions Reference

### Download Workflow

```python
from hmda_data_manager import download_workflow

download_workflow(
    years=range(2018, 2025),         # Year range to download
    destination_folder=None,          # Optional: custom destination
    include_mlar=False,              # Include Modified LAR files
    include_historical=False,        # Include 2007-2017 files
    pause_length=5,                  # Seconds between downloads
    wait_time=10,                    # Seconds to wait for JS to load
    overwrite_mode="skip",           # "skip", "always", "if_newer", "if_size_diff"
)
```

### Import Post-2018 Workflow

```python
from hmda_data_manager import import_post2018_workflow

results = import_post2018_workflow(
    min_year=2018,                   # Minimum year to process
    max_year=2024,                   # Maximum year to process
    datasets=None,                   # None = all, or ["loans", "panel", "transmissal_series"]
    replace=False,                   # Replace existing files
)
# Returns: {'loans': True, 'panel': True, 'transmissal_series': True}
```

### Import 2007-2017 Workflow

```python
from hmda_data_manager import import_2007_2017_workflow

success = import_2007_2017_workflow(
    min_year=2007,                   # Minimum year to process
    max_year=2017,                   # Maximum year to process
    drop_tract_vars=True,            # Drop bulky tract variables (recommended)
    replace=False,                   # Replace existing files
)
# Returns: True if successful, False otherwise
```

### Import Pre-2007 Workflow

```python
from hmda_data_manager import import_pre2007_workflow

results = import_pre2007_workflow(
    min_year=1990,                   # Minimum year to process
    max_year=2006,                   # Maximum year to process
    datasets=None,                   # None = all, or ["loans", "panel", "transmissal_series"]
    replace=False,                   # Replace existing files
)
# Returns: {'loans': True, 'panel': True, 'transmissal_series': True}
```

## CLI Commands Reference

### Download Commands

```bash
# Download data for specific years
hmda download --years 2018-2024

# Download single year
hmda download --years 2020

# Download with MLAR files
hmda download --years 2018-2024 --include-mlar

# Download with historical files
hmda download --years 2018-2024 --include-historical

# Download to custom location
hmda download --years 2020-2024 --destination ./my_data

# Control overwrite behavior
hmda download --years 2018-2024 --overwrite if_newer

# Adjust timing
hmda download --years 2018-2024 --pause 10 --wait 15
```

### Import Commands

#### Post-2018 Data

```bash
# Import all datasets
hmda import post2018 --min-year 2018 --max-year 2024

# Import specific datasets
hmda import post2018 --min-year 2020 --max-year 2024 --datasets loans panel

# Replace existing files
hmda import post2018 --min-year 2018 --max-year 2024 --replace
```

#### 2007-2017 Data

```bash
# Import with default settings (drop tract vars)
hmda import 2007-2017 --min-year 2007 --max-year 2017 --drop-tract-vars

# Import without dropping tract vars (larger files)
hmda import 2007-2017 --min-year 2007 --max-year 2017

# Replace existing files
hmda import 2007-2017 --min-year 2007 --max-year 2017 --drop-tract-vars --replace
```

#### Pre-2007 Data

```bash
# Import all datasets
hmda import pre2007 --min-year 1990 --max-year 2006

# Import specific datasets
hmda import pre2007 --min-year 1990 --max-year 2006 --datasets loans

# Replace existing files
hmda import pre2007 --min-year 1990 --max-year 2006 --replace
```

### Global Options

```bash
# Set log level
hmda --log-level DEBUG download --years 2020

# Get help
hmda --help
hmda download --help
hmda import --help
hmda import post2018 --help
```

## Running Examples

### Prerequisites

Make sure the `hmda_data_manager` package is installed:

```bash
# Option 1: Install in development mode (recommended)
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
# In 02_example_import_workflow_post2018.py
min_year = 2020        # Change year range
max_year = 2024
```

## Complete Workflow

Here's a typical workflow for processing HMDA data:

### Using Example Scripts

```bash
# 1. Download data
python examples/01_example_download_hmda_data.py

# 2. Process data (build bronze/silver)
python examples/02_example_import_workflow_post2018.py

# 3. Create lender summaries
python examples/99_example_create_summaries.py

# 4. Query the database efficiently
python examples/99_example_load_dc_purchases.py
```

### Using CLI Commands

```bash
# 1. Download data
hmda download --years 2018-2024

# 2. Process data
hmda import post2018 --min-year 2018 --max-year 2024

# 3. Analyze (use Python scripts or notebooks)
python examples/99_example_load_dc_purchases.py
```

### Using Python API

```python
from hmda_data_manager import (
    download_workflow,
    import_post2018_workflow,
)

# 1. Download data
download_workflow(years=range(2018, 2025))

# 2. Process data
results = import_post2018_workflow(min_year=2018, max_year=2024)

# 3. Query data (use Polars or DuckDB)
import polars as pl
df = pl.scan_parquet("data/silver/loans/post2018")
result = df.filter(pl.col("state_code") == "DC").collect()
```

## Data Organization

After running the workflows, your data will be organized as follows:

```
data/
├── raw/                                    # Downloaded files
│   ├── loans/
│   ├── panel/
│   ├── transmissal_series/
│   ├── msamd/
│   └── misc/
├── bronze/                                 # Parquet files (one per archive)
│   ├── loans/{post2018,period_2007_2017,pre2007}/
│   ├── panel/{post2018,pre2007}/
│   └── transmissal_series/{post2018,pre2007}/
└── silver/                                 # Hive-partitioned (analysis-ready)
    ├── loans/{post2018,period_2007_2017,pre2007}/
    │   └── activity_year=YYYY/file_type=X/
    ├── panel/{post2018,pre2007}/
    │   └── activity_year=YYYY/file_type=X/
    └── transmissal_series/{post2018,pre2007}/
        └── activity_year=YYYY/file_type=X/
```

## Getting Help

- Check the docstrings in each example file for detailed explanations
- Use `hmda --help` for CLI documentation
- See the main README.md for package documentation
- Look at function documentation: `help(function_name)` in Python
- Review CLAUDE.md for development guidance

## Contributing Examples

Have a useful HMDA analysis workflow? Consider contributing an example:

1. Follow the naming convention: `##_example_[task_description].py`
2. Include comprehensive docstrings and comments
3. Add configuration variables at the top for easy customization
4. Update this README.md with your example
