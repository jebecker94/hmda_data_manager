# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

HMDA Data Manager is a Python package for managing CFPB's Home Mortgage Disclosure Act (HMDA) data for academic research. The package handles downloading, importing, processing, and analyzing large HMDA datasets across three distinct time periods with different data schemas.

## Development Environment

**Python Version**: 3.12+ (package requires >=3.12, target 3.13)

**Key Dependencies**:
- `polars` - Primary data processing library (preferred over pandas)
- `pyarrow` - Parquet file handling
- `python-decouple` - Configuration management
- `selenium`, `beautifulsoup4`, `requests` - Web scraping for downloads

**Installation**:
```bash
# Development mode (recommended)
pip install -e .
```

**Linting & Formatting**:
```bash
# Format code
ruff format <path>

# Lint code
ruff check <path>
```

**Testing**:
```bash
# Run all tests
pytest

# Run specific test file
pytest tests/test_cleaning_utilities.py
```

## Architecture Overview

### Medallion Data Pipeline

The project implements a medallion architecture (raw → bronze → silver) for data processing:

- **Raw Layer** (`data/raw/`): Original ZIP files from CFPB, unchanged
- **Bronze Layer** (`data/bronze/{loans,panel,transmissal_series}/{pre2007,period_2007_2017,post2018}`): Minimal processing, one parquet file per archive
- **Silver Layer** (`data/silver/{loans,panel,transmissal_series}/{period_2007_2017,post2018}/`): Analysis-ready, Hive-partitioned by `activity_year` and `file_type`

### Three Time Periods

The codebase handles three distinct HMDA data periods with different schemas:

1. **Pre-2007**: Historical data from openICPSR (fixed-width format converted to delimited)
2. **2007-2017** (`period_2007_2017`): Standardized format with some year-to-year variations
3. **Post-2018**: Current format with expanded fields and HMDAIndex unique identifier

### Package Structure

```
src/hmda_data_manager/
├── core/                    # Core functionality
│   ├── config.py           # Path configuration (PROJECT_DIR, DATA_DIR, etc.)
│   ├── download.py         # Download functions for CFPB data
│   └── import_data/        # Import functions by time period
│       ├── pre2007.py
│       ├── period_2007_2017.py
│       └── post2018.py     # build_bronze_post2018, build_silver_post2018
├── core/lenders/           # Lender panel/TS merge utilities
│   ├── post2018.py
│   └── period_2007_2017.py
├── utils/                  # Utility functions
│   ├── io.py              # File operations (unzip, delimiter detection)
│   ├── schema.py          # Schema parsing and column mapping
│   ├── cleaning.py        # Data cleaning pipeline
│   ├── identity.py        # Record keys and deduplication
│   ├── geo.py             # Tract variable handling
│   └── export.py          # Stata export
└── schemas/               # HTML schema files for validation
```

## Common Commands

### Building Bronze Layer (Raw → Bronze)

**Post-2018 data**:
```python
from hmda_data_manager.core import build_bronze_post2018

build_bronze_post2018("loans", min_year=2018, max_year=2024)
build_bronze_post2018("panel", min_year=2018, max_year=2024)
build_bronze_post2018("transmissal_series", min_year=2018, max_year=2024)
```

**2007-2017 data**:
```python
from hmda_data_manager.core import build_bronze_period_2007_2017

build_bronze_period_2007_2017("loans", min_year=2007, max_year=2017)
```

### Building Silver Layer (Bronze → Silver)

**Post-2018 data** (creates Hive-partitioned database):
```python
from hmda_data_manager.core import build_silver_post2018

# Creates partitions: activity_year=YYYY/file_type=X/*.parquet
build_silver_post2018("loans", min_year=2018, max_year=2024)
build_silver_post2018("panel", min_year=2018, max_year=2024)
build_silver_post2018("transmissal_series", min_year=2018, max_year=2024)
```

**2007-2017 data**:
```python
from hmda_data_manager.core import build_silver_period_2007_2017

# Optional: drop_tract_vars=True removes bulky census tract summary statistics
build_silver_period_2007_2017("loans", min_year=2007, max_year=2017, drop_tract_vars=True)
```

### Querying Silver Data

```python
import polars as pl

# Lazy load all post-2018 loans
df = pl.scan_parquet("data/silver/loans/post2018")

# Filter by partition (efficient - only reads relevant files)
df_2020 = pl.scan_parquet("data/silver/loans/post2018/activity_year=2020/file_type=c")

# SQL query on partitioned data
query = """
SELECT lei, loan_amount, action_taken
FROM self
WHERE activity_year = 2020 AND state_code = 'DC'
"""
result = df.sql(query).collect()
```

### Downloading Data

```python
from hmda_data_manager.core import download_hmda_files

# Download post-2018 data
download_hmda_files(
    years=range(2018, 2025),
    file_types=["snapshot", "one_year", "three_year"],
    include_mlar=False,
    download_folder="data/raw"
)
```

## Important Concepts

### HMDAIndex (Post-2018 Only)

Auto-generated unique identifier for post-2018 data with format `YYYYt_#########`:
- `YYYY`: Four-digit year
- `t`: File type code ('a'=three_year, 'b'=one_year, 'c'=snapshot, 'd'=MLAR)
- `#########`: Zero-padded nine-digit row number

Purpose: No unique identifiers exist in raw HMDA files. HMDAIndex enables consistent record references across derived datasets.

### File Type Codes

- `a`: Three-year dataset
- `b`: One-year dataset
- `c`: Snapshot dataset
- `d`: MLAR (Modified LAR - preliminary)
- `e`: Panel data (lender information)

### Dataset Types

- **loans**: Loan Application Register (LAR) - individual loan records
- **panel**: Lender panel information (respondent details)
- **transmissal_series**: Transmittal sheet metadata (submission info)

### Tract Variables

Post-2018 files include extensive census tract summary statistics (population, income, housing characteristics). These add significant file size. Use `drop_tract_vars=True` in silver build functions to remove them for analysis.

## Coding Conventions

### Polars-First Data Processing

- **Always prefer Polars over pandas** for new functionality
- Use `pl.scan_parquet()` for lazy loading (don't load full datasets into memory)
- Write silver layer with Hive partitioning:
  ```python
  lf.sink_parquet(
      pl.PartitionByKey(out_dir, by=[pl.col("activity_year"), pl.col("file_type")], include_key=True)
  )
  ```
- Process one input file at a time in silver builds to ensure consistent dtypes
- Keep derived/duplicate fields minimal in bronze; do conversions in silver

### Naming Conventions

- Use `period_2007_2017` (not `pre2018`) for 2007-2017 code and filenames
- Use `snake_case` for all Python variables, functions, modules, and files
- Example filenames: `01_example_*.py` for core workflows, `99_example_*.py` for non-core examples

### Function Patterns

- Import functions use consistent signatures: `build_*_*(dataset, min_year, max_year, replace=False, **kwargs)`
- Include `replace` parameter for overwrite behavior (default: `False` to avoid reprocessing)
- Use `should_process_output()` helper to check if output already exists
- Return None (not status objects) - log errors instead

### Error Handling

- Use structured logging with `logging.getLogger(__name__)`
- Log levels: DEBUG (processing steps), INFO (progress), WARNING (non-critical), ERROR (failures)
- Provide actionable error messages that guide users to solutions
- Continue processing other files when one fails (don't halt entire batch)

### Configuration

- All paths managed through `src/hmda_data_manager/core/config.py`
- Support environment variable overrides via `python-decouple`
- Key paths: `PROJECT_DIR`, `DATA_DIR`, `RAW_DIR`, `BRONZE_DIR`, `SILVER_DIR`
- Use `get_medallion_dir(stage, dataset, period)` for medallion paths

## Code Quality Standards

- **Type hints required** for all function parameters and return values
- **Docstrings required** for all public functions (NumPy/Google style)
- Use `pathlib.Path` (not string paths)
- Before editing code, read files first to understand current implementation
- Keep edits small and focused - avoid large refactors unless explicitly requested
- Maintain stable schemas and dtypes across partitions/years

## Testing

- Tests located in `tests/` directory
- Existing test coverage:
  - `test_get_file_schema.py` - Schema parsing validation
  - `test_cleaning_utilities.py` - Outlier detection, data cleaning
  - `test_import_common.py` - Import function helpers
  - `test_save_file_to_stata.py` - Export functionality
- Run tests before committing: `pytest`
- Note in PR if no tests collected

## Project Planning

- **PLANNING.md**: Core technical roadmap, code quality improvements, testing priorities
- **EXTENSIONS.md**: Analysis, visualization, and integration features (future work)
- Update planning docs as tasks are completed (check off items)

## Example Workflows

Located in `examples/` directory with comprehensive documentation:

1. **`01_example_download_hmda_data.py`**: Download workflow
2. **`02_example_import_workflow_post2018.py`**: Complete post-2018 import pipeline (recommended starting point)
3. **`03_example_import_workflow_2007_2017.py`**: 2007-2017 import pipeline
4. **`99_example_create_summaries.py`**: Combine panel and transmittal sheet data
5. **`99_example_load_dc_purchases.py`**: Query hive-partitioned silver data

See `examples/README.md` for full workflow documentation.

## Important Notes

- **Never commit data files** - they are large and contain sensitive information
- **Avoid monolithic convenience functions** - keep workflows explicit and composable
- **Don't import from `deprecated/` folder** - it's reference only
- **Prefer lazy/low-memory operations** - avoid scanning entire folders when single-file processing works
- Before deleting code, search for references and update imports
