# HMDA Data Manager

Python package for managing CFPB's Home Mortgage Disclosure Act (HMDA) data for academic research.

## Overview

HMDA data files are large and complex, spanning multiple time periods with different schemas and formats. This package provides a structured pipeline to download, process, and analyze HMDA data efficiently using a modern medallion architecture.

## Key Features

- **Medallion Architecture Pipeline**: Raw → Bronze → Silver data flow with consistent schemas
- **Multi-Period Support**: Handles Pre-2007, 2007-2017, and Post-2018 data formats
- **Automated Downloads**: Direct integration with CFPB/FFIEC data sources
- **Efficient Storage**: Hive-partitioned Parquet files for fast querying
- **Schema Harmonization**: Automatic resolution of column naming inconsistencies across years
- **HMDAIndex Creation**: Unique identifiers for post-2018 records (format: `YYYYt_#########`)

## Architecture

### Medallion Layers

**Raw Layer** (`data/raw/`)
- Original ZIP files from CFPB, unchanged
- Organized by dataset type: `loans`, `panel`, `transmissal_series`

**Bronze Layer** (`data/bronze/`)
- Minimal processing: one Parquet file per archive
- Organized by dataset and time period: `{dataset}/{pre2007,period_2007_2017,post2018}/`
- All columns preserved as strings for schema stability

**Silver Layer** (`data/silver/`)
- Analysis-ready with proper data types and cleaned values
- Hive-partitioned by `activity_year` and `file_type` for efficient querying
- Schema harmonization applied across years
- Optional census tract variable removal to reduce file size

### Time Period Support

**Pre-2007** (Historical HMDA)
- Source: openICPSR Historical HMDA package (Forrester, 2021)
- Fixed-width format converted to delimited files
- Status: In development

**2007-2017** (Standardized Period)
- Source: FFIEC/CFPB HMDA platform
- Consistent schema with minor year-to-year variations
- Bronze and silver pipelines implemented

**Post-2018** (Modern HMDA)
- Source: FFIEC/CFPB HMDA platform
- Expanded fields with LEI-based reporting
- Includes HMDAIndex unique identifier
- Full bronze and silver pipelines implemented

## Dataset Types

- **Loans**: Loan Application Register (LAR) - individual mortgage applications and originations
- **Panel**: Lender information (respondent details, agency codes, parent entities)
- **Transmittal Series**: Submission metadata (transmittal sheet information)

## File Type Codes

Post-2018 data uses single-character codes:
- `a`: Three-year dataset
- `b`: One-year dataset
- `c`: Snapshot dataset (final annual release)
- `d`: MLAR (Modified LAR - preliminary)
- `e`: Panel data

## The HMDAIndex Variable

Post-2018 HMDA files contain no unique identifiers, making it difficult to reference specific records across derived datasets. This package automatically creates **HMDAIndex** during silver layer processing.

**Format**: `YYYYt_#########`
- `YYYY`: Four-digit activity year
- `t`: File type code (a/b/c/d)
- `#########`: Zero-padded nine-digit row number from original file

**Purpose**: Enables consistent record references across researchers using different versions of HMDA files.

## Installation

```bash
# Development mode (recommended)
pip install -e .
```

**Requirements**: Python 3.12+

## Getting Started

See `examples/README.md` for comprehensive workflow documentation and example scripts.

Basic workflow:
1. Download raw HMDA files (manual or automated)
2. Build bronze layer (minimal processing)
3. Build silver layer (analysis-ready, partitioned)
4. Query silver data using Polars LazyFrames

## Data Sources

**Historical HMDA (Pre-2007)**
Forrester, Andrew. Historical Home Mortgage Disclosure Act (HMDA) Data. Ann Arbor, MI: Inter-university Consortium for Political and Social Research [distributor], V1 (2021). https://doi.org/10.3886/E151921V1

**Modern HMDA (2007-Present)**
FFIEC/CFPB HMDA Data Publication Platform: https://ffiec.cfpb.gov/data-publication/

## Documentation

- `examples/README.md`: Complete workflow guide with example scripts
- `CLAUDE.md`: Development guidelines and coding conventions
- `docs/PLANNING.md`: Roadmap and planned features
- `DATA_QUALITY_ISSUES.md`: Known data quality issues and correction strategies

## Development

**Linting**: `ruff check <path>`
**Formatting**: `ruff format <path>`
**Testing**: `pytest`

See `CLAUDE.md` for detailed development guidelines.

## License

[Add license information]

## Citation

[Add citation information]
