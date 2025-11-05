# hmda_data_manager
Tools for managing CFPB's Home Mortgage Disclosure Act (HMDA) data for research projects.

# Overview
This project is designed to streamline the use of the HMDA data for academic research purposes.

# Motivation
The HMDA data files are large and unweildy. To my knowledge, there are few resources for managing these files for use in research projects.

# Functionality
Major functions on this project include:
1. **Medallion Architecture ETL Pipeline**
   - Raw → Bronze → Silver data processing with consistent schemas
   - Support for all HMDA time periods: Pre-2007 [In progress], 2007-2017, and Post-2018
   - Hive-partitioned silver layer for efficient querying
2. **Managing downloads**
   - Automated download tools for CFPB HMDA data
   - Tracking changes across major revisions of files
3. **Streamlined reading and storage**
   - Convert large files to parquet, with appropriate column types and convenient partitioning
   - Schema harmonization across years and file types
   - Robust dtype casting and NA value handling
4. **Standardizing variables, detecting outliers, and fixing data errors**
   - Year-by-reporter checks for systematic data errors (e.g., incorrect conventions for reporting rates, points+fees, etc.)
5. **Creating derived datasets**
   - Linking originated and purchased loans within and across years

# The HMDAIndex Variable
As part of the data ingestion process, these import scripts create a new variable called 'HMDAIndex' for data released starting in 2018.

Construction:
The HMDAIndex variable has format YYYYt_#########, which is constructed from three components:
- The data coverage year YYYY: The four-digit year covered by the data.
- The file type code t: A one-digit alphanumeric character identifying the HMDA file type. This takes values of 'a' for three-year files, 'b' for the one-year files, 'c' for the snapshot files, and 'd' for the (preliminary) MLAR files.
- The row number #########: A nine-digit number identifying the row number in the HMDA file for year YYYY and type code t. Note that values are left-padded with zeros and begin with 000000000.

Purpose:
There are no unique identifiers in the HMDA data files released starting in 2018. This makes it difficult to share code and derived datasets between researchers using their own versions of the HMDA files. The HMDAIndex variable follows a straightforward construction procedure and makes it easy for any researcher to reconsruct a unique identifier to reference specific HMDA observations in derived datasets like crosswalks.

# Matching HMDA Sellers and Purchasers
HMDA's reporting requirements mean that loans which are originated and sold to a non-GSE are reported twice--once as an originated loan (action_taken==1) and once as a purchased loan (action_taken==6).

We provide preliminary matching scripts that match loan originations to subsequent loan purchases.

This can be used in a number of ways:
- Studying seller-purchaser relationships in the wholesale mortgage market
- Improving matches between HMDA and other datasets by providing a better lender identifier for subsequent matches (e.g., matching forward to MBS transactions)

# How to Use This Project
In order to use this project, there are a few manual steps to take before you can run the code.

## Download HMDA data
You can get HMDA data by either:
- Navigating to the HMDA data website and download the static files you with to use, and place the zip files in the raw folder.
- Using the provided automated download scripts with any necessary changes to user agents, etc.

### Note on data sourcing:
Historical HMDA (pre-2007)

1990–2006 HMDA
For historical data, this project relies on the Historical Home Mortgage Disclosure Act (HMDA) Data package on openICPSR (PI: Andrew Forrester). These files convert the pre-2006 fixed-width HMDA text from the National Archives into delimited formats that are easier to analyze. DOI: 10.3886/E151921V1. Access: https://www.openicpsr.org/openicpsr/project/151921/version/V1/view

Modern HMDA (2007–present)
From 2007 onward, HMDA public data are distributed via the FFIEC/CFPB HMDA Publication platform. This repository’s code auto-downloads the 2007–present data directly from that portal. For documentation and manual access, use the FFIEC/CFPB HMDA site. 

Suggested citation for historical source:
   Forrester, Andrew. Historical Home Mortgage Disclosure Act (HMDA) Data. Ann Arbor, MI: Inter-university Consortium for Political and Social Research [distributor], V1 (2021). https://doi.org/10.3886/E151921V1


## Medallion Data Architecture

This package implements a modern medallion architecture for HMDA data processing:

### Data Layout Structure
- **Raw**: `data/raw/{loans,panel,transmissal_series}` (Original ZIP files, unchanged)
- **Bronze**: `data/bronze/{loans,panel,transmissal_series}/{pre2007,period_2007_2017,post2018}` (Minimal processing, one parquet per archive)
- **Silver**: `data/silver/{loans,panel,transmissal_series}/{period_2007_2017,post2018}/activity_year=YYYY/file_type=X/*.parquet` (Hive-partitioned, analysis-ready)

### Post-2018 Data (2018-2024)

```python
from hmda_data_manager.core import build_bronze_post2018, build_silver_post2018

# Choose years
min_year, max_year = 2018, 2024

# Bronze (per dataset)
build_bronze_post2018("loans", min_year=min_year, max_year=max_year)
build_bronze_post2018("panel", min_year=min_year, max_year=max_year)
build_bronze_post2018("transmissal_series", min_year=min_year, max_year=max_year)

# Silver (hive-partitioned with HMDAIndex, file_type, tract variable handling)
build_silver_post2018("loans", min_year=min_year, max_year=max_year)
build_silver_post2018("panel", min_year=min_year, max_year=max_year)
build_silver_post2018("transmissal_series", min_year=min_year, max_year=max_year)
```

### 2007-2017 Data (Standardized Period)

```python
from hmda_data_manager.core import build_bronze_period_2007_2017, build_silver_period_2007_2017

# Bronze (loans only for this period)
build_bronze_period_2007_2017("loans", min_year=2007, max_year=2017)

# Silver (with schema harmonization, dtype casting, and tract variable handling)
build_silver_period_2007_2017("loans", min_year=2007, max_year=2017, drop_tract_vars=True)
```

### Key Features

- **Schema Harmonization**: Automatically resolves column naming inconsistencies across years
- **Robust Type Casting**: Handles mixed string/numeric columns with proper NA value conversion
- **Tract Variable Management**: Optional dropping of bulky census tract summary statistics
- **Hive Partitioning**: Efficient querying by `activity_year` and `file_type`
- **File Type Detection**: Smart inference from filenames (three_year→'a', one_year→'b', public_lar→'c', nationwide→'d')

### Reading Silver Data

```python
import polars as pl

# Load all post-2018 loans
df_post2018 = pl.scan_parquet("data/silver/loans/post2018")

# Load all 2007-2017 loans  
df_2007_2017 = pl.scan_parquet("data/silver/loans/period_2007_2017")

# Load specific year and file type
df_2020_snapshot = pl.scan_parquet("data/silver/loans/post2018/activity_year=2020/file_type=c")
```