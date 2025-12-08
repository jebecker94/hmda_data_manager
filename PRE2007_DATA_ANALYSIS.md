# Pre-2007 HMDA Data: Analysis and Implementation Roadmap

**Date:** 2025-12-06
**Purpose:** Document the current state of pre-2007 HMDA data and provide implementation recommendations

---

## Table of Contents

1. [Executive Summary](#executive-summary)
2. [Data Inventory](#data-inventory)
3. [Current Implementation Status](#current-implementation-status)
4. [Schema Analysis](#schema-analysis)
5. [Key Differences from Later Periods](#key-differences-from-later-periods)
6. [Challenges and Issues](#challenges-and-issues)
7. [Implementation Recommendations](#implementation-recommendations)
8. [Suggested Implementation Sequence](#suggested-implementation-sequence)

---

## Executive Summary

### Current State
- **Raw data exists**: 18 years of LAR (1981-2006), plus panel and transmittal series
- **Medallion architecture implemented**: `build_bronze_pre2007()` and `build_silver_pre2007()` functions
- **Bronze/silver layers**: Hive-partitioned by activity_year
- **Example workflow**: `examples/04_example_import_workflow_pre2007.py`
- **No schema files**: No HTML validation schemas for pre-2007 (not needed with current approach)

### Key Findings
- Pre-2007 data is **simpler** than later periods (23-38 columns vs 50-99 in later periods)
- **Schema evolution** exists: columns increase from 23 (1990) to 38 (2006)
- **1981 data is special**: Contains aggregate census tract data, not individual loans
- **Source**: openICPSR conversion of National Archives fixed-width files

### Completed Actions
1. ✅ Implemented `build_bronze_pre2007()` and `build_silver_pre2007()` functions
2. ✅ Handle schema evolution gracefully (two schema versions: 1990-2003 and 2004-2006)
3. ✅ Created example workflow script (`examples/04_example_import_workflow_pre2007.py`)
4. Extend to panel and transmittal series datasets

---

## Data Inventory

### Loans (LAR - Loan Application Register)

**Location:** `data/raw/loans/`

**Files:**
- `HMDA_LAR_1981_1989.zip` (39 MB) - Contains 9 separate TXT files (1981-1989)
- Individual year files: `HMDA_LAR_1990.zip` through `HMDA_LAR_2006.zip`

**Coverage:** 1981-2006 (26 years)

**Size Statistics:**
- Compressed: ~3.7 GB total
- Largest file: 2005 (443 MB)
- Smallest: 1990 (62 MB)

**Record Counts (sampled):**
- 1990: ~6.7M records
- 2000: ~13.5M records
- 2006: ~34M records

### Panel (Lender Information)

**Location:** `data/raw/HMDA_PANEL.zip` (6.4 MB)

**Coverage:** 1990-2006 (17 TXT files, one per year)

**Schema:** 15-16 columns
- Lender identification
- Agency information
- Parent company details
- Asset size

### Transmittal Series (Submission Metadata)

**Location:** `data/raw/HMDA_TS.zip` (5.3 MB)

**Coverage:** 1990-2006 (17 TXT files, one per year)

**Schema:** 15 columns
- Respondent information
- Contact details
- Edit status
- Tax ID

### Current Bronze/Silver State

**Bronze layer:** `data/bronze/`
- ❌ No `loans/pre2007/` directory
- ❌ No `panel/pre2007/` directory
- ❌ No `transmissal_series/pre2007/` directory

**Silver layer:** `data/silver/`
- ❌ No `loans/pre2007/` directory
- ❌ No `panel/pre2007/` directory
- ❌ No `transmissal_series/pre2007/` directory

**Action needed:** Create directory structure and build functions

---

## Current Implementation Status

### Existing Code

**File:** `src/hmda_data_manager/core/import_data/pre2007.py`

**Functions:**
1. `build_bronze_pre2007(dataset, min_year, max_year, replace)` - Creates bronze layer from raw ZIPs
2. `build_silver_pre2007(dataset, min_year, max_year, replace)` - Creates silver layer from bronze
3. Internal helpers:
   - `_standardize_geographic_codes(lf)` - Standardizes FIPS codes (2, 5, 11 digits)
   - `_harmonize_schema_pre2007(lf)` - Type conversions (31 int, 1 float)
   - `_rename_columns_pre2007(lf)` - Column renaming

**Current functionality:**
- ✅ Implements medallion architecture (bronze → silver)
- ✅ Reads pipe-delimited TXT from ZIP archives
- ✅ Handles schema evolution (23→38 columns across years)
- ✅ Converts numeric strings with proper scaling (loan_amount*1000, income*1000)
- ✅ Standardizes geographic codes (state, county, census tract, MSA/MD)
- ✅ Hive-partitioned output by activity_year
- ✅ Handles loans, panel, and transmittal series datasets

**Exported API:**
```python
from hmda_data_manager.core import build_bronze_pre2007, build_silver_pre2007
```

### Implemented Components

1. ✅ **Medallion architecture functions implemented**
2. ✅ **Example workflow:** `examples/04_example_import_workflow_pre2007.py`
3. ✅ **Inspection script:** `examples/inspect_bronze_pre2007.py` (loads silver data)

3. **No schema files:**
   - No `schemas/hmda_lar_schema_pre2007.html`
   - No validation infrastructure

4. **Incomplete dataset support:**
   - Only LAR implemented
   - Panel and TS not handled

---

## Schema Analysis

### Schema Evolution Timeline

#### 1981 Data (SPECIAL CASE - Aggregate Data)

**Format:** 25 columns of census tract-level aggregates

**Key columns:**
```
respondent_name, respondent_id, msamd, census_tract, state_code, county_code,
agency_code, census_validity, flag_govt, num_govt, vol_govt, flg_conv, num_conv,
vol_conv, flg_improv, num_improv, vol_improv, flg_multi, num_multi, vol_multi,
flg_nonocc, num_nonocc, vol_nunocc, record_quality, activity_year
```

**Note:** This is NOT individual loan data - it's aggregated by census tract. Contains counts and volumes by loan category.

**Recommendation:** Skip 1981 data in initial implementation or flag as aggregate data.

#### 1990-2003 Schema (Early Format)

**Format:** 23 columns, individual loan records

**Columns:**
```
activity_year, respondent_id, agency_code, loan_type, loan_purpose,
occupancy_type, loan_amount, action_taken, msamd, state_code, county_code,
census_tract, applicant_race_1, co_applicant_race_1, applicant_sex,
co_applicant_sex, income, purchaser_type, denial_reason_1, denial_reason_2,
denial_reason_3, edit_status, sequence_number
```

**Characteristics:**
- Basic demographic information (single race field per applicant)
- No ethnicity data
- No rate spread or HOEPA status
- No multiple race fields

#### 2004-2006 Schema (Expanded Format)

**Format:** 38 columns, individual loan records

**New fields added:**
- Multiple race fields (applicant_race_2 through applicant_race_5)
- Multiple co-applicant race fields (co_applicant_race_2 through co_applicant_race_5)
- Ethnicity fields (applicant_ethnicity, co_applicant_ethnicity)
- Rate spread
- HOEPA status
- Lien status
- Property type
- Preapproval status

**Full schema (2006):**
```
activity_year, respondent_id, agency_code, loan_type, loan_purpose, occupancy,
loan_amount, action_taken, msamd, state_code, county_code, census_tract,
applicant_sex, co_applicant_sex, income, purchaser_type, denial_reason_1,
denial_reason_2, denial_reason_3, edit_status, property_type, preapproval,
applicant_ethnicity, co_applicant_ethnicity, applicant_race_1, applicant_race_2,
applicant_race_3, applicant_race_4, applicant_race_5, co_applicant_race_1,
co_applicant_race_2, co_applicant_race_3, co_applicant_race_4,
co_applicant_race_5, rate_spread, hoepa_status, lien_status, sequence_number
```

**Column name differences:**
- `occupancy` (2004-2006) vs `occupancy_type` (1990-2003)
- Need to standardize via `rename_hmda_columns()`

### Panel Schema

**1990 Panel:** 15 columns
```
respondent_id, msamd, agency_code, agency_group, respondent_name, respondent_city,
respondent_state, respondent_fips, assets, other_lender_code, parent_id,
parent_name, parent_city, parent_state, activity_year
```

**2006 Panel:** 16 columns (adds `respondent_rssd`)

### Transmittal Series Schema

**2006 TS:** 15 columns
```
activity_year, agency_code, respondent_id, respondent_name, respondent_addr,
respondent_city, respondent_state, respondent_zip_code, parent_name,
parent_addr, parent_city, parent_state, parent_zip_code, edit_status, tax_id
```

### Data Format Characteristics

**File structure:**
- ZIP archives containing pipe-delimited TXT files
- Delimiter: `|`
- Encoding: Latin-1
- Headers: First row contains column names
- Structure: One TXT file per year per dataset

**Data types:**
- All columns stored as strings in raw files
- Require type conversion to numeric
- No special "Exempt" values (simpler than post-2018)

**File sizes (uncompressed estimate):**
- 1990 LAR: ~500 MB
- 2006 LAR: ~2.5 GB

---

## Key Differences from Later Periods

### vs. 2007-2017 Period

**Similarities:**
- No HMDAIndex unique identifier
- Numeric fields stored as strings
- Requires destringing and type conversion
- Basic loan application fields

**Differences:**
- ✅ **Simpler schema**: 23-38 columns vs 50+ in 2007-2017
- ✅ **No tract variables**: No census tract summary statistics
- ⚠️ **More schema evolution**: Greater variation year-to-year
- ⚠️ **Different naming**: Some legacy column names need mapping
- ⚠️ **Aggregate data**: 1981 is special case (aggregates, not loans)

### vs. Post-2018 Period

**Major Differences:**
- ❌ **No HMDAIndex**: No auto-generated unique identifier
- ❌ **No file type codes**: No 'a', 'b', 'c', 'e' classifications
- ❌ **No derived columns**: No derived_race, derived_ethnicity, etc.
- ❌ **No tract variables**: No tract_population, tract_minority_population_percent, etc.
- ❌ **No "Exempt" values**: Simpler missing data handling
- ✅ **Much simpler**: 23-38 columns vs 99 columns in post-2018
- ✅ **Smaller files**: More manageable data sizes
- ⚠️ **Different source**: openICPSR conversion vs direct CFPB downloads

### Comparison Table

| Feature | Pre-2007 | 2007-2017 | Post-2018 |
|---------|----------|-----------|-----------|
| **Unique ID** | None | None | HMDAIndex |
| **File types** | None | None | a, b, c, d, e |
| **Column count** | 23-38 | 50+ | 99 |
| **Derived cols** | No | No | Yes |
| **Tract vars** | No | Yes | Yes |
| **"Exempt" vals** | No | No | Yes |
| **Schema evolution** | High (2 versions) | Low | Very Low |
| **Data source** | openICPSR | CFPB | CFPB |
| **Complexity** | Low | Medium | High |

---

## Challenges and Issues

### Technical Challenges

#### 1. Schema Evolution
- **Issue**: Column count varies from 23 (1990) to 38 (2006)
- **Impact**: Need year-aware schema handling
- **Solutions:**
  - Create two schema versions: early (1990-2003) and late (2004-2006)
  - Use flexible column detection
  - Handle missing columns gracefully

#### 2. 1981 Aggregate Data
- **Issue**: 1981 contains census tract aggregates, not individual loans
- **Impact**: Fundamentally different data structure
- **Solutions:**
  - **Recommended**: Skip 1981 data (start at 1990)
  - **Alternative**: Flag as aggregate with `is_aggregate` column
  - **Document**: Note limitation in README

#### 3. Column Naming Inconsistencies
- **Issue**: "occupancy" (2006) vs "occupancy_type" (1990-2003)
- **Impact**: Need robust renaming logic
- **Solution**: Extend `rename_hmda_columns()` function

#### 4. Data Provenance
- **Issue**: Converted from fixed-width format by third party
- **Impact**: Potential encoding/conversion issues
- **Mitigation**: Validate record counts, test data quality

#### 5. No Validation Schemas
- **Issue**: No HTML schema files for automated validation
- **Impact**: Can't use `get_file_schema()` function
- **Workaround**: Define schemas programmatically in code

### Architectural Challenges

#### 1. ✅ Pattern Consistency (RESOLVED)
- ✅ **Implemented**: Medallion architecture with `build_bronze_pre2007()` + `build_silver_pre2007()`
- ✅ **Integrated**: Fully compatible with post-2018 and 2007-2017 patterns
- ✅ **Exported**: Available through `hmda_data_manager.core` API

#### 2. ✅ Infrastructure Complete (RESOLVED)
- ✅ **Bronze/silver directories** created with Hive partitioning
- ✅ **Example workflow** script available
- ✅ **Inspection utilities** for data validation

#### 3. ✅ Panel and TS Support (RESOLVED)
- ✅ **All datasets supported**: Loans (LAR), Panel, and Transmittal Series
- **Needed**: Panel and transmittal series support
- **Solution**: Extend functions to handle all three dataset types

---

## Implementation Recommendations

### Priority 1: Core Medallion Architecture (HIGH PRIORITY)

#### Implement `build_bronze_pre2007()`

**Signature:**
```python
def build_bronze_pre2007(
    dataset: Literal["loans", "panel", "transmissal_series"],
    min_year: int = 1990,  # Start at 1990 to skip 1981 aggregates
    max_year: int = 2006,
    replace: bool = False,
) -> None:
    """Create bronze layer parquet files for pre-2007 data.

    Reads raw ZIPs from data/raw/<dataset>, extracts, renames columns,
    destrings numeric fields, and writes one parquet per year to
    data/bronze/<dataset>/pre2007/.
    """
```

**Key functionality:**
- Extract TXT files from ZIP archives
- Apply column renaming via `rename_hmda_columns()`
- Destring numeric columns
- Save one parquet per year: `{dataset}_{year}.parquet`
- Use `should_process_output()` for skip logic
- Handle 1981-1989 multi-year archive for LAR

#### Implement `build_silver_pre2007()`

**Signature:**
```python
def build_silver_pre2007(
    dataset: Literal["loans", "panel", "transmissal_series"],
    min_year: int = 1990,
    max_year: int = 2006,
    replace: bool = False,
) -> None:
    """Create hive-partitioned silver layer for pre-2007 data.

    Processes bronze parquet files, applies additional standardization,
    and writes to data/silver/<dataset>/pre2007/activity_year=YYYY/*.parquet.
    """
```

**Key functionality:**
- Read from bronze layer
- Ensure consistent dtypes across years
- Add `file_type` column (all 'c' for public LAR)
- Create Hive partitions by `activity_year`
- Data quality validation
- Handle schema evolution gracefully

### Priority 2: Schema Evolution Handling (HIGH PRIORITY)

#### Create Year-Specific Logic

**Approach:**
```python
def _get_schema_version(year: int) -> str:
    """Determine schema version based on year."""
    if year <= 2003:
        return "early"  # 23 columns
    else:
        return "late"   # 38 columns

def _get_expected_columns(schema_version: str) -> list[str]:
    """Return expected columns for schema version."""
    if schema_version == "early":
        return EARLY_COLUMNS  # 23 columns
    else:
        return LATE_COLUMNS   # 38 columns
```

#### Define Column Lists in Config

**Add to `config.py`:**
```python
# Pre-2007 Data Constants
PRE2007_EARLY_COLUMNS = [...]  # 23 columns (1990-2003)
PRE2007_LATE_COLUMNS = [...]   # 38 columns (2004-2006)
PRE2007_NUMERIC_COLUMNS = [...]  # Columns to destring
```

#### Handle Missing Columns

**Strategy:**
- Use `lf.select()` with `strict=False` to handle missing columns
- Add missing columns with null values for consistency
- Log warnings when unexpected schema differences found

### Priority 3: Documentation and Examples (MEDIUM PRIORITY)

#### Create Example Workflow

**File:** `examples/04_example_import_workflow_pre2007.py`

**Content:**
```python
"""
Example: Pre-2007 HMDA Data Import Workflow

Complete workflow for importing pre-2007 HMDA data (1990-2006).

Prerequisites:
1. Download raw data from openICPSR
2. Place ZIP files in data/raw/

Note: 1981 data is excluded (aggregate data, not individual loans)
"""

from hmda_data_manager.core import (
    build_bronze_pre2007,
    build_silver_pre2007,
)

# Build bronze layer
build_bronze_pre2007("loans", min_year=1990, max_year=2006)
build_bronze_pre2007("panel", min_year=1990, max_year=2006)
build_bronze_pre2007("transmissal_series", min_year=1990, max_year=2006)

# Build silver layer
build_silver_pre2007("loans", min_year=1990, max_year=2006)
build_silver_pre2007("panel", min_year=1990, max_year=2006)
build_silver_pre2007("transmissal_series", min_year=1990, max_year=2006)
```

#### Update README.md

**Add section:**
```markdown
### Pre-2007 Data (1990-2006)

Source: openICPSR Project 151921 (converted from National Archives fixed-width files)

Workflow:
1. Download from openICPSR
2. Run `examples/04_example_import_workflow_pre2007.py`
3. Query from `data/silver/loans/pre2007/`

Note: 1981 data excluded (aggregate census tract data, not individual loans)
```

#### Update CLAUDE.md

**Add pre-2007 section:**
- Document schema evolution (early vs late)
- Explain 1981 exclusion
- Note openICPSR source requirement

### Priority 4: Testing and Validation (MEDIUM PRIORITY)

#### Create Test Suite

**File:** `tests/test_import_pre2007.py`

**Tests:**
1. Schema evolution handling
2. Column renaming correctness
3. Numeric type conversion
4. Record count validation
5. Cross-year dtype consistency

#### Data Validation Checks

**In silver build:**
```python
# Validate record counts
expected_counts = {...}  # From source documentation
actual_count = df.select(pl.len()).collect().item()
if expected_counts.get(year):
    assert actual_count == expected_counts[year]

# Validate schema completeness
assert "activity_year" in df.columns
assert "respondent_id" in df.columns
```

### Priority 5: Future Enhancements (LOW PRIORITY)

#### Create Schema HTML Files

**Files to create:**
- `schemas/hmda_lar_schema_pre2007_early.html` (1990-2003)
- `schemas/hmda_lar_schema_pre2007_late.html` (2004-2006)
- `schemas/hmda_panel_schema_pre2007.html`
- `schemas/hmda_ts_schema_pre2007.html`

**Benefits:**
- Programmatic validation via `get_file_schema()`
- Documentation reference
- Type inference automation

#### Lender Utilities

**Extend:** `src/hmda_data_manager/core/lenders/`

**Create:** `pre2007.py` module for panel + TS merging

#### Longitudinal Analysis Support

**Goal:** Enable queries across all three periods (pre-2007, 2007-2017, post-2018)

**Approach:**
- Identify common columns across periods
- Create standardized views
- Document period-specific differences

---

## Suggested Implementation Sequence

### Phase 1: Basic Medallion Architecture (Week 1)

**Tasks:**
1. ✅ Create `build_bronze_pre2007()` function for loans
2. ✅ Test with single year (2006)
3. ✅ Create bronze directory structure
4. ✅ Implement `should_process_output()` logic
5. ✅ Handle ZIP extraction

**Deliverable:** Working bronze layer for loans

### Phase 2: Silver Layer and Schema Evolution (Week 1-2)

**Tasks:**
6. ✅ Implement `build_silver_pre2007()` for loans
7. ✅ Add schema version detection
8. ✅ Handle early vs late column differences
9. ✅ Create Hive partitioning by activity_year
10. ✅ Test all years (1990-2006)

**Deliverable:** Working silver layer with proper partitioning

### Phase 3: Panel and Transmittal Series (Week 2)

**Tasks:**
11. ✅ Extend bronze/silver for panel dataset
12. ✅ Extend bronze/silver for transmissal_series dataset
13. ✅ Test complete workflow for all datasets

**Deliverable:** Full pre-2007 import pipeline

### Phase 4: Documentation (Week 3)

**Tasks:**
14. ✅ Create `examples/04_example_import_workflow_pre2007.py`
15. ✅ Update README.md with pre-2007 section
16. ✅ Update CLAUDE.md with schema details
17. ✅ Update `examples/README.md`

**Deliverable:** Complete documentation

### Phase 5: Testing and Polish (Week 3-4)

**Tasks:**
18. ✅ Create `tests/test_import_pre2007.py`
19. ✅ Add data validation checks
20. ✅ Improve error handling
21. ✅ Optimize performance
22. ✅ Code review and cleanup

**Deliverable:** Production-ready implementation

---

## Data Source Citation

**Source:** openICPSR Project 151921
**PI:** Andrew Forrester
**DOI:** 10.3886/E151921V1
**URL:** https://www.openicpsr.org/openicpsr/project/151921/version/V1/view

**Description:** Conversion of National Archives fixed-width HMDA data to pipe-delimited format for easier analysis.

**Required Citation:**
```
Forrester, Andrew. Converted Home Mortgage Disclosure Act Data, 1981-2006.
Inter-university Consortium for Political and Social Research [distributor],
2021-08-30. https://doi.org/10.3886/E151921V1
```

**Note:** This citation requirement should be added to README.md

---

## Appendix: Code Examples

### Example: Build Bronze Pre-2007 (Pseudocode)

```python
def build_bronze_pre2007(
    dataset: Literal["loans", "panel", "transmissal_series"],
    min_year: int = 1990,
    max_year: int = 2006,
    replace: bool = False,
) -> None:
    raw_folder = RAW_DIR / dataset
    bronze_folder = get_medallion_dir("bronze", dataset, "pre2007")
    bronze_folder.mkdir(parents=True, exist_ok=True)

    for year in range(min_year, max_year + 1):
        # Find ZIP archive for this year
        if dataset == "loans":
            if 1981 <= year <= 1989:
                archive = raw_folder / "HMDA_LAR_1981_1989.zip"
            else:
                archive = raw_folder / f"HMDA_LAR_{year}.zip"
        else:
            archive = raw_folder / f"HMDA_{dataset.upper()}.zip"

        save_file = bronze_folder / f"{dataset}_{year}.parquet"
        if not should_process_output(save_file, replace):
            continue

        # Extract and process
        raw_file = extract_from_zip(archive, year)
        df = pl.read_csv(raw_file, separator="|")

        # Standardize
        df = rename_hmda_columns(df)
        df = destring_numeric_columns(df)

        # Save
        df.write_parquet(save_file)
```

### Example: Schema Evolution Handling

```python
def _harmonize_schema_pre2007(lf: pl.LazyFrame, year: int) -> pl.LazyFrame:
    """Harmonize schema for pre-2007 data with year-specific handling."""

    # Rename legacy column names
    if year <= 2003:
        # Early schema uses "occupancy_type"
        pass
    else:
        # Late schema uses "occupancy" -> standardize to "occupancy_type"
        lf = lf.rename({"occupancy": "occupancy_type"}, strict=False)

    # Ensure expected columns exist
    schema_version = "early" if year <= 2003 else "late"
    expected_cols = _get_expected_columns(schema_version)

    # Add missing columns as nulls
    for col in expected_cols:
        if col not in lf.columns:
            lf = lf.with_columns(pl.lit(None).alias(col))

    # Destring numeric columns
    for col in PRE2007_NUMERIC_COLUMNS:
        if col in lf.columns:
            lf = lf.with_columns(
                pl.col(col).cast(pl.Float64, strict=False).alias(col)
            )

    return lf
```

---

## Summary

The pre-2007 HMDA data represents a **valuable historical dataset** covering 26 years of mortgage lending activity. While the infrastructure is partially complete, implementing the full medallion architecture will bring it in line with the rest of the project and enable comprehensive longitudinal analysis.

**Key implementation priorities:**
1. Build medallion architecture functions (bronze + silver)
2. Handle schema evolution (early vs late format)
3. Create comprehensive documentation and examples
4. Extend to panel and transmittal series datasets

**Timeline estimate:** 3-4 weeks for complete implementation including testing and documentation.
