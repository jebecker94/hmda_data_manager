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
- **Raw data exists**: 18 years of LAR (1990-2006), plus panel and transmittal series
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

### Panel (Lender Information)

**Location:** `data/raw/HMDA_PANEL.zip` (6.4 MB)

### Transmittal Series (Submission Metadata)

**Location:** `data/raw/HMDA_TS.zip` (5.3 MB)

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
- Need to standardize

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
