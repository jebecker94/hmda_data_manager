# HMDA Data Manager - Project Planning

The following roadmap outlines priorities for improving code organization, consistency, and valuable extensions using HMDA data.

## Phase 1: Code Organization & Consistency ✅

### Package Structure (Completed)
- [x] Migrate to `src/hmda_data_manager/` package structure
- [x] Create modular `core/`, `utils/`, `schemas/` subpackages  
- [x] Implement proper `__init__.py` files with clean APIs
- [x] Create comprehensive example workflows

### Documentation & Standards (Completed)
- [x] Update `README.md` with package usage examples
- [x] Document examples in `examples/README.md`
- [x] Establish coding standards in `AGENTS.md`

### Core Functionality (Completed)
- [x] Modularize import functions by time period (pre-2007, 2007-2017, post-2018)
- [x] Implement hive-partitioned database creation
- [x] Create summary/lender combination utilities
- [x] Build comprehensive download workflow

## Phase 2: Code Quality & Consistency

### Error Handling & Logging
- [ ] Standardize exception classes across all modules
  - [ ] `HMDAImportError`, `HMDAValidationError`, `HMDAConfigError`
  - [ ] Consistent error messages with actionable guidance
- [x] Implement structured logging with different levels (basic logging implemented throughout)
  - [x] DEBUG: Detailed processing steps (via logger.debug calls)
  - [x] INFO: Progress and completion status (via logger.info calls)
  - [x] WARNING: Non-critical issues (via logger.warning calls)
  - [x] ERROR: Critical failures (via logger.error calls)
- [ ] Create centralized logging configuration (currently using per-module logging.getLogger)

### Configuration Management
- [x] Expand `config.py` with comprehensive settings
  - [ ] Default file naming conventions
- [ ] Add configuration validation on startup
- [ ] Support multiple environment configurations (dev/prod)
- [ ] Create configuration documentation and examples

### Function Standardization
- [x] Standardize all import function signatures (mostly complete)
  - [x] Consistent parameter naming and ordering (`min_year`, `max_year`, `replace` pattern)
  - [ ] Standard return types (success/failure status) - currently returns None
  - [x] Unified keyword argument patterns
- [x] Implement consistent file handling patterns (mostly complete)
  - [x] Standardized overwrite modes (`replace` boolean parameter in all build functions)
  - [x] Consistent file existence checks (`should_process_output` helper)
  - [ ] Unified file validation checks (ad-hoc validation exists but not centralized)

## Phase 3: Testing & Validation

### Automated Testing
- [ ] Unit tests for all core functions (partial)
  - [ ] Import functions with mock data
  - [ ] Configuration loading and validation
  - [x] File handling edge cases (`test_cleaning_utilities.py` exists)
- [ ] Integration tests with sample HMDA data
  - [ ] End-to-end workflow tests
  - [ ] Cross-year compatibility tests
- [ ] Data quality tests (partial)
  - [ ] Schema compliance validation
  - [ ] Cross-reference integrity checks
  - [x] Statistical reasonableness tests (`test_cleaning_utilities.py` includes outlier tests)

### Schema Management
- [x] Add column mapping utilities for schema changes (`rename_hmda_columns` in `utils/schema.py`)
- [ ] Create schema comparison tools for validation across years

## Phase 4: HMDA-Specific Data Quality & Validation

### Automated Data Quality Checks
- [ ] **Missing Data Analysis** (partial - example script exists)
  - [x] Identify patterns in missing/exempt data (`99_examine_exempt_reporters.py` example)
  - [ ] Flag unusual exemption rates or patterns
  - [ ] Generate missing data reports by geography and time
- [x] **Outlier Detection** (basic implementation exists)
  - [x] Statistical outlier detection (`flag_outliers_basic` in `utils/cleaning.py`, `99_example_hmda_outlier_detection.py`)
  - [ ] Geographic outlier identification (unusual pricing by MSA)
  - [ ] Temporal outlier detection (sudden changes in lender behavior)
- [ ] **Cross-Year Consistency Checks**
  - [ ] Track lender panel information changes over time
  - [ ] Identify unusual year-over-year volume changes
  - [ ] Flag potential data quality issues in submissions
- [ ] **Regulatory Compliance Validation** (partial)
  - [ ] Check for required field completeness by lender type
  - [x] Validate geographic codes (`harmonize_census_tract` in `utils/cleaning.py`)
  - [x] Basic plausibility checks (`apply_plausibility_filters`, `clean_rate_spread` in `utils/cleaning.py`)

### Reference Data Validation  
- [x] **Geographic Validation** (partial)
  - [x] Cross-validate census tracts (`harmonize_census_tract` in `utils/cleaning.py`)
  - [ ] Check MSA/MD assignments for consistency
  - [ ] Flag invalid or deprecated geographic codes
- [ ] **Lender Information Validation**
  - [ ] LEI format and check-digit validation
  - [ ] Agency code consistency checks
  - [ ] Parent-subsidiary relationship validation

## Performance & Scalability

### Performance & Scalability
- [x] **Memory Optimization** (mostly complete)
  - [x] Implement chunked processing for large datasets (Polars LazyFrame provides lazy evaluation)
  - [ ] Memory profiling and optimization
  - [x] Lazy loading strategies for large files (Polars LazyFrame used throughout)
