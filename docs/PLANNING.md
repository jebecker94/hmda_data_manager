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
- [x] Establish coding standards in `CLAUDE.md`

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

### Configuration Management
- [x] Expand `config.py` with comprehensive settings
- [ ] Add configuration validation on startup
- [ ] Create configuration documentation and examples

### Function Standardization
- [x] Standardize all import function signatures (mostly complete)
  - [x] Consistent parameter naming and ordering (`min_year`, `max_year`, `replace` pattern)
  - [x] Unified keyword argument patterns
- [x] Implement consistent file handling patterns (mostly complete)
  - [x] Standardized overwrite modes (`replace` boolean parameter in all build functions)
  - [x] Consistent file existence checks (`should_process_output` helper)
  - [ ] Unified file validation checks (ad-hoc validation exists but not centralized)

## Phase 3: Schema Management & Validation

### Schema Management
- [x] Add column mapping utilities for schema changes (`rename_hmda_columns` in `utils/schema.py`)
- [ ] Create schema comparison tools for validation across years

## Phase 4: HMDA-Specific Data Quality & Validation

### Automated Data Quality Checks
- [x] **Missing Data Analysis** (example script exists)
  - [x] Identify patterns in missing/exempt data (`99_examine_exempt_reporters.py` example)
- [x] **Outlier Detection** (implementation exists)
  - [x] Statistical outlier detection (`flag_outliers_basic` in `utils/cleaning.py`, `99_example_hmda_outlier_detection.py`, `99_isolation_forest_example.py`)
- [x] **Regulatory Compliance Validation** (basic implementation)
  - [x] Validate geographic codes (`harmonize_census_tract` in `utils/cleaning.py`)
  - [x] Basic plausibility checks (`apply_plausibility_filters`, `clean_rate_spread` in `utils/cleaning.py`)

### Reference Data Validation
- [x] **Geographic Validation** (implemented)
  - [x] Cross-validate census tracts (`harmonize_census_tract` in `utils/cleaning.py`)

## Phase 5: Performance & Scalability

- [x] **Memory Optimization** (complete)
  - [x] Implement chunked processing for large datasets (Polars LazyFrame provides lazy evaluation)
  - [x] Lazy loading strategies for large files (Polars LazyFrame used throughout)
