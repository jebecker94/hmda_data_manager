# -*- coding: utf-8 -*-
"""
Configuration management for HMDA Data Manager.

This module handles path configuration and environment variable setup
for the HMDA data management package.
"""

# Import Packages
from decouple import config
from pathlib import Path
from typing import Literal

# Specific Data Folders  
# Note: __file__.parent.parent.parent.parent goes from src/hmda_data_manager/core/ back to project root
PROJECT_DIR = Path(config("PROJECT_DIR", default=Path(__file__).parent.parent.parent.parent))
DATA_DIR = Path(config("DATA_DIR", default=PROJECT_DIR / "data"))
RAW_DIR = Path(config("HMDA_RAW_DIR", default=DATA_DIR / "raw"))
CLEAN_DIR = Path(config("HMDA_CLEAN_DIR", default=DATA_DIR / "clean"))

# Medallion layout directories
BRONZE_DIR = Path(config("HMDA_BRONZE_DIR", default=DATA_DIR / "bronze"))
SILVER_DIR = Path(config("HMDA_SILVER_DIR", default=DATA_DIR / "silver"))


# ============================================================================
# Post-2018 Data Constants
# ============================================================================

# HMDAIndex column name (unique identifier for post-2018 records)
HMDA_INDEX_COLUMN = "HMDAIndex"

# Derived columns that are dropped in bronze layer
DERIVED_COLUMNS = [
    "derived_loan_product_type",
    "derived_race",
    "derived_ethnicity",
    "derived_sex",
    "derived_dwelling_category",
]

# Census tract summary statistics columns (optionally dropped for size reduction)
POST2018_TRACT_COLUMNS = [
    "tract_population",
    "tract_minority_population_percent",
    "ffiec_msa_md_median_family_income",
    "tract_to_msa_income_percentage",
    "tract_owner_occupied_units",
    "tract_one_to_four_family_homes",
    "tract_median_age_of_housing_units",
]

# Float columns for post-2018 data
POST2018_FLOAT_COLUMNS = [
    "interest_rate",
    "combined_loan_to_value_ratio",
    "rate_spread",
    "total_loan_costs",
    "total_points_and_fees",
    "origination_charges",
    "discount_points",
    "lender_credits",
    "tract_minority_population_percent",
    "ffiec_msa_md_median_family_income",
    "tract_to_msa_income_percentage",
]

# Integer columns for post-2018 data
POST2018_INTEGER_COLUMNS = [
    "activity_year",
    "loan_type",
    "loan_purpose",
    "occupancy_type",
    "loan_amount",
    "action_taken",
    "msa_md",
    "loan_term",
    "derived_msa_md",
    "applicant_race_1",
    "applicant_race_2",
    "applicant_race_3",
    "applicant_race_4",
    "applicant_race_5",
    "co_applicant_race_1",
    "co_applicant_race_2",
    "co_applicant_race_3",
    "co_applicant_race_4",
    "co_applicant_race_5",
    "applicant_ethnicity_1",
    "applicant_ethnicity_2",
    "applicant_ethnicity_3",
    "applicant_ethnicity_4",
    "applicant_ethnicity_5",
    "co_applicant_ethnicity_1",
    "co_applicant_ethnicity_2",
    "co_applicant_ethnicity_3",
    "co_applicant_ethnicity_4",
    "co_applicant_ethnicity_5",
    "applicant_sex",
    "co_applicant_sex",
    "income",
    "multifamily_affordable_units",
    "property_value",
    "prepayment_penalty_term",
    "intro_rate_period",
    "purchaser_type",
    "submission_of_application",
    "initially_payable_to_institution",
    "preapproval",
    "lien_status",
    "reverse_mortgage",
    "open_end_line_of_credit",
    "business_or_commercial_purpose",
    "hoepa_status",
    "negative_amortization",
    "interest_only_payment",
    "balloon_payment",
    "other_nonamortizing_features",
    "construction_method",
    "manufactured_home_secured_property_type",
    "manufactured_home_land_property_interest",
    "applicant_credit_score_type",
    "co_applicant_credit_score_type",
    "applicant_race_observed",
    "co_applicant_race_observed",
    "applicant_ethnicity_observed",
    "co_applicant_ethnicity_observed",
    "applicant_sex_observed",
    "co_applicant_sex_observed",
    "aus_1",
    "aus_2",
    "aus_3",
    "aus_4",
    "aus_5",
    "denial_reason_1",
    "denial_reason_2",
    "denial_reason_3",
    "denial_reason_4",
    "tract_population",
    "tract_owner_occupied_units",
    "tract_one_to_four_family_homes",
    "tract_median_age_of_housing_units",
    "conforming_loan_limit",
    "agency_code",
    "assets",
    "other_lender_code",
    "lar_count",
    "calendar_quarter",
]

# Columns that may contain "Exempt" values requiring special handling
POST2018_EXEMPT_COLUMNS = [
    "combined_loan_to_value_ratio",
    "interest_rate",
    "rate_spread",
    "loan_term",
    "prepayment_penalty_term",
    "intro_rate_period",
    "income",
    "multifamily_affordable_units",
    "property_value",
    "total_loan_costs",
    "total_points_and_fees",
    "origination_charges",
    "discount_points",
    "lender_credits",
]


# ============================================================================
# Pre-2007 Data Constants
# ============================================================================

# Columns to convert to integers in pre-2007 silver layer
PRE2007_INTEGER_COLUMNS = [
    "activity_year",
    "loan_amount",  # Will be multiplied by 1000
    "income",  # Will be multiplied by 1000
    "occupancy_type",
    "edit_status",  # 5=Validity edit failure(s), 6=Quality edit failure(s), 7=Both
    "sequence_number",
    "lien_status",
    "hoepa_status",
    "preapproval",
    "property_type",
    "loan_type",
    "loan_purpose",
    "action_taken",
    "purchaser_type",
    # Demographic columns (non-numeric values treated as errors, converted to null)
    "applicant_sex",
    "co_applicant_sex",
    "applicant_ethnicity",
    "co_applicant_ethnicity",
    "applicant_race_1",
    "applicant_race_2",
    "applicant_race_3",
    "applicant_race_4",
    "applicant_race_5",
    "co_applicant_race_1",
    "co_applicant_race_2",
    "co_applicant_race_3",
    "co_applicant_race_4",
    "co_applicant_race_5",
    # Denial reasons (non-numeric values treated as errors, converted to null)
    "denial_reason_1",
    "denial_reason_2",
    "denial_reason_3",
]

# Columns to convert to floats in pre-2007 silver layer
PRE2007_FLOAT_COLUMNS = [
    "rate_spread",
]

# Note: msa_md is standardized to 5-digit string with leading zeros in _standardize_geographic_codes()

# Note: agency_code contains both numeric (1-7) and letter codes (B,C,D,E,X)
# Letter codes indicate State Exempts:
#   B = FRS (Federal Reserve System)
#   C = FDIC (Federal Deposit Insurance Corporation)
#   D = OTS (Office of Thrift Supervision)
#   E = NCUA (National Credit Union Administration)
#   X = Unidentified
# Due to mixed numeric/letter values, agency_code is kept as String type


# ============================================================================
# 2007-2017 Data Constants
# ============================================================================

# Census tract summary statistics columns for 2007-2017 data
PERIOD_2007_2017_TRACT_COLUMNS = [
    "tract_population",
    "tract_minority_population_percent",
    "ffiec_msa_md_median_family_income",
    "tract_to_msa_income_percentage",
    "tract_owner_occupied_units",
    "tract_one_to_four_family_units",
    "tract_median_age_of_housing_units",
]

# Integer columns for 2007-2017 data
PERIOD_2007_2017_INTEGER_COLUMNS = [
    "activity_year",
    "agency_code",
    "loan_type",
    "property_type",
    "loan_purpose",
    "occupancy_type",
    "loan_amount",
    "preapproval",
    "action_taken",
    # "msa_md",
    "applicant_ethnicity",
    "co_applicant_ethnicity",
    "applicant_race_1",
    "applicant_race_2",
    "applicant_race_3",
    "applicant_race_4",
    "applicant_race_5",
    "co_applicant_race_1",
    "co_applicant_race_2",
    "co_applicant_race_3",
    "co_applicant_race_4",
    "co_applicant_race_5",
    "applicant_ethnicity_1",
    "applicant_ethnicity_2",
    "applicant_ethnicity_3",
    "applicant_ethnicity_4",
    "applicant_ethnicity_5",
    "co_applicant_ethnicity_1",
    "co_applicant_ethnicity_2",
    "co_applicant_ethnicity_3",
    "co_applicant_ethnicity_4",
    "co_applicant_ethnicity_5",
    "applicant_sex",
    "co_applicant_sex",
    "hoepa_status",
    "lien_status",
    "income",
    "purchaser_type",
    "denial_reason_1",
    "denial_reason_2",
    "denial_reason_3",
    "denial_reason_4",
    "edit_status",
    "sequence_number",
    "application_date_indicator",
    "tract_population",
    "ffiec_msa_md_median_family_income",
    "tract_owner_occupied_units",
    "tract_one_to_four_family_units",
    "tract_median_age_of_housing_units",
]

# Float columns for 2007-2017 data
PERIOD_2007_2017_FLOAT_COLUMNS = [
    "rate_spread",
    "tract_minority_population_percent",
    "tract_to_msa_income_percentage",
]


# ============================================================================
# Column Rename Dictionary (Legacy → Modern Names)
# ============================================================================

# Comprehensive dictionary mapping legacy HMDA column names to modern standardized names.
# Applied across all time periods during silver layer construction.
# Only columns that exist in the data will be renamed (safe application).

RENAME_DICTIONARY = {
    # Core field renames (2007-2017 period)
    "as_of_year": "activity_year",
    "applicant_income_000s": "income",
    "loan_amount_000s": "loan_amount",
    "census_tract_number": "census_tract",
    # Occupancy variants (pre-2007 and 2007-2017)
    "occupancy": "occupancy_type",  # Pre-2007 format
    "owner_occupancy": "occupancy_type",  # 2007-2017 format
    # MSA/MD standardization (pre-2007 and 2007-2017)
    "msamd": "msa_md",
    # Tract variable renames (2007-2017 period)
    "population": "tract_population",
    "minority_population": "tract_minority_population_percent",
    "hud_median_family_income": "ffiec_msa_md_median_family_income",
    "tract_to_msamd_income": "tract_to_msa_income_percentage",
    "number_of_owner_occupied_units": "tract_owner_occupied_units",
    "number_of_1_to_4_family_units": "tract_one_to_four_family_units",
    # Post-2018 corrections
    "loan_to_value_ratio": "combined_loan_to_value_ratio",
}


# ============================================================================
# Helper Functions
# ============================================================================


def get_medallion_dir(
    stage: Literal["bronze", "silver"],
    dataset: Literal["loans", "panel", "transmissal_series"],
    period: Literal["pre2007", "period_2007_2017", "post2018"] = "post2018",
) -> Path:
    """Return medallion directory for a given stage/dataset/period.

    Parameters
    ----------
    stage : {"bronze", "silver"}
        Medallion layer.
    dataset : {"loans", "panel", "transmissal_series"}
        Dataset family.
    period : {"pre2007", "period_2007_2017", "post2018"}
        Time period subfolder (defaults to post2018).

    Returns
    -------
    Path
        The target directory path.
    """
    base = BRONZE_DIR if stage == "bronze" else SILVER_DIR
    return base / dataset / period
