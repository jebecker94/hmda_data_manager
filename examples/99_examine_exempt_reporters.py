# Import Packages
import logging

import polars as pl
from hmda_data_manager.core import DATA_DIR


logger = logging.getLogger(__name__)

# Load Dataset
# Note: Using direct parquet reading - replace path with your actual clean data location
# Load silver data instead of clean data
data_path = DATA_DIR / "bronze" / "loans" / "post2018" / "**/*.parquet"
df = pl.scan_parquet(data_path, extra_columns='ignore')

# Polars Filters
df = df.filter([pl.col("action_taken")=="1"])
df = df.filter(pl.col('activity_year')=="2024")

# See how many total originations there are
logger.info("Length: %s", df.select(pl.len()).collect())

# Columns missing for exempt
exempt_columns = [
    "reverse_mortgage",
    "open_end_line_of_credit",
    "business_or_commercial_purpose",
    "prepayment_penalty_term",
    "intro_rate_period",
    "negative_amortization",
    "interest_only_payment",
    "balloon_payment",
    "other_nonamortizing_features",
    "manufactured_home_secured_property_type",
    "manufactured_home_land_property_interest",
    "applicant_credit_score_type",
    "co_applicant_credit_score_type",
    "multifamily_affordable_units",
    "debt_to_income_ratio",
    "combined_loan_to_value_ratio",
    "interest_rate",
    "property_value",
    "rate_spread",
    "loan_term",
    "total_loan_costs",
    "total_points_and_fees",
    "origination_charges",
    "discount_points",
    "lender_credits",
    "aus_1",
    "denial_reason_1",
]

# Then filter and select the desired columns
df_exemptions = (
    df.with_columns(
        # Count Exemptions by summing boolean conditions cast to integers
        pl.sum_horizontal(
            [
                pl.col(column).cast(pl.Utf8).is_in(["1111", "Exempt"]).cast(pl.Int64)
                for column in exempt_columns if column in df.columns
            ]
        ).alias("CountExemptions")
    )
    .with_columns(
        # Calculate Average Exemptions per LEI using a window function
        pl.col("CountExemptions")
        .mean()
        .over(["lei", "activity_year"])
        .alias("AverageExemptions")
    )
    .filter(
        # Filter based on the calculated AverageExemptions
        (pl.col("AverageExemptions") < 26) & (pl.col("AverageExemptions") >= 1)
    )
    .select(
        # Select the final set of columns
        ["HMDAIndex", "lei", "activity_year"]
        + [column for column in exempt_columns if column in df.columns]
        + ["CountExemptions", "AverageExemptions"]
    )
)

# See How Many "Weird" Reporters there are
logger.info("Length: %s", df_exemptions.select(pl.len()).collect())

df_exemptions = df_exemptions.collect()
