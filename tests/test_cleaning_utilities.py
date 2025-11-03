"""Tests for migrated HMDA cleaning utilities."""

import pandas as pd

from hmda_data_manager import (
    add_identity_keys,
    apply_plausibility_filters,
    clean_hmda,
    clean_rate_spread,
    coerce_numeric_columns,
    deduplicate_records,
    flag_outliers_basic,
    harmonize_census_tract,
    normalize_missing_and_derived,
    replace_na_like_values,
    standardize_schema,
)


def test_replace_na_like_values_replaces_tokens():
    df = pd.DataFrame({"col": ["NA", "value"]})
    result = replace_na_like_values(df, ["col"])
    assert pd.isna(result.loc[0, "col"])
    assert result.loc[1, "col"] == "value"


def test_add_identity_keys_post2018_builds_key():
    df = pd.DataFrame(
        {
            "lei": ["L1  "],
            "universal_loan_identifier": ["  ID1"],
        }
    )
    result = add_identity_keys(df, post2018=True)
    assert result.loc[0, "hmda_record_key"] == "L1||ID1"


def test_deduplicate_records_tracks_attr():
    df = pd.DataFrame(
        {
            "hmda_record_key": ["a", "a", "b"],
            "value": [1, 2, 3],
        }
    )
    result = deduplicate_records(df, keep="last")
    assert len(result) == 2
    assert result.attrs["dedup_dropped"] == 1


def test_standardize_schema_coerces_numeric():
    df = pd.DataFrame(
        {
            "loan_amount": ["100"],
            "applicant_income": ["50"],
            "rate_spread": ["1.5"],
        }
    )
    result = standardize_schema(df, post2018=True)
    assert result["loan_amount"].dtype.kind in {"i", "u", "f"}
    assert result["rate_spread"].dtype.kind in {"i", "u", "f"}


def test_normalize_missing_and_derived_prefers_derived_columns():
    df = pd.DataFrame(
        {
            "race": ["NA"],
            "derived_race": ["White"],
        }
    )
    result = normalize_missing_and_derived(df, post2018=True)
    assert result.loc[0, "race"] == "White"


def test_harmonize_census_tract_uses_crosswalk():
    df = pd.DataFrame(
        {
            "census_tract": ["000100"],
            "activity_year": [2017],
        }
    )
    crosswalk = pd.DataFrame(
        {
            "tract_src": ["000100"],
            "tract_2020": ["999900"],
            "year": [2017],
        }
    )
    result = harmonize_census_tract(df, crosswalk=crosswalk)
    assert result.loc[0, "census_tract"] == "999900"


def test_apply_plausibility_filters_drops_out_of_range():
    df = pd.DataFrame(
        {
            "hmda_record_key": ["x", "y"],
            "ltv": [50, 500],
        }
    )
    result = apply_plausibility_filters(df)
    assert len(result) == 1
    assert result.attrs["plausibility_dropped"] == 1


def test_clean_rate_spread_clamps_outliers():
    df = pd.DataFrame({"rate_spread": ["25", "3"]})
    result = clean_rate_spread(df, post2018=True)
    assert pd.isna(result.loc[0, "rate_spread"])
    assert result.loc[1, "rate_spread"] == 3


def test_flag_outliers_basic_creates_flag():
    df = pd.DataFrame({"interest_rate": [2, 2, 2, 100]})
    result = flag_outliers_basic(df)
    assert result["outlier_interest_rate"].sum() == 1


def test_clean_hmda_pipeline_post2018():
    df = pd.DataFrame(
        {
            "lei": ["L1", "L1"],
            "universal_loan_identifier": ["ID1", "ID1"],
            "loan_amount": ["100", "100"],
            "applicant_income": ["50", "50"],
            "rate_spread": ["1.5", "1.5"],
            "race": ["NA", "NA"],
            "derived_race": ["White", "White"],
            "census_tract": ["000100", "000100"],
            "activity_year": [2019, 2019],
        }
    )
    crosswalk = pd.DataFrame(
        {
            "tract_src": ["000100"],
            "tract_2020": ["999900"],
            "year": [2019],
        }
    )
    result = clean_hmda(df, post2018=True, crosswalk=crosswalk)
    assert len(result) == 1
    assert result.loc[0, "census_tract"] == "999900"
    assert "outlier_rate_spread" in result.columns
    assert result["rate_spread"].dtype.kind in {"i", "u", "f"}


def test_coerce_numeric_columns_handles_missing_columns():
    df = pd.DataFrame({"a": ["1"], "b": ["two"]})
    result = coerce_numeric_columns(df, ["a", "missing"])
    assert result.loc[0, "a"] == 1
    assert "missing" not in result.columns
