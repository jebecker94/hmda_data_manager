"""
Cleaning utilities (Polars): NA handling, schema, plausibility, outliers.
"""

from typing import Optional, Sequence, Tuple
import polars as pl


def replace_na_like_values(
    df: pl.DataFrame,
    columns: Sequence[str],
    na_like: Sequence[str] = ("NA", "N/A", "Exempt", "Not Applicable", "NA   ", "nan"),
) -> pl.DataFrame:
    columns_to_update = [column for column in columns if column in df.columns]
    if not columns_to_update:
        return df.clone()
    replacements = list(na_like)
    return df.with_columns(
        [
            pl.col(column).replace(replacements, [None] * len(replacements)).alias(column)
            for column in columns_to_update
        ]
    )


def coerce_numeric_columns(
    df: pl.DataFrame,
    numeric_columns: Sequence[str],
) -> pl.DataFrame:
    columns_to_update = [column for column in numeric_columns if column in df.columns]
    if not columns_to_update:
        return df.clone()
    return df.with_columns(
        [pl.col(column).cast(pl.Float64, strict=False).alias(column) for column in columns_to_update]
    )


def standardize_schema(
    df: pl.DataFrame,
    post2018: bool,
    rename_map_post2018: Optional[dict[str, str]] = None,
    rename_map_pre2018: Optional[dict[str, str]] = None,
) -> pl.DataFrame:
    out = df.clone()
    if post2018:
        default_map = {
            "loan_amount": "loan_amount",
            "applicant_income": "applicant_income",
            "debt_to_income_ratio": "dti",
            "loan_to_value_ratio": "ltv",
            "credit_score": "credit_score",
            "action_taken": "action_taken",
            "lien_status": "lien_status",
            "loan_purpose": "loan_purpose",
            "loan_type": "loan_type",
            "occupancy_type": "occupancy_type",
            "rate_spread": "rate_spread",
            "hoepa_status": "hoepa_status",
            "purchaser_type": "purchaser_type",
        }
        if rename_map_post2018:
            default_map.update(rename_map_post2018)
        out = out.rename(default_map)
    else:
        default_map = {
            "applicant_income_000s": "applicant_income",
            "action_taken_name": "action_taken",
        }
        if rename_map_pre2018:
            default_map.update(rename_map_pre2018)
        out = out.rename(default_map)

    numeric_columns = [
        column
        for column in (
            "loan_amount",
            "applicant_income",
            "dti",
            "ltv",
            "credit_score",
            "rate_spread",
        )
        if column in out.columns
    ]
    return coerce_numeric_columns(out, numeric_columns)


def normalize_missing_and_derived(
    df: pl.DataFrame,
    post2018: bool,
    na_like: Sequence[str] = ("NA", "N/A", "Exempt", "Not Applicable"),
) -> pl.DataFrame:
    out = replace_na_like_values(df, df.columns, na_like)
    if post2018:
        for raw_column, derived_column in (
            ("race", "derived_race"),
            ("ethnicity", "derived_ethnicity"),
            ("sex", "derived_sex"),
        ):
            if derived_column in out.columns:
                out = out.with_columns(pl.col(derived_column).alias(raw_column))
    return out


def harmonize_census_tract(
    df: pl.DataFrame,
    crosswalk: Optional[pl.DataFrame] = None,
    tract_col: str = "census_tract",
    year_col: str = "activity_year",
    to_vintage: Optional[int] = 2020,
    crosswalk_cols: Tuple[str, str] = ("tract_src", "tract_2020"),
) -> pl.DataFrame:
    if crosswalk is None or tract_col not in df.columns or year_col not in df.columns:
        return df.clone()
    if not isinstance(crosswalk, pl.DataFrame):
        raise TypeError("crosswalk must be a Polars DataFrame")
    cw = crosswalk.clone()
    src_col, dst_col = crosswalk_cols
    if src_col not in cw.columns or dst_col not in cw.columns:
        return df.clone()
    if "target_year" in cw.columns and to_vintage is not None:
        cw = cw.filter(pl.col("target_year") == to_vintage)
    right_columns = [src_col, dst_col]
    join_left = [tract_col]
    join_right = [src_col]
    if "year" in cw.columns:
        cw = cw.with_columns(pl.col("year"))
        right_columns.append("year")
        join_left.append(year_col)
        join_right.append("year")
    if "target_year" in cw.columns:
        right_columns.append("target_year")
    cw_subset = cw.select(right_columns)
    merged = df.join(cw_subset, left_on=join_left, right_on=join_right, how="left")
    drop_columns = [src_col, dst_col]
    if "year" in right_columns:
        drop_columns.append("year")
    if "target_year" in right_columns:
        drop_columns.append("target_year")
    merged = merged.with_columns(
        pl.coalesce([pl.col(dst_col), pl.col(tract_col)]).alias(tract_col)
    )
    drop_columns = [column for column in drop_columns if column in merged.columns]
    if drop_columns:
        merged = merged.drop(drop_columns)
    return merged


def apply_plausibility_filters(
    df: pl.DataFrame,
    bounds: Optional[dict[str, Tuple[Optional[float], Optional[float]]]] = None,
) -> pl.DataFrame:
    default_bounds: dict[str, Tuple[Optional[float], Optional[float]]] = {
        "ltv": (0, 200),
        "credit_score": (500, 820),
        "dti": (0, 250),
        "applicant_income": (0, 1_000_000),
        "loan_amount": (0, 1_500_000),
    }
    if bounds:
        default_bounds.update(bounds)
    mask_expr = pl.lit(True)
    for column, (lower, upper) in default_bounds.items():
        if column not in df.columns:
            continue
        values = pl.col(column).cast(pl.Float64, strict=False)
        column_mask = values.is_null()
        if lower is not None:
            column_mask = column_mask | (values >= lower)
        if upper is not None:
            column_mask = column_mask | (values <= upper)
        mask_expr = mask_expr & column_mask
    filtered = df.filter(mask_expr)
    dropped = df.height - filtered.height
    return filtered.with_columns(pl.lit(dropped).alias("_metadata_plausibility_dropped"))


def clean_rate_spread(
    df: pl.DataFrame,
    post2018: bool,
    rate_spread_col: str = "rate_spread",
) -> pl.DataFrame:
    if rate_spread_col not in df.columns:
        return df.clone()
    max_abs = 20 if post2018 else 20
    cast_col = pl.col(rate_spread_col).cast(pl.Float64, strict=False)
    cleaned = pl.when(cast_col.abs() > max_abs).then(None).otherwise(cast_col)
    return df.with_columns(cleaned.alias(rate_spread_col))


def flag_outliers_basic(
    df: pl.DataFrame,
    columns: Sequence[str] = ("interest_rate", "rate_spread", "total_loan_costs"),
    z_threshold: float = 6.0,
) -> pl.DataFrame:
    out = df.clone()
    for column in columns:
        if column not in out.columns:
            continue
        stats = out.select(
            pl.col(column).cast(pl.Float64, strict=False).mean().alias("mean"),
            pl.col(column).cast(pl.Float64, strict=False).std().alias("std"),
        )
        mean_val = stats["mean"][0]
        std_val = stats["std"][0]
        if std_val is not None and std_val > 0:
            out = out.with_columns(
                (
                    (pl.col(column).cast(pl.Float64, strict=False) - mean_val).abs()
                    > z_threshold * std_val
                ).alias(f"outlier_{column}")
            )
    return out


def clean_hmda(
    df: pl.DataFrame,
    post2018: bool,
    bounds: Optional[dict[str, Tuple[Optional[float], Optional[float]]]] = None,
    crosswalk: Optional[pl.DataFrame] = None,
) -> pl.DataFrame:
    out = add_identity_keys(df, post2018=post2018)
    out = deduplicate_records(out, keep="last")
    out = standardize_schema(out, post2018=post2018)
    out = normalize_missing_and_derived(out, post2018=post2018)
    out = harmonize_census_tract(out, crosswalk=crosswalk)
    out = apply_plausibility_filters(out, bounds=bounds)
    out = clean_rate_spread(out, post2018=post2018)
    out = flag_outliers_basic(out)
    return out


# Prepare for Stata
def downcast_hmda_variables(df):
    """
    Downcast HMDA variables

    Parameters
    ----------
    df : pandas DataFrame
        DataFrame whose numeric columns should be downcast to smaller dtypes.

    Returns
    -------
    df : pandas DataFrame
        DataFrame with downcasted numeric columns.

    """

    # Downcast Numeric Types
    # for col in df.columns :
    #     try :
    #         df[col] = df[col].astype('Int16')
    #     except (TypeError, OverflowError) :
    #         print('Cannot downcast variable:', col)
    for col in ["msa_md", "county_code", "sequence_number"]:
        if col in df.columns:
            df[col] = df[col].astype("Int32")

    # Return DataFrame and Labels
    return df


# late imports to avoid cycles
from .identity import add_identity_keys, deduplicate_records  # noqa: E402


__all__ = [
    "replace_na_like_values",
    "coerce_numeric_columns",
    "standardize_schema",
    "normalize_missing_and_derived",
    "harmonize_census_tract",
    "apply_plausibility_filters",
    "clean_rate_spread",
    "flag_outliers_basic",
    "clean_hmda",
]


