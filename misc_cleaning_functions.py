# hmda_cleaning.py
import pandas as pd
import numpy as np
from typing import Optional, Dict, Sequence, Tuple

############################################################
# 0) Utilities
############################################################

def _to_nan(df: pd.DataFrame, cols: Sequence[str], na_codes: Sequence[str]=("NA", "N/A", "Exempt", "Not Applicable", "NA   ", "nan")) -> pd.DataFrame:
    out = df.copy()
    for c in cols:
        if c in out.columns:
            out[c] = out[c].replace(list(na_codes), np.nan)
    return out

def _coerce_numeric(df: pd.DataFrame, numeric_cols: Sequence[str]) -> pd.DataFrame:
    out = df.copy()
    for c in numeric_cols:
        if c in out.columns:
            out[c] = pd.to_numeric(out[c], errors="coerce")
    return out

############################################################
# 1) Identity keys & deduplication
############################################################

def add_identity_keys(
    df: pd.DataFrame,
    post2018: bool,
    uli_col: str = "universal_loan_identifier",
    lei_col: str = "lei",
    respondent_id_col: str = "respondent_id",
    agency_col: str = "agency_code",
    seq_col: str = "sequence_number"
) -> pd.DataFrame:
    """
    Build a stable record key depending on HMDA vintage.
    - Post-2018: use LEI + ULI (ULI contains a check digit).
    - Pre-2018: use respondent_id + agency_code + sequence_number.
    """
    out = df.copy()
    if post2018:
        for c in (uli_col, lei_col):
            if c not in out.columns:
                out[c] = np.nan
        out["hmda_record_key"] = out[lei_col].astype(str).str.strip() + "||" + out[uli_col].astype(str).str.strip()
    else:
        for c in (respondent_id_col, agency_col, seq_col):
            if c not in out.columns:
                out[c] = np.nan
        out["hmda_record_key"] = (
            out[respondent_id_col].astype(str).str.strip() + "||" +
            out[agency_col].astype(str).str.strip() + "||" +
            out[seq_col].astype(str).str.strip()
        )
    return out

def deduplicate_records(
    df: pd.DataFrame,
    keep: str = "last",
    subset: Optional[Sequence[str]] = None
) -> pd.DataFrame:
    """
    Drop exact duplicates using 'hmda_record_key'. If 'subset' is provided,
    use those columns for deduping (e.g., entire row).
    """
    out = df.copy()
    if "hmda_record_key" not in out.columns:
        raise ValueError("Run add_identity_keys() first.")
    if subset is None:
        subset = ["hmda_record_key"]
    before = len(out)
    out = out.drop_duplicates(subset=subset, keep=keep)
    out.attrs["dedup_dropped"] = before - len(out)
    return out

############################################################
# 2) Standardize columns across vintages
############################################################

def standardize_schema(
    df: pd.DataFrame,
    post2018: bool,
    rename_map_post2018: Optional[Dict[str, str]] = None,
    rename_map_pre2018: Optional[Dict[str, str]] = None
) -> pd.DataFrame:
    """
    Harmonize canonical columns (names/types). You can pass custom rename maps.
    """
    out = df.copy()
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
            # add more as needed
        }
        if rename_map_post2018:
            default_map.update(rename_map_post2018)
        out = out.rename(columns=default_map)
    else:
        default_map = {
            # map legacy names into the canonical set above
            "applicant_income_000s": "applicant_income",
            "action_taken_name": "action_taken",   # if the file uses labels instead of codes
            # add more as needed for your specific pre-2018 extracts
        }
        if rename_map_pre2018:
            default_map.update(rename_map_pre2018)
        out = out.rename(columns=default_map)

    # Coerce numeric on key numeric columns if present
    numeric_cols = [c for c in ["loan_amount","applicant_income","dti","ltv","credit_score","rate_spread"] if c in out.columns]
    out = _coerce_numeric(out, numeric_cols)
    return out

############################################################
# 3) Derived fields & “NA/Exempt” handling
############################################################

def normalize_missing_and_derived(
    df: pd.DataFrame,
    post2018: bool,
    na_like: Sequence[str] = ("NA","N/A","Exempt","Not Applicable")
) -> pd.DataFrame:
    """
    Convert NA/exempt strings to NaN; prefer CFPB derived fields (race/ethnicity/sex) if present.
    """
    out = df.copy()
    out = _to_nan(out, out.columns, na_like)
    # Prefer derived fields if present in public post-2018 HMDA
    for raw, derived in [
        ("race", "derived_race"),
        ("ethnicity", "derived_ethnicity"),
        ("sex", "derived_sex")
    ]:
        if post2018 and derived in out.columns:
            out[raw] = out[derived]
    return out

############################################################
# 4) Geography harmonization hook
############################################################

def harmonize_census_tract(
    df: pd.DataFrame,
    crosswalk: Optional[pd.DataFrame] = None,
    tract_col: str = "census_tract",
    year_col: str = "activity_year",
    to_vintage: int = 2020,
    crosswalk_cols: Tuple[str, str] = ("tract_src","tract_2020")
) -> pd.DataFrame:
    """
    If you provide a crosswalk (long format with src->target mapping by year),
    map HMDA tract codes to a single tract vintage (e.g., 2020).
    """
    out = df.copy()
    if crosswalk is None or tract_col not in out.columns or year_col not in out.columns:
        return out
    cw = crosswalk.copy()
    src, dst = crosswalk_cols
    # Example: merge and prefer the mapped code where available
    out = out.merge(cw[[src, "year", dst]], left_on=[tract_col, year_col], right_on=[src, "year"], how="left")
    out[tract_col] = out[dst].combine_first(out[tract_col])
    out.drop(columns=[src, dst, "year"], errors="ignore", inplace=True)
    return out

############################################################
# 5) Plausibility filters (configurable)
############################################################

def apply_plausibility_filters(
    df: pd.DataFrame,
    bounds: Dict[str, Tuple[Optional[float], Optional[float]]] = None
) -> pd.DataFrame:
    """
    Drop observations outside configured bounds; return filtered copy.
    """
    out = df.copy()
    default_bounds = {
        "ltv": (0, 200),
        "credit_score": (500, 820),
        "dti": (0, 250),
        "applicant_income": (0, 1_000_000),
        "loan_amount": (0, 1_500_000)
    }
    if bounds:
        default_bounds.update(bounds)
    mask = pd.Series(True, index=out.index)
    for col, (lo, hi) in default_bounds.items():
        if col in out.columns:
            if lo is not None:
                mask &= (out[col].isna()) | (out[col] >= lo)
            if hi is not None:
                mask &= (out[col].isna()) | (out[col] <= hi)
    filtered = out[mask].copy()
    filtered.attrs["plausibility_dropped"] = len(out) - len(filtered)
    return filtered

############################################################
# 6) Rate-spread sanity check (post-2018 aware)
############################################################

def clean_rate_spread(
    df: pd.DataFrame,
    post2018: bool,
    rate_spread_col: str = "rate_spread"
) -> pd.DataFrame:
    """
    Basic guardrails: coerce numerics, clamp wild outliers; note that definitions changed around 2018.
    """
    out = df.copy()
    if rate_spread_col in out.columns:
        out[rate_spread_col] = pd.to_numeric(out[rate_spread_col], errors="coerce")
        # very loose clamp; adapt for your analysis
        out.loc[out[rate_spread_col].abs() > 20, rate_spread_col] = np.nan
    return out

############################################################
# 7) QC: Outlier flagger (simple)
############################################################

def flag_outliers_basic(
    df: pd.DataFrame,
    cols: Sequence[str] = ("interest_rate","rate_spread","total_loan_costs"),
    z: float = 6.0
) -> pd.DataFrame:
    """
    Add boolean flags for extreme z-scores in a few pricing fields (if present).
    Inspired by FDIC 'outlier' screens.
    """
    out = df.copy()
    for c in cols:
        if c in out.columns:
            x = pd.to_numeric(out[c], errors="coerce")
            mu, sd = x.mean(skipna=True), x.std(skipna=True)
            if pd.notnull(sd) and sd > 0:
                out[f"outlier_{c}"] = ((x - mu).abs() > z*sd)
    return out

############################################################
# 8) Top-level cleaner pipeline (optional convenience)
############################################################

def clean_hmda(
    df: pd.DataFrame,
    post2018: bool,
    bounds: Optional[Dict[str, Tuple[Optional[float], Optional[float]]]] = None,
    crosswalk: Optional[pd.DataFrame] = None
) -> pd.DataFrame:
    """
    One-call cleaning: identity, dedup, schema, missing/derived, geography, plausibility, rate-spread, QC.
    You can still call individual steps piecemeal.
    """
    out = add_identity_keys(df, post2018=post2018)
    out = deduplicate_records(out, keep="last")
    out = standardize_schema(out, post2018=post2018)
    out = normalize_missing_and_derived(out, post2018=post2018)
    out = harmonize_census_tract(out, crosswalk=crosswalk)
    out = apply_plausibility_filters(out, bounds=bounds)
    out = clean_rate_spread(out, post2018=post2018)
    out = flag_outliers_basic(out)
    return out