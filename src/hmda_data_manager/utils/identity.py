"""
Identity/key and record utilities (Polars).
"""

from typing import Optional, Sequence
import polars as pl


def add_identity_keys(
    df: pl.DataFrame,
    post2018: bool,
    uli_col: str = "universal_loan_identifier",
    lei_col: str = "lei",
    respondent_id_col: str = "respondent_id",
    agency_col: str = "agency_code",
    seq_col: str = "sequence_number",
) -> pl.DataFrame:
    """Construct a stable HMDA record key (Polars)."""
    out = df.clone()
    if post2018:
        for column in (uli_col, lei_col):
            if column not in out.columns:
                out = out.with_columns(pl.lit(None).alias(column))
        out = out.with_columns(
            pl.concat_str(
                [
                    pl.col(lei_col).cast(pl.Utf8).str.strip(),
                    pl.lit("||"),
                    pl.col(uli_col).cast(pl.Utf8).str.strip(),
                ]
            ).alias("hmda_record_key")
        )
    else:
        for column in (respondent_id_col, agency_col, seq_col):
            if column not in out.columns:
                out = out.with_columns(pl.lit(None).alias(column))
        out = out.with_columns(
            pl.concat_str(
                [
                    pl.col(respondent_id_col).cast(pl.Utf8).str.strip(),
                    pl.lit("||"),
                    pl.col(agency_col).cast(pl.Utf8).str.strip(),
                    pl.lit("||"),
                    pl.col(seq_col).cast(pl.Utf8).str.strip(),
                ]
            ).alias("hmda_record_key")
        )
    return out


def deduplicate_records(
    df: pl.DataFrame,
    keep: str = "last",
    subset: Optional[Sequence[str]] = None,
) -> pl.DataFrame:
    """Drop duplicate rows by key or subset (Polars)."""
    if "hmda_record_key" not in df.columns:
        raise ValueError("Run add_identity_keys() before deduplication.")
    dedupe_subset = list(subset) if subset is not None else ["hmda_record_key"]
    before = df.height
    out = df.unique(subset=dedupe_subset, keep=keep, maintain_order=True)
    dropped = before - out.height
    return out.with_columns(pl.lit(dropped).alias("_metadata_dedup_dropped"))


__all__ = [
    "add_identity_keys",
    "deduplicate_records",
]


