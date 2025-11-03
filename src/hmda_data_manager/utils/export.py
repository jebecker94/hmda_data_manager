"""
Export utilities (Stata)
========================

Helpers to export HMDA datasets to Stata with variable and value labels.
"""

import ast
import logging
from pathlib import Path

import pandas as pd


logger = logging.getLogger(__name__)


def prepare_hmda_for_stata(
    df: pd.DataFrame,
    labels_folder: Path | None = None,
    value_label_file: Path | None = None,
    variable_label_file: Path | None = None,
) -> tuple[pd.DataFrame, dict[str, str], dict[str, dict[int, str]]]:
    """Create variable and value labels and tidy columns for Stata export.

    Returns the transformed DataFrame, a variable labels dict, and a value labels
    dict suitable for ``DataFrame.to_stata(..., variable_labels=..., value_labels=...)``.
    """
    if labels_folder is None:
        labels_folder = Path(__file__).parent / "labels"
    labels_folder.mkdir(parents=True, exist_ok=True)

    if value_label_file is None:
        value_label_file = labels_folder / "hmda_value_labels.txt"
    if variable_label_file is None:
        variable_label_file = labels_folder / "hmda_variable_labels.txt"

    with open(value_label_file, "r") as f:
        value_labels = ast.literal_eval(f.read())
    with open(variable_label_file, "r") as f:
        variable_labels = ast.literal_eval(f.read())

    # Trim names/labels to Stata limits and align to existing columns
    variable_labels = {
        key[0:32].replace("-", "_"): value[0:80]
        for key, value in variable_labels.items()
        if key in df.columns
    }
    value_labels = {
        key[0:32].replace("-", "_"): value
        for key, value in value_labels.items()
        if key in df.columns
    }
    df = df.copy()
    df.columns = [x[0:32].replace("-", "_") for x in df.columns]

    # Downcast common integer-like columns to compact Stata types
    vl = [key for key, _ in value_labels.items()]
    for col in vl + ["activity_year"]:
        try:
            if col in df.columns:
                df[col] = df[col].astype("Int16")
        except (TypeError, OverflowError):
            logger.warning("Cannot downcast variable: %s", col)
    for col in ["msa_md", "county_code", "sequence_number"]:
        if col in df.columns:
            df[col] = df[col].astype("Int32")

    return df, variable_labels, value_labels


def save_file_to_stata(file: Path) -> None:
    """Convert a Parquet file to Stata ``.dta`` with labels.

    Saves alongside the source file path with the ``.dta`` suffix.
    """
    df = pd.read_parquet(file)
    df, variable_labels, value_labels = prepare_hmda_for_stata(df)
    save_file_dta = file.with_suffix(".dta")
    df.to_stata(
        save_file_dta,
        write_index=False,
        variable_labels=variable_labels,
        value_labels=value_labels,
    )


__all__ = [
    "prepare_hmda_for_stata",
    "save_file_to_stata",
]


