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
