# -*- coding: utf-8 -*-
"""
Configuration management for HMDA Data Manager.

This module handles path configuration and environment variable setup
for the HMDA data management package.
"""

# Import Packages
from decouple import config
from pathlib import Path

# Specific Data Folders  
# Note: __file__.parent.parent.parent.parent goes from src/hmda_data_manager/core/ back to project root
PROJECT_DIR = Path(config("PROJECT_DIR", default=Path(__file__).parent.parent.parent.parent))
DATA_DIR = Path(config("DATA_DIR", default=PROJECT_DIR / "data"))
RAW_DIR = Path(config("HMDA_RAW_DIR", default=DATA_DIR / "raw"))
CLEAN_DIR = Path(config("HMDA_CLEAN_DIR", default=DATA_DIR / "clean"))
