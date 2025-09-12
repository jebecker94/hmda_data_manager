# -*- coding: utf-8 -*-
"""
Created on Sun Dec 29 08:46:50 2024
@author: Jebecker3
"""

# Import Packages
from decouple import config
from pathlib import Path

# Specific Data Folders
PROJECT_DIR = Path(config("PROJECT_DIR", default=Path.cwd()))
DATA_DIR = Path(config("DATA_DIR", default=PROJECT_DIR / "data"))
RAW_DIR = Path(config("HMDA_RAW_DIR", default=DATA_DIR / "raw"))
CLEAN_DIR = Path(config("HMDA_CLEAN_DIR", default=DATA_DIR / "clean"))
