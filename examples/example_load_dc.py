# -*- coding: utf-8 -*-
"""
Created on Tue Mar 18 07:00:00 2025
@author: Jonathan E. Becker
"""

# Import Packages
import os
from pathlib import Path
import config
import HMDALoader

os.chdir(Path(__file__).resolve().parent.parent)

# Main Routine
if __name__ == "__main__":
    # Load Combined DC Data
    df = HMDALoader.load_hmda_file(
        config.CLEAN_DIR,
        filters=[("state_code", "==", "DC"), ("action_taken", "in", [1, 6])],
    )
