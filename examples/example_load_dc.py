# -*- coding: utf-8 -*-
"""
Created on Tue Mar 18 07:00:00 2025
@author: Jonathan E. Becker
"""

# Import Packages
import os
os.chdir(os.path.dirname(__file__) + '/..')
import config
import HMDALoader

# Main Routine
if __name__=='__main__':

    # Load Combined DC Data
    df = HMDALoader.load_hmda_file(
            config.CLEAN_DIR,
            filters=[('state_code','==','DC'),('action_taken','in',[1,6])],
    )
