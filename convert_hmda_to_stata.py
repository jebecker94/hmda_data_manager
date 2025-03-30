#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on: Sat Dec 3 09:49:44 2022
Last updated on: Thu Mar 14 08:06:38 2024
@author: Jonathan E. Becker
"""

# Import Packages
import ast
import config
import pandas as pd

# Save to Stata
def save_file_to_stata(file) :
    df = pd.read_parquet(file)
    df, variable_labels, value_labels = prepare_hmda_for_stata(df)
    save_file_dta = file.replace('.parquet','.dta')
    df.to_stata(save_file_dta,
                write_index=False,
                variable_labels=variable_labels,
                value_labels=value_labels,
                )

# Prepare for Stata
def prepare_hmda_for_stata(df) :
    """
    Create variable and value labels to save DTA files for stata.

    Parameters
    ----------
    df : pandas DataFrame
        DESCRIPTION.

    Returns
    -------
    df : pandas DataFrame
        DESCRIPTION.
    variable_labels : dictionary
        DESCRIPTION.
    value_labels : dictionary
        DESCRIPTION.

    """

    # Read Value Labels
    value_label_file = RAW_DIR / "loans/hmda_value_labels.txt"
    with open(value_label_file, 'r') as f :
        value_labels = ast.literal_eval(f.read())

    # Read Variable Labels
    variable_label_file = RAW_DIR / "loans/hmda_variable_labels.txt"
    with open(variable_label_file, 'r') as f :
        variable_labels = ast.literal_eval(f.read())

    # Trim Value and Variable Labels
    variable_labels = {key[0:32].replace('-','_'):value[0:80] for key,value in variable_labels.items() if key in df.columns}
    value_labels = {key[0:32].replace('-','_'):value for key,value in value_labels.items() if key in df.columns}
    df.columns = [x[0:32].replace('-','_') for x in df.columns]

    # Downcast Numeric Types
    vl = [key for key,value in value_labels.items()]
    for col in vl+['activity_year'] :
        try :
            df[col] = df[col].astype('Int16')
        except (TypeError, OverflowError) :
            print('Cannot downcast variable:', col)
    for col in ['msa_md', 'county_code', 'sequence_number'] :
        if col in df.columns :
            df[col] = df[col].astype('Int32')

    # Return DataFrame and Labels
    return df, variable_labels, value_labels

#%% Main Routine
if __name__ == '__main__' :

    # Define Folder Paths
    RAW_DIR = config.RAW_DIR
    CLEAN_DIR = config.CLEAN_DIR
    PROJECT_DIR = config.PROJECT_DIR
