#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on: Saturday December 3, 2022
Last updated on: Sunday March 30, 2025
@author: Jonathan E. Becker
"""

# Import Packages
import os
import glob
import pandas as pd
import numpy as np
from pyarrow import csv
import pyarrow as pa
import pyarrow.parquet as pq
import config
import time
import polars as pl
import shutil
from import_support_functions import *
import HMDALoader

#%% Import Functions
# Import Historic HMDA Files (Still Needs Work)
def import_hmda_pre_2007(data_folder, save_folder, min_year=1981, max_year=2006, contains_string='HMDA_LAR', save_to_parquet=True) :
    """
    Import and clean HMDA data before 2007.

    Parameters
    ----------
    data_folder : str
        DESCRIPTION.
    save_folder : str
        DESCRIPTION.
    contains_string : str, optional
        DESCRIPTION. The default is 'HMDA_LAR'.

    Returns
    -------
    None.

    """

    # Loop Over Years
    for year in range(min_year, max_year+1) :

        # Get Files
        files = glob.glob(f"{data_folder}/*{year}*.txt")
        for file in files :

            # Get File Name
            file_name = os.path.basename(file).split('.')[0]
            save_file_csv = f'{save_folder}/{file_name}.csv.gz'
            save_file_parquet = f'{save_folder}/{file_name}.parquet'

            # Read File
            if not os.path.exists(save_file_parquet) :

                # Load Raw Data
                print('Reading file:', file)
                parse_options = csv.ParseOptions(delimiter=get_delimiter(file, bytes=16000))
                df = csv.read_csv(file, parse_options=parse_options).to_pandas()

                # Rename Columns
                df = rename_hmda_columns(df)

                # Destring Numeric Columns
                df = destring_hmda_cols_pre2007(df)

                # Convert to PyArrow Table
                dt = pa.Table.from_pandas(df, preserve_index = False)

                # Save to CSV
                write_options = csv.WriteOptions(delimiter = '|')    
                with pa.CompressedOutputStream(save_file_csv, "gzip") as out :
                    csv.write_csv(dt, out, write_options=write_options)
                if save_to_parquet :   
                    pq.write_table(dt, save_file_parquet)

# Import Data w/ Streaming
def import_hmda_streaming(data_folder, save_folder, schema_file, min_year=2007, max_year=2023, replace=False, remove_raw_file=True, add_hmda_index=True) :
    """
    Import and clean HMDA data for 2007 onward.
    
    Also adds HMDAIndex to HMDA files from 2018 onward. The HMDAIndex is a unique identifier
    for each loan application record consisting of:
    - Activity year (4 digits)  
    - File type code (1 character)
    - Row number (9 digits, zero padded)
    For example: 2018a_000000000

    Parameters
    ----------
    data_folder : str
        Folder where raw data is stored.
    save_folder : str
        Folder where cleaned data will be saved.
    min_year : int, optional
        First year of data to include. The default is 2007.
    max_year : int, optional
        Last year of data to include. The default is 2023.
    replace : bool, optional
        Whether to replace existing files. The default is False.
    remove_raw_file : bool, optional
        Whether to remove the raw file after processing. The default is True.
    add_hmda_index : bool, optional
        Whether to add the HMDAIndex to the data starting in 2017. The default is True.

    Returns
    -------
    None.

    """

    # Loop Over Years
    for year in range(min_year, max_year+1) :

        # Get File Name
        files = glob.glob(f"{data_folder}/*{year}*.zip")
        for file in files :

            # Save File Names
            file_name = os.path.basename(file).split('.')[0]
            if file_name.endswith('_csv') :
                file_name = file_name[:-4]
            if file_name.endswith('_pipe') :
                file_name = file_name[:-5]
            save_file = f'{save_folder}/{file_name}.parquet'

            # Clean if Files are Missing
            if (not os.path.exists(save_file)) or replace :

                # Detect Delimiter and Read File
                print('Reading file:', file)

                # Setup Read and Write Options
                raw_file = unzip_hmda_file(file, data_folder)
                delimiter = get_delimiter(raw_file, bytes=16000)
                schema = get_file_schema(schema_file=schema_file, schema_type='polars')

                # Limit Schema to Cols in CSV
                csv_columns = pd.read_csv(raw_file, nrows=0, sep=delimiter).columns
                print(csv_columns)
                # if year <= 2017 :
                    # # Check size compatibility
                    # schema = pa.schema([(name, dtype) for name, dtype in zip(csv_columns, schema.types)])
                if year >= 2018 :
                    # Add functionality to limit to only csv columns
                    pass

                # Read HMDA Data (Adding HMDA Index After 2017)
                if (year < 2017) | (add_hmda_index==False) :
                    lf = pl.scan_csv(raw_file, separator=delimiter, low_memory=True, schema=schema)
                else :
                    # lf = pl.scan_csv(raw_file, separator=delimiter, low_memory=True, schema=schema, row_index_name='HMDAIndex', infer_schema_length=None)
                    lf = pl.scan_csv(raw_file, separator=delimiter, low_memory=True, row_index_name='HMDAIndex', infer_schema_length=None)
                    file_type = HMDALoader.get_file_type_code(file)
                    lf = lf.cast({'HMDAIndex': pl.String}, strict=False)
                    lf = lf.with_columns(pl.col('HMDAIndex').str.zfill(9).alias('HMDAIndex'))
                    lf = lf.with_columns((str(year)+file_type+'_'+pl.col('HMDAIndex')).alias('HMDAIndex'))

                # Save as Parquet (Streaming)
                lf.sink_parquet(save_file)

                # Remove the Temporary Raw File
                if remove_raw_file :
                    time.sleep(1)
                    os.remove(raw_file)

#%% Cleaning Functions
# Clean Data After 2017
def clean_hmda_post_2017(data_folder, min_year=2018, max_year=2023, replace=False) :
    """
    Import and clean HMDA data for 2018 onward.

    Parameters
    ----------
    data_folder : str
        Folder where parquet files are stored.
    min_year : int, optional
        First year of data to include. The default is 2018.
    max_year : int, optional
        Last year of data to include. The default is 2022.
    replace : bool, optional
        Whether to replace existing files. The default is False.

    Returns
    -------
    None.

    """

    # Loop Over Years
    for year in range(min_year, max_year+1) :

        # Get File Name
        files = glob.glob(f"{data_folder}/*{year}*.parquet")
        for file in files :

            # Save File Names
            save_file_parquet = file.replace('.parquet','_clean.parquet')

            # Clean if Files are Missing
            if (not os.path.exists(save_file_parquet)) or replace:

                lf = pl.scan_parquet(file, low_memory=True)

                # Drop Derived Columns b/c of redundancies
                derived_columns = [
                    'derived_loan_product_type',
                    'derived_race',
                    'derived_ethnicity',
                    'derived_sex',
                    'derived_dwelling_category',
                ]
                lf = lf.drop(derived_columns, strict=False)

                # Drop Tract Columns
                tract_columns = [
                    'tract_population',
                    'tract_minority_population_percent',
                    'ffiec_msa_md_median_family_income',
                    'tract_to_msa_income_percentage',
                    'tract_owner_occupied_units',
                    'tract_one_to_four_family_homes',
                    'tract_median_age_of_housing_units',
                ]
                lf = lf.drop(tract_columns, strict=False)

                # Rename HMDA Columns
                # lf = rename_hmda_columns(lf, df_type='polars')

                # Destring HMDA Columns
                lf = destring_hmda_cols_after_2018(lf)

                # Census Tract to String and Fix NAs
                lf = lf.cast({'census_tract': pl.Float64}, strict=False)
                lf = lf.cast({'census_tract': pl.Int64}, strict=False)
                lf = lf.cast({'census_tract': pl.String}, strict=False)
                lf = lf.with_columns(pl.col('census_tract').str.zfill(11).alias('census_tract'))

                # Prepare for Stata
                # df = downcast_hmda_variables(df)

                # Save to Parquet
                # dt = pa.Table.from_pandas(df, preserve_index=False)
                # pq.write_table(dt, save_file_parquet)
                lf.sink_parquet(save_file_parquet)

                # Replace Original File
                shutil.move(save_file_parquet, file)

# Clean Historic HMDA Files (2007-2017)
def clean_hmda_2007_2017(data_folder, min_year=2007, max_year=2017, replace=False) :
    """
    Import and clean HMDA data for 2007-2017.

    Parameters
    ----------
    data_folder : TYPE
        DESCRIPTION.
    save_folder : TYPE
        DESCRIPTION.
    min_year : TYPE, optional
        DESCRIPTION. The default is 2007.
    max_year : TYPE, optional
        DESCRIPTION. The default is 2017.

    Returns
    -------
    None.

    """

    # Loop Over Years
    for year in range(min_year, max_year+1) :

        # Get File Name
        files = glob.glob(f"{data_folder}/*{year}*records*.parquet") + glob.glob(f"{data_folder}/*{year}*public*.parquet")
        for file in files :

            # Save File Names
            save_file_parquet = file.replace('.parquet','_clean.parquet')

            # Clean if Files are Missing
            if (not os.path.exists(save_file_parquet)) or replace:

                df = csv.read_parquet(file)

                # Rename HMDA Columns
                df = rename_hmda_columns(df)

                # Create Unique HMDA Index
                if year == 2017 :
                    file_type = HMDALoader.get_file_type_code(file)
                    df['HMDAIndex'] = range(df.shape[0])
                    df['HMDAIndex'] = df['HMDAIndex'].astype('str').str.zfill(9)
                    df['HMDAIndex'] = df['activity_year'].astype('str') + file_type + '_' + df['HMDAIndex']

                # Drop Derived Columns b/c of redundancies
                derived_columns = ['derived_loan_product_type','derived_race','derived_ethnicity','derived_sex','derived_dwelling_category']
                df = df.drop(columns = derived_columns, errors='ignore')

                # Drop Tract Variables

                # Destring HMDA Columns
                df = destring_hmda_cols_2007_2017(df)
                
                # Census Tract to String and Fix NAs
                df['census_tract'] = pd.to_numeric(df['census_tract'], errors='coerce')
                df['census_tract'] = df['census_tract'].astype('Int64')
                df['census_tract'] = df['census_tract'].astype('str')
                df['census_tract'] = df['census_tract'].str.zfill(11)
                df.loc[df['census_tract'].str.contains('NA', regex=False), 'census_tract'] = ""

                # Save to Parquet
                dt = pa.Table.from_pandas(df, preserve_index=False)
                pq.write_table(dt, save_file_parquet)

#%% Combine Files
# Combine Lenders After 2018
def combine_lenders_panel_ts_post2018(panel_folder, ts_folder, save_folder, min_year=2018, max_year=2023) :
    """
    Combine Transmissal Series and Panel data for lenders between 2018 and 2022.

    Parameters
    ----------
    panel_folder : str
        Folder where raw panel data is stored.
    ts_folder : str
        Folder where raw transmissal series data is stored.
    save_folder : str
        Folder where combined data will be saved.
    min_year : int, optional
        First year of data to include. The default is 2018.
    max_year : int, optional
        Last year of data to include. The default is 2023.

    Returns
    -------
    None.

    """

    # Import Panel Data
    df_panel = []
    for year in range(min_year, max_year+1) :
        file = glob.glob(f'{panel_folder}/*{year}*.parquet')[0]
        df_a = pd.read_parquet(file)
        df_panel.append(df_a)
        del df_a
    df_panel = pd.concat(df_panel)

    # Import Transmissal Series Data
    df_ts = []
    for year in range(min_year, max_year+1) :
        file = glob.glob(f'{ts_folder}/*{year}*.parquet')[0]
        df_a = pd.read_parquet(file)
        df_ts.append(df_a)
        del df_a
    df_ts = pd.concat(df_ts)

    # Combined TS and Panel
    df = df_panel.merge(df_ts,
                        on = ['activity_year','lei'],
                        how = 'outer',
                        suffixes = ('_panel','_ts')
                        )
    df = df[df.columns.sort_values()]

    # Save
    df.to_csv(f'{save_folder}/hmda_lenders_combined_{min_year}-{max_year}.csv', index = False, sep = '|')
    df.to_parquet(f'{save_folder}/hmda_lenders_combined_{min_year}-{max_year}.parquet', index = False)

    ## Deprecated Code
    # ts_folder = '/project/cl/external_data/HMDA/raw_files/transmissal_series'
    # ts_files = glob.glob(f'{ts_folder}/*.txt')
    # df_ts = []
    # for year in range(2018, 2022+1) :
    #     file = [x for x in ts_files if str(year) in x][0]
    #     df_a = pd.read_csv(file, sep = '|', quoting = 1)
    #     df_ts.append(df_a)
    # df_ts = pd.concat(df_ts)

    # # Import Panel and TS Data
    # panel_folder = '/project/cl/external_data/HMDA/raw_files/panel'
    # panel_files = glob.glob(f'{panel_folder}/*.txt')
    # df_panel = []
    # for year in range(2018, 2022+1) :
    #     file = [x for x in panel_files if str(year) in x][0]
    #     df_a = pd.read_csv(file, sep = '|', quoting = 1)
    #     df_a = df_a.rename(columns = {'upper':'lei'})
    #     df_panel.append(df_a)
    # df_panel = pd.concat(df_panel)

# Combine Lenders Before 2018
def combine_lenders_panel_ts_pre2018(panel_folder, ts_folder, save_folder, min_year=2007, max_year=2017) :
    """
    Combine Transmissal Series and Panel data for lenders between 2007 and 2017.

    Parameters
    ----------
    panel_folder : str
        Folder where raw panel data is stored.
    ts_folder : str
        Folder where raw transmissal series data is stored.
    save_folder : str
        Folder where combined data will be saved.
    min_year : int, optional
        First year of data to include. The default is 2007.
    max_year : int, optional
        Last year of data to include. The default is 2017.

    Returns
    -------
    None.

    """

    # Import TS Data
    df_ts = []
    for year in range(2007, 2017+1) :
        file = glob.glob(f'{ts_folder}/*{year}*.csv')[0]
        df_a = pd.read_csv(file, low_memory = False)
        df_a.columns = [x.strip() for x in df_a.columns]
        df_a = df_a.drop(columns = ['Respondent Name (Panel)','Respondent City (Panel)','Respondent State (Panel)'], errors = 'ignore')
        df_ts.append(df_a)
        del df_a
    df_ts = pd.concat(df_ts)

    # Import Panel Data
    df_panel = []
    for year in range(2007, 2017+1) :
        file = glob.glob(f'{panel_folder}/*{year}*.csv')[0]
        df_a = pd.read_csv(file, low_memory = False)
        df_a = df_a.rename(columns = {'Respondent Identification Number':'Respondent ID',
                                      'Parent Identification Number': 'Parent Respondent ID',
                                      'Parent State (Panel)': 'Parent State',
                                      'Parent City (Panel)': 'Parent City',
                                      'Parent Name (Panel)': 'Parent Name',
                                      'Respondent State (Panel)': 'Respondent State',
                                      'Respondent Name (Panel)': 'Respondent Name',
                                      'Respondent City (Panel)': 'Respondent City',
                                      })
        df_panel.append(df_a)
        del df_a
    df_panel = pd.concat(df_panel)

    # Combined TS and Panel
    df = df_panel.merge(df_ts,
                        on = ['Activity Year','Respondent ID','Agency Code'],
                        how = 'outer',
                        suffixes = (' Panel',' TS')
                        )
    df = df[df.columns.sort_values()]

    # Strip Extra Spaces and Replace Missings with None
    for column in df.columns :
        df[column] = [x.strip() if isinstance(x, str) else x for x in df[column]]
        df.loc[df[column].isin([np.nan, '']), column] = None

    # Save
    df.to_csv(f'{save_folder}/hmda_lenders_combined_{min_year}-{max_year}.csv',
              index = False,
              sep = '|',
              )

#%% Main Routine
if __name__ == '__main__' :

    # Define Folder Paths
    RAW_DIR = config.RAW_DIR
    CLEAN_DIR = config.CLEAN_DIR
    PROJECT_DIR = config.PROJECT_DIR

    # Import HMDA Loan Data
    data_folder = RAW_DIR / 'loans'
    save_folder = CLEAN_DIR / 'loans'
    schema_file='./schemas/hmda_lar_schema_post2018.html'
    import_hmda_streaming(data_folder, save_folder, schema_file, min_year=2024, max_year=2024)
    # clean_hmda_post_2017(save_folder, min_year=2018, max_year=2023, replace=False)

    # Import HMDA Transmittal Series Data
    data_folder = RAW_DIR / 'transmissal_series'
    save_folder = CLEAN_DIR / 'transmissal_series'
    schema_file='./schemas/hmda_ts_schema_post2018.html'
    # import_hmda_streaming(data_folder, save_folder, schema_file)

    # Import HMDA Panel Data
    data_folder = RAW_DIR / 'panel'
    save_folder = CLEAN_DIR / 'panel'
    schema_file='./schemas/hmda_panel_schema_post2018.html'
    # import_hmda_post_streaming(data_folder, save_folder, schema_file)

    # Combine Lender Files
    ts_folder = CLEAN_DIR / 'transmissal_series'
    panel_folder = CLEAN_DIR / 'panel'
    save_folder = PROJECT_DIR / 'data'
    # combine_lenders_panel_ts_pre2018(panel_folder, ts_folder, save_folder, min_year=2007, max_year=2017)
    # combine_lenders_panel_ts_post2018(panel_folder, ts_folder, save_folder, min_year=2018, max_year=2023)

    # Update File List
    data_folder = CLEAN_DIR
    # HMDALoader.update_file_list(data_folder)

#%%
