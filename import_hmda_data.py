#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on: Sat Dec 3 09:49:44 2022
Last updated on: Thu Mar 14 08:06:38 2024
@author: Jonathan E. Becker (jebecker3@wisc.edu)
"""

# Import Packages
import io
import platform
import ast
import os
import glob
import zipfile
import pandas as pd
import numpy as np
import subprocess
from pyarrow import csv
import pyarrow as pa
import pyarrow.parquet as pq
from csv import Sniffer
import re

#%% Local Functions
# Get Delimiter
def get_delimiter(file_path, bytes=4096) :
    """
    Gets the delimiter used in a delimiter-separated text file.

    Parameters
    ----------
    file_path : string
        File path of a delimiter-separated text file.
    bytes : integer, optional
        Number of bytes in {file_path} to read in. The default is 4096.

    Returns
    -------
    delimiter : string
        Delimiter used in {file_path}.

    """

    # Initialize CSV Sniffer
    sniffer = Sniffer()

    # Open File
    data = io.open(file_path, mode='r', encoding='latin-1').read(bytes)

    # Find Delimiter
    delimiter = sniffer.sniff(data).delimiter

    # Return Delimiter
    return delimiter

# Unzip HMDA Data
def unzip_hmda_data(data_folder, save_folder, replace = False, file_string = 'lar') :
    """
    Unzip zipped HMDA archives.

    Parameters
    ----------
    data_folder : TYPE
        DESCRIPTION.
    save_folder : TYPE
        DESCRIPTION.
    replace : TYPE, optional
        DESCRIPTION. The default is False.
    file_string : TYPE, optional
        DESCRIPTION. The default is 'lar'.

    Returns
    -------
    None.

    """

    # Get HMDA Zip Files and keep files with desired string pattern
    zip_files = glob.glob(f'{data_folder}/*.zip')
    zip_files = [x for x in zip_files if file_string.lower() in os.path.basename(x).lower()]
    for zip_file in zip_files :

        with zipfile.ZipFile(zip_file) as z :

            for file in z.namelist() :

                # Unzip if New File Doesn't Exist or Replace Option is On
                new_file_name = f'{save_folder}/{file}'
                if not os.path.exists(new_file_name) :

                    # Extract and Create Temporary File
                    print('Extracting File:', file)
                    try :
                        z.extract(file, path = save_folder)
                    except :
                        print('Could not unzip file:', file, 'with Pythons Zipfile package. Using 7z instead.')
                        unzip_string = "C:/Program Files/7-Zip/7z.exe"
                        p = subprocess.Popen([unzip_string, "e", f"{zip_file}", f"-o{save_folder}", f"{file}", "-y"])
                        p.wait()

# Unzip HMDA Data
def unzip_hmda_file(zip_file, raw_folder) :
    """
    Unzip zipped HMDA archives.

    Parameters
    ----------
    data_folder : TYPE
        DESCRIPTION.
    save_folder : TYPE
        DESCRIPTION.
    replace : TYPE, optional
        DESCRIPTION. The default is False.
    file_string : TYPE, optional
        DESCRIPTION. The default is 'lar'.

    Returns
    -------
    None.

    """
    
    # Check that File is Zip. If not, check for similar named zip file
    if not zip_file.lower().endswith('.zip') :
        zip_file = zip_file+'.zip'
        if not os.path.exists(zip_file) :
            raise ValueError("The file name was not given as a zip file. Failed to find a comparably-named zip file.")

    # Unzip File
    with zipfile.ZipFile(zip_file) as z :
        for file in z.namelist() :

            # Unzip if New File Doesn't Exist or Replace Option is On
            raw_file_name = f'{raw_folder}/{file}'
            if not os.path.exists(raw_file_name) :

                # Extract and Create Temporary File
                print('Extracting File:', file)
                try :
                    z.extract(file, path = raw_folder)
                except :
                    print('Could not unzip file:', file, 'with Pythons Zipfile package. Using 7z instead.')
                    unzip_string = "C:/Program Files/7-Zip/7z.exe"
                    p = subprocess.Popen([unzip_string, "e", f"{zip_file}", f"-o{raw_folder}", f"{file}", "-y"])
                    p.wait()

    # Return Raw File Name
    return raw_file_name

# Rename HMDA Columns
def rename_hmda_columns(df) :
    """
    Rename HMDA columns to standardize variable names.

    Parameters
    ----------
    df : TYPE
        DESCRIPTION.

    Returns
    -------
    df : TYPE
        DESCRIPTION.

    """
    
    # Column Name Dictionary
    column_dictionary = {'occupancy': 'occupancy_type',
                         'as_of_year': 'activity_year',
                         'owner_occupancy': 'occupancy_type',
                         'loan_amount_000s': 'loan_amount',
                         'census_tract_number': 'census_tract',
                         'applicant_income_000s': 'income',
                         'derived_msa-md': 'msa_md',
                         'derived_msa_md': 'msa_md',
                         'msamd': 'msa_md',
                         'population': 'tract_population',
                         'minority_population': 'tract_minority_population_percent',
                         'hud_median_family_income': 'ffiec_msa_md_median_family_income',
                         'tract_to_msamd_income': 'tract_to_msa_income_percentage',
                         'number_of_owner_occupied_units': 'tract_owner_occupied_units',
                         'number_of_1_to_4_family_units': 'tract_one_to_four_family_homes',
                         }

    # Rename
    df = df.rename(columns = column_dictionary, errors = 'ignore')
    
    # Return DataFrame
    return df

# Dstring HMDA Columns before 2007
def destring_hmda_cols_pre2007(df) :
    
    # Numeric and Categorical Columns
    numeric_columns = ['activity_year',
                       'loan_type',
                       'loan_purpose',
                       'occupancy_type',
                       'loan_amount',
                       'action_taken',
                       'msa_md',
                       'state_code',
                       'county_code',
                       'applicant_race_1',
                       'co_applicant_race_1',
                       'applicant_sex',
                       'co_applicant_sex',
                       'income',
                       'purchaser_type',
                       'denial_reason_1',
                       'denial_reason_2',
                       'denial_reason_3',
                       'edit_status',
                       'sequence_number']

    # Convert Numeric Columns
    for col in numeric_columns :
        if col in df.columns :
            df[col] = pd.to_numeric(df[col], errors = 'coerce')

    # Return DataFrame            
    return df

# Destring HMDA Columns
def destring_hmda_cols_2007_2017(df) :
    """
    Destring numeric HMDA columns

    Parameters
    ----------
    df : TYPE
        DESCRIPTION.

    Returns
    -------
    df : TYPE
        DESCRIPTION.

    """

    # Dsplay Progress
    print('Destringing HMDA Variables')
    
    # Fix County Code and Census Tract
    geo_cols = ['state_code', 'county_code', 'census_tract']
    df[geo_cols] = df[geo_cols].apply(pd.to_numeric, errors = 'coerce')
    df['state_code'].astype('Int16')
    df['county_code'] = (1000*df['state_code'] + df['county_code']).astype('Int32')
    df['census_tract'] = np.round(100*df['census_tract']).astype('Int32')
    df['census_tract'] = df['census_tract'].astype(str)
    df['census_tract'] = [x.zfill(6) for x in df['census_tract']]
    df['census_tract'] = df['county_code'].astype('str') + df['census_tract']
    df['census_tract'] = pd.to_numeric(df['census_tract'], errors = 'coerce')
    df['census_tract'] = df['census_tract'].astype('Int64')
    
    # Numeric and Categorical Columns
    numeric_columns = ['activity_year',
                       'loan_type',
                       'loan_purpose',
                       'occupancy_type',
                       'loan_amount',
                       'action_taken',
                       'msa_md',
                       'applicant_race_1',
                       'applicant_race_2',
                       'applicant_race_3',
                       'applicant_race_4',
                       'applicant_race_5',
                       'co_applicant_race_1',
                       'co_applicant_race_2',
                       'co_applicant_race_3',
                       'co_applicant_race_4',
                       'co_applicant_race_5',
                       'applicant_sex',
                       'co_applicant_sex',
                       'income',
                       'purchaser_type',
                       'denial_reason_1',
                       'denial_reason_2',
                       'denial_reason_3',
                       'edit_status',
                       'sequence_number',
                       'rate_spread',
                       'tract_population',
                       'tract_minority_population_percent',
                       'ffiec_msa_md_median_family_income',
                       'tract_to_msa_income_percentage',
                       'tract_owner_occupied_units',
                       'tract_one_to_four_family_homes',
                       'tract_median_age_of_housing_units',
                       ]

    # Convert Columns to Numeric
    for numeric_column in numeric_columns :
        if numeric_column in df.columns :
            df[numeric_column] = pd.to_numeric(df[numeric_column], errors = 'coerce')

    # Return DataFrame
    return df

# Destring HMDA Columns
def destring_hmda_cols_after_2018(df) :
    """
    Destring numeric HMDA variables after 2018.

    Parameters
    ----------
    df : TYPE
        DESCRIPTION.

    Returns
    -------
    df : TYPE
        DESCRIPTION.

    """

    # Dsplay Progress
    print('Destringing HMDA Variables')

    # Replace Exempt w/ -99999
    for col in ['combined_loan_to_value_ratio', 'interest_rate', 'rate_spread', 'loan_term', 'prepayment_penalty_term', 'intro_rate_period', 'income', 'multifamily_affordable_units', 'property_value', 'total_loan_costs', 'total_points_and_fees', 'origination_charges', 'discount_points', 'lender_credits'] :
        print('Replacing exemptions for variable:', col)
        df.loc[df[col] == "Exempt", col] = -99999
        df[col] = pd.to_numeric(df[col], errors = 'coerce')

    # Clean Units
    col = 'total_units'
    df.loc[df[col] == "5-24", col] = 5
    df.loc[df[col] == "25-49", col] = 6
    df.loc[df[col] == "50-99", col] = 7
    df.loc[df[col] == "100-149", col] = 8
    df.loc[df[col] == ">149", col] = 9
    df[col] = pd.to_numeric(df[col], errors='coerce')

    # Clean Age
    for col in ['applicant_age', 'co_applicant_age'] :
        df.loc[df[col] == "<25", col] = 1
        df.loc[df[col] == "25-34", col] = 2
        df.loc[df[col] == "35-44", col] = 3
        df.loc[df[col] == "45-54", col] = 4
        df.loc[df[col] == "55-64", col] = 5
        df.loc[df[col] == "65-74", col] = 6
        df.loc[df[col] == ">74", col] = 7
        df[col] = pd.to_numeric(df[col], errors = 'coerce')

    # Clean Age Dummy Variables
    for col in ['applicant_age_above_62', 'co_applicant_age_above_62'] :
        df.loc[df[col].isin(["No","no","NO"]), col] = 0
        df.loc[df[col].isin(["Yes","yes","YES"]), col] = 1
        df.loc[df[col].isin(["Na","na","NA"]), col] = np.nan
        df[col] = pd.to_numeric(df[col], errors = 'coerce')

	# Clean Debt-to-Income
    col = 'debt_to_income_ratio'
    df.loc[df[col] == "<20%", col] = 10
    df.loc[df[col] == "20%-<30%", col] = 20
    df.loc[df[col] == "30%-<36%", col] = 30
    df.loc[df[col] == "50%-60%", col] = 50
    df.loc[df[col] == ">60%", col] = 60
    df.loc[df[col] == "Exempt", col] = -99999
    df[col] = pd.to_numeric(df[col], errors = 'coerce')

	# Clean Conforming Loan Limit
    col = 'conforming_loan_limit'
    if col in df.columns :
        df.loc[df[col] == "NC", col] = 0
        df.loc[df[col] == "C", col] = 1
        df.loc[df[col] == "U", col] = 1111
        df.loc[df[col] == "NA", col] = -1111
        df[col] = pd.to_numeric(df[col], errors = 'coerce')

    # Numeric and Categorical Columns
    numeric_columns = ['activity_year',
                       'loan_type',
                       'loan_purpose',
                       'occupancy_type',
                       'loan_amount',
                       'action_taken',
                       'msa_md',
                       'county_code',
                       'applicant_race_1',
                       'applicant_race_2',
                       'applicant_race_3',
                       'applicant_race_4',
                       'applicant_race_5',
                       'co_applicant_race_1',
                       'co_applicant_race_2',
                       'co_applicant_race_3',
                       'co_applicant_race_4',
                       'co_applicant_race_5',
                       'applicant_sex',
                       'co_applicant_sex',
                       'income',
                       'purchaser_type',
                       'denial_reason_1',
                       'denial_reason_2',
                       'denial_reason_3',
                       'edit_status',
                       'sequence_number',
                       'rate_spread',
                       'tract_population',
                       'tract_minority_population_percent',
                       'ffiec_msa_md_median_family_income',
                       'tract_to_msa_income_percentage',
                       'tract_owner_occupied_units',
                       'tract_one_to_four_family_homes',
                       'tract_median_age_of_housing_units',
                       ]

    # Convert Columns to Numeric
    for numeric_column in numeric_columns :
        if numeric_column in df.columns :
            df[numeric_column] = pd.to_numeric(df[numeric_column], errors = 'coerce')

    # Return DataFrame
    return df

# Rename HMDA Columns
def split_and_save_tract_variables(df, save_folder, file_name) :
    """
    Create and save cesnsus tract data.

    Parameters
    ----------
    df : TYPE
        DESCRIPTION.
    save_folder : TYPE
        DESCRIPTION.
    file_name : TYPE
        DESCRIPTION.

    Returns
    -------
    df : TYPE
        DESCRIPTION.

    """
    
    # Column Name Dictionary
    tract_variables = ['tract_population',
                       'tract_minority_population_percent',
                       'ffiec_msa_md_median_family_income',
                       'tract_to_msa_income_percentage',
                       'tract_owner_occupied_units',
                       'tract_one_to_four_family_homes',
                       'tract_median_age_of_housing_units',
                       ]
    tract_variables = [x for x in tract_variables if x in df.columns]

    # Convert Columns to Numeric
    for tract_variable in tract_variables :
        if tract_variable in df.columns :
            df[tract_variable] = pd.to_numeric(df[tract_variable], errors = 'coerce')

    # Separate and DropExisting Tract Variables
    if tract_variables :
        
        # Separate Tract Variables
        df_tract = df[['activity_year','census_tract']+tract_variables].drop_duplicates()
        df_tract.to_csv(f'{save_folder}/tract_variables/tract_vars_{file_name}.csv',
                        index=False,
                        sep='|',
                        )
        
        # Drop Tract Variables and Return DataFrame
        df = df.drop(columns = tract_variables)

    # Return DataFrame
    return df

# Prepare for Stata
def downcast_hmda_variables(df) :
    """
    Downcast HMDA variables

    Parameters
    ----------
    df : pandas DataFrame
        DESCRIPTION.

    Returns
    -------
    df : pandas DataFrame
        DESCRIPTION.

    """

    # Downcast Numeric Types
    # for col in df.columns :
    #     try :
    #         df[col] = df[col].astype('Int16')
    #     except (TypeError, OverflowError) :
    #         print('Cannot downcast variable:', col)
    for col in ['msa_md', 'county_code', 'sequence_number'] :
        if col in df.columns :
            df[col] = df[col].astype('Int32')

    # Return DataFrame and Labels
    return df

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
    value_label_file = f"{base_path}/cl/external_data/HMDA/raw_files/loans/hmda_value_labels.txt"
    with open(value_label_file, 'r') as f :
        value_labels = ast.literal_eval(f.read())

    # Read Variable Labels
    variable_label_file = f"{base_path}/cl/external_data/HMDA/raw_files/loans/hmda_variable_labels.txt"
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

# Import Historic HMDA Files (2007-2017)
def import_hmda_2007_2017(data_folder, temp_folder, save_folder, min_year=2007, max_year=2017, save_to_stata=False, save_to_csv=True, remove_raw_file=True) :
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
        files = glob.glob(f"{data_folder}/*{year}*records*.zip") + glob.glob(f"{data_folder}/*{year}*public*.zip")
        for file in files :

            # Save File Names
            file_name = os.path.basename(file).split('.')[0]
            if file_name.endswith('_csv') :
                file_name = file_name[:-4]
            save_file_csv = f'{save_folder}/{file_name}.csv.gz'
            save_file_parquet = f'{save_folder}/{file_name}.parquet'
            save_file_dta = f'{save_folder}/{file_name}.dta'

            # Read File
            if not os.path.exists(save_file_parquet) :

                # Unzip File
                raw_file = unzip_hmda_file(file, temp_folder)                

                # Detect Delimiter and Read File
                print('Reading file:', file)
                parse_options = csv.ParseOptions(delimiter=get_delimiter(raw_file, bytes=16000))
                df = csv.read_csv(raw_file, parse_options=parse_options).to_pandas()

                # Rename HMDA Columns
                df = rename_hmda_columns(df)

                # Create Unique HMDA Index
                if year == 2017 :
                    file_type = get_file_type_code(file)
                    df['HMDAIndex'] = range(df.shape[0])
                    df['HMDAIndex'] = df['HMDAIndex'].astype('str').str.zfill(9)
                    df['HMDAIndex'] = df['activity_year'].astype('str') + file_type + '_' + df['HMDAIndex']

                # Drop Derived Columns b/c of redundancies
                derived_columns = ['derived_loan_product_type','derived_race','derived_ethnicity','derived_sex','derived_dwelling_category']
                df = df.drop(columns = derived_columns, errors='ignore')

                # Destring HMDA Columns
                df = destring_hmda_cols_2007_2017(df)
                
                # Census Tract to String and Fix NAs
                df['census_tract'] = pd.to_numeric(df['census_tract'], errors='coerce')
                df['census_tract'] = df['census_tract'].astype('Int64')
                df['census_tract'] = df['census_tract'].astype('str')
                df['census_tract'] = df['census_tract'].str.zfill(11)
                df.loc[df['census_tract'].str.contains('NA', regex=False), 'census_tract'] = ""

                # Split Off Tract Variables
                df = split_and_save_tract_variables(df, save_folder, file_name)

                # Save to Parquet
                dt = pa.Table.from_pandas(df, preserve_index=False)
                pq.write_table(dt, save_file_parquet)

                # Save to CSV
                if save_to_csv :
                    write_options = csv.WriteOptions(delimiter = '|')
                    with pa.CompressedOutputStream(save_file_csv, "gzip") as out :
                        csv.write_csv(dt, out, write_options=write_options)

                # Save to Stata
                if save_to_stata :
                    df, variable_labels, value_labels = prepare_hmda_for_stata(df)
                    df.to_stata(save_file_dta,
                                write_index=False,
                                variable_labels=variable_labels,
                                value_labels=value_labels,
                                )

                # Remove the Temporary Raw File
                if remove_raw_file :
                    os.remove(raw_file)

# Import Data After 2017
def import_hmda_post_2017(data_folder, temp_folder, save_folder, min_year=2018, max_year=2023, replace=False, save_to_stata=False, save_to_csv=True, remove_raw_file=True) :
    """
    Import and clean HMDA data for 2018 onward.

    Parameters
    ----------
    data_folder : str
        DESCRIPTION.
    save_folder : str
        DESCRIPTION.
    min_year : int, optional
        DESCRIPTION. The default is 2018.
    max_year : int, optional
        DESCRIPTION. The default is 2022.

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
            save_file_csv = f'{save_folder}/{file_name}.csv.gz'
            save_file_parquet = f'{save_folder}/{file_name}.parquet'
            save_file_dta = f'{save_folder}/{file_name}.dta'

            # Clean if Files are Missing
            if (not os.path.exists(save_file_parquet)) or ((not os.path.exists(save_file_csv)) and save_to_csv) or replace:

                # Unzip File
                raw_file = unzip_hmda_file(file, temp_folder)                

                # Detect Delimiter and Read File
                print('Reading file:', file)
                parse_options = csv.ParseOptions(delimiter=get_delimiter(raw_file, bytes=16000))
                df = csv.read_csv(raw_file, parse_options=parse_options).to_pandas()

                # Create Unique HMDA Index
                file_type = get_file_type_code(file)
                df['HMDAIndex'] = range(df.shape[0])
                df['HMDAIndex'] = df['HMDAIndex'].astype('str').str.zfill(9)
                df['HMDAIndex'] = df['activity_year'].astype('str') + file_type + '_' + df['HMDAIndex']

                # Drop Derived Columns b/c of redundancies
                derived_columns = ['derived_loan_product_type','derived_race','derived_ethnicity','derived_sex','derived_dwelling_category']
                df = df.drop(columns = derived_columns, errors='ignore')

                # Rename HMDA Columns
                df = rename_hmda_columns(df)

                # Destring HMDA Columns
                df = destring_hmda_cols_after_2018(df)

                # Census Tract to String and Fix NAs
                df['census_tract'] = pd.to_numeric(df['census_tract'], errors='coerce')
                df['census_tract'] = df['census_tract'].astype('Int64')
                df['census_tract'] = df['census_tract'].astype('str')
                df['census_tract'] = df['census_tract'].str.zfill(11)
                df.loc[df['census_tract'].str.contains('NA', regex=False), 'census_tract'] = ""

                # Split Off Tract Variables
                df = split_and_save_tract_variables(df, save_folder, file_name)

                # Prepare for Stata
                df = downcast_hmda_variables(df)

                # Save to Parquet
                dt = pa.Table.from_pandas(df, preserve_index=False)
                pq.write_table(dt, save_file_parquet)

                # Save to CSV
                if save_to_csv :
                    write_options = csv.WriteOptions(delimiter = '|')
                    with pa.CompressedOutputStream(save_file_csv, "gzip") as out :
                        csv.write_csv(dt, out, write_options=write_options)

                # Save to Stata
                if save_to_stata :
                    df, variable_labels, value_labels = prepare_hmda_for_stata(df)
                    df.to_stata(save_file_dta,
                                write_index=False,
                                variable_labels=variable_labels,
                                value_labels=value_labels,
                                )

                # Remove the Temporary Raw File
                if remove_raw_file :
                    os.remove(raw_file)
                    
#%% Combine Files
# Combine Lenders After 2018
def combine_lenders_panel_ts_post2018(panel_folder, ts_folder, save_folder, min_year = 2018, max_year = 2022) :
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
        First year of data to include. The default is 2007.
    max_year : int, optional
        Last year of data to include. The default is 2017.

    Returns
    -------
    None.

    """

    # Import Panel and TS Data
    ts_folder = '/project/cl/external_data/HMDA/raw_files/transmissal_series'
    ts_files = glob.glob(f'{ts_folder}/*.txt')
    df_ts = []
    for year in range(2018, 2022+1) :
        file = [x for x in ts_files if str(year) in x][0]
        df_a = pd.read_csv(file, sep = '|', quoting = 1)
        df_ts.append(df_a)
    df_ts = pd.concat(df_ts)

    # Import Panel and TS Data
    panel_folder = '/project/cl/external_data/HMDA/raw_files/panel'
    panel_files = glob.glob(f'{panel_folder}/*.txt')
    df_panel = []
    for year in range(2018, 2022+1) :
        file = [x for x in panel_files if str(year) in x][0]
        df_a = pd.read_csv(file, sep = '|', quoting = 1)
        df_a = df_a.rename(columns = {'upper':'lei'})
        df_panel.append(df_a)
    df_panel = pd.concat(df_panel)

    # Combined TS and Panel
    df = df_panel.merge(df_ts,
                        on = ['activity_year','lei'],
                        how = 'outer',
                        suffixes = ('_panel','_ts')
                        )
    df = df[df.columns.sort_values()]

    # Save
    df.to_csv(f'{save_folder}/hmda_lenders_combined_{min_year}-{max_year}.csv',
              index = False,
              sep = '|',
              )

# Combine Lenders Before 2018
def combine_lenders_panel_ts_pre2018(panel_folder, ts_folder, save_folder, min_year = 2007, max_year = 2017) :
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

#%% File Management
# Extract Year From FileNames
def extract_years_from_strings(strings) :
    years = []
    for string in strings:
        found_year = re.findall(r'\d{4}', string)[0]
        years.append(found_year)
    return years

# Update File List
def update_file_list(data_folder) :
    """
    Creates a CSV list of all HMDA files. Helps to standardize future work by
    keeping track of which version of a file we should be using.

    Parameters
    ----------
    data_folder : str
        Folder where cleaned HMDA data is stored.

    Returns
    -------
    None.

    """

    # Get List of Files and Drop Folders
    files = glob.glob(f'{data_folder}/*')
    files = [x for x in files if os.path.isfile(x)]
    files = [x for x in files if 'file_list_hmda' not in x]

    # Create DataFrame and Get Prefix
    df = pd.DataFrame(files, columns=['FileName'])
    df['FilePrefix'] = [os.path.basename(x).split('.')[0] for x in df['FileName']]

    # Get File Types by Prefix
    df['FileParquet'] = 1*df.FileName.str.lower().str.endswith('.parquet')
    df['FileCSVGZ'] = 1*df.FileName.str.lower().str.endswith('.csv.gz')
    df['FileDTA'] = 1*df.FileName.str.lower().str.endswith('.dta')
    df.FileParquet = df.groupby(['FilePrefix'])['FileParquet'].transform('max')
    df.FileCSVGZ = df.groupby(['FilePrefix'])['FileCSVGZ'].transform('max')
    df.FileDTA = df.groupby(['FilePrefix'])['FileDTA'].transform('max')
    df = df.drop(columns=['FileName'])
    df = df.drop_duplicates()

    # Get Years
    df['Year'] = extract_years_from_strings(df.FilePrefix)
    df.Year = df.Year.astype('Int16')

    # Get Version Types from Prefixes
    df['VersionType'] = 'LAR'
    df.loc[df['FilePrefix'].str.lower().str.contains('mlar'), 'VersionType'] = 'MLAR'
    df.loc[df['FilePrefix'].str.lower().str.contains('nationwide'), 'VersionType'] = 'NARC'
    df.loc[df['FilePrefix'].str.lower().str.contains('public_lar'), 'VersionType'] = 'SNAP'
    df.loc[df['FilePrefix'].str.lower().str.contains('one_year'), 'VersionType'] = 'YEAR1'
    df.loc[df['FilePrefix'].str.lower().str.contains('three_year'), 'VersionType'] = 'YEAR3'
    df.VersionType = pd.Categorical(df.VersionType,
                                    categories=['YEAR3','YEAR1','SNAP','MLAR','NARC','LAR'],
                                    ordered=True)

    # Create Master Indicator
    df = df.sort_values(by=['Year', 'VersionType'], ascending=[False, True])
    df['VersionRank'] = df.groupby(['Year'])['VersionType'].rank('dense')
    df['i_Master'] = 1*(df['VersionRank'] == 1)
    df = df.drop(columns=['VersionRank'])

    # Re-order Variables and Save
    df = df[['Year','FilePrefix','VersionType','i_Master','FileParquet','FileCSVGZ','FileDTA']]
    df.to_csv(f'{data_folder}/file_list_hmda.csv', index=False)

# Get File Type
def get_file_type_code(file_name) :
    
    # Get Base Name of File
    base_name = os.path.basename(file_name).split('.')[0]
    
    # Get Version Types from Prefixes
    file_type_code = None
    if 'mlar' in base_name.lower() :
        file_type_code = 'e'
    if 'nationwide' in base_name.lower() :
        file_type_code = 'd'
    if 'public_lar' in base_name.lower() :
        file_type_code = 'c'
    if 'one_year' in base_name.lower() :
        file_type_code = 'b'
    if 'three_year' in base_name.lower() :
        file_type_code = 'a'
    if not file_type_code :
        raise ValueError("Cannot parse the HMDA file type from the provided file name.")
        
    # Return Type Code
    return file_type_code

#%% Main Routine
if __name__ == '__main__' :

    # Setup: Define Base Folder
    system_type = platform.system()
    if 'windows' in system_type.lower() :
        base_path = 'V:'
    elif 'linux' in system_type.lower() :
        base_path = '/project'

    ## Unzip HMDA Data
    # LAR
    data_folder = f'{base_path}/cl/external_data/HMDA/zip_files/loans'
    save_folder = f'{base_path}/cl/external_data/HMDA/raw_files/loans'
    # unzip_hmda_data(data_folder, save_folder, file_string = 'lar')
    # MSA-MD
    data_folder = f'{base_path}/cl/external_data/HMDA/zip_files/msamd'
    save_folder = f'{base_path}/cl/external_data/HMDA/raw_files/msamd'
    # unzip_hmda_data(data_folder, save_folder, file_string='msa')
    # Panel
    data_folder = f'{base_path}/cl/external_data/HMDA/zip_files/panel'
    save_folder = f'{base_path}/cl/external_data/HMDA/raw_files/panel'
    # unzip_hmda_data(data_folder, save_folder, file_string='panel')
    # Transmissal Series
    data_folder = f'{base_path}/cl/external_data/HMDA/zip_files/transmissal_series'
    save_folder = f'{base_path}/cl/external_data/HMDA/raw_files/transmissal_series'
    # unzip_hmda_data(data_folder, save_folder, file_string='ts')

    ## Import HMDA Data
    data_folder = f'{base_path}/cl/external_data/HMDA/zip_files/loans'
    temp_folder = f'{base_path}/cl/external_data/HMDA/raw_files/loans'
    save_folder = f'{base_path}/cl/external_data/HMDA/clean_files'
    # for year in range(1990, 2006+1) :
    #     import_hmda_pre_2007(data_folder, save_folder, contains_string = f'{year}')
    import_hmda_2007_2017(data_folder, temp_folder, save_folder, min_year=2017, max_year=2017, save_to_stata=False)
    # import_hmda_post_2017(data_folder, temp_folder, save_folder, min_year=2018, max_year=2023) 

    # Combine Lender Files
    ts_folder = f'{base_path}/cl/external_data/HMDA/raw_files/transmissal_series'
    panel_folder = f'{base_path}/cl/external_data/HMDA/raw_files/panel'
    save_folder = f'{base_path}/cl/external_data/HMDA'
    # combine_lenders_panel_ts_pre2018(panel_folder, ts_folder, save_folder, min_year = 2007, max_year = 2017)
    # combine_lenders_panel_ts(panel_folder, ts_folder, save_folder, min_year = 2018, max_year = 2022)

    # Update File List
    data_folder = f'{base_path}/cl/external_data/HMDA/clean_files'
    update_file_list(data_folder)
