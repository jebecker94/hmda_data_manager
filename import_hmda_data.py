#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on: Saturday December 3, 2022
Last updated on: Sunday March 30, 2025
@author: Jonathan E. Becker
"""

# Import Packages
import io
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
import config
import time
import polars as pl
import shutil

#%% Support Functions
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

# Get File Schema
def get_file_schema(schema_file, schema_type='pyarrow') :
    """
    Convert CFPB HMDA schema to a PyArrow or Pandas schema.

    Parameters
    ----------
    schema_file : str, optional
        File path to the CFPB HMDA schema. The default is './hmda_public_lar_schema.html'.
    schema_type : str, optional
        Type of schema to convert to. The default is 'pyarrow'.

    Returns
    -------
    schema : PyArrow Schema or Pandas DataFrame
        Schema for the HMDA LAR data.
    """

    # Check Schema Type
    if schema_type not in ['pyarrow','pandas','polars'] :
        raise ValueError('The schema type must be either "pyarrow" or "pandas" or "polars".')

    # Load the schema file
    df = pd.read_html(schema_file)[0]

    # Get Field Column
    FieldVar = 'Field'
    if 'Field' not in df.columns :
        FieldVar = 'Fields'
    
    LengthVar = 'Max Length'
    if 'Max Length' not in df.columns :
        LengthVar = 'Maximum Length'

    # Convert the schema to a PyArrow schema
    if schema_type == 'pyarrow' :
        schema = []
        for _,row in df.iterrows() :
            pa_type = pa.string()
            if (row['Type'] == 'Numeric'):
                pa_type = pa.float64()
            if (row['Type'] == 'Numeric') & (row[LengthVar] <= 4):
                pa_type = pa.int16()
            if (row['Type'] == 'Numeric') & (row[LengthVar] > 4):
                pa_type = pa.int32()
            if (row['Type'] == 'Numeric') & (row[LengthVar] > 9):
                pa_type = pa.int64()
            schema.append((row[FieldVar], pa_type))
        schema = pa.schema(schema)

    # Convert the schema to a Pandas schema
    elif schema_type == 'pandas' :
        schema = {}
        for _,row in df.iterrows() :
            pd_type = 'str'
            if (row['Type'] == 'Numeric') :
                pd_type = 'Float64'
            if (row['Type'] == 'Numeric') & (row[LengthVar] <= 4):
                pd_type = 'Int16'
            if (row['Type'] == 'Numeric') & (row[LengthVar] > 4):
                pd_type = 'Int32'
            if (row['Type'] == 'Numeric') & (row[LengthVar] > 9):
                pd_type = 'Int64'
            schema[row[FieldVar]] = pd_type

    # Convert the schema to a Polars schema (In progress)
    elif schema_type == 'polars' :
        schema = {}
        for _,row in df.iterrows() :
            pd_type = pl.String()
            if (row['Type'] == 'Numeric') :
                pd_type = pl.Float64()
            if (row['Type'] == 'Numeric') & (row[LengthVar] <= 4):
                pd_type = pl.Int16()
            if (row['Type'] == 'Numeric') & (row[LengthVar] > 4):
                pd_type = pl.Int32()
            if (row['Type'] == 'Numeric') & (row[LengthVar] > 9):
                pd_type = pl.Int64()
            schema[row[FieldVar]] = pd_type
        schema = pl.Schema(schema)

    # Return the schema
    return schema

# Replace Column Names in CSV
def replace_csv_column_names(csv_file, column_name_mapper={}) :
    """
    Check the first line of a CSV and replace the column names with correct names according to the provided dictionary.

    Parameters
    ----------
    csv_file : str
        File path to the CSV file.
    column_name_mapper : dict
        Dictionary of column names to replace.

    Returns
    -------
    None.

    """

    # Get File Delimiter
    delimiter = get_delimiter(csv_file, bytes=16000)

    # Read First Line
    with open(csv_file, 'r') as f:
        first_line = f.readline().strip()

    # Replace Column Names
    first_line_items = first_line.split(delimiter)
    new_first_line_items = []
    for first_line_item in first_line_items :
        for key,item in column_name_mapper.items() :
            if first_line_item == key :
                first_line_item = item
        new_first_line_items.append(first_line_item)
    new_first_line = delimiter.join(new_first_line_items)

    # Write New First Line and Copy Rest of File
    with open(csv_file, 'r') as f:
        lines = f.readlines()
    lines[0] = new_first_line+'\n'
    with open(csv_file, 'w') as f:
        f.writelines(lines)

# Unzip HMDA Data
def unzip_hmda_file(zip_file, raw_folder, replace=False) :
    """
    Unzip zipped HMDA archives.

    Parameters
    ----------
    data_folder : str
        Folder where zip files are stored.
    save_folder : str
        Folder where unzipped files will be saved.
    replace : bool, optional
        Whether to replace the unzipped file if one already exists. The default is False.
    file_string : str, optional
        Partial string to query specific file. The default is 'lar'.

    Returns
    -------
    raw_file_name : str
        File name of the unzipped file.

    """
    
    # Check that File is Zip. If not, check for similar named zip file
    if not zip_file.lower().endswith('.zip') :
        zip_file = zip_file+'.zip'
        if not os.path.exists(zip_file) :
            raise ValueError("The file name was not given as a zip file. Failed to find a comparably-named zip file.")

    # Unzip File
    with zipfile.ZipFile(zip_file) as z :
        delimited_files = [x for x in z.namelist() if (x.endswith('.txt') or x.endswith('.csv')) and '/' not in x]
        for file in delimited_files :

            # Unzip if New File Doesn't Exist or Replace Option is On
            raw_file_name = f'{raw_folder}/{file}'
            if (not os.path.exists(raw_file_name)) or replace :

                # Extract and Create Temporary File
                print('Extracting File:', file)
                try :
                    z.extract(file, path=raw_folder)
                except :
                    print('Could not unzip file:', file, 'with Pythons Zipfile package. Using 7z instead.')
                    unzip_string = "C:/Program Files/7-Zip/7z.exe"
                    p = subprocess.Popen([unzip_string, "e", f"{zip_file}", f"-o{raw_folder}", f"{file}", "-y"])
                    p.wait()

            # Convert First Line of Panel Files
            if 'panel' in file :
                column_name_mapper = {'topholder_rssd':'top_holder_rssd',
                                    'topholder_name':'top_holder_name',
                                    'upper':'lei',
                                    }
                replace_csv_column_names(raw_file_name, column_name_mapper=column_name_mapper)

    # Return Raw File Name
    return raw_file_name

# Rename HMDA Columns
def rename_hmda_columns(df, df_type='polars') :
    """
    Rename HMDA columns to standardize variable names.

    Parameters
    ----------
    df : pd.DataFrame, pl.DataFrame, pl.LazyFrame
        DataFrame to rename columns in.

    Returns
    -------
    df : pd.DataFrame, pl.DataFrame, pl.LazyFrame
        DataFrame with renamed columns.

    """
    
    # Column Name Dictionary
    column_dictionary = {
        'occupancy': 'occupancy_type',
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
    if df_type == 'pandas' :
        df = df.rename(columns = column_dictionary, errors='ignore')
    elif df_type == 'polars' :
        df = df.rename(column_dictionary, strict=False)

    # Return DataFrame
    return df

# Dstring HMDA Columns before 2007
def destring_hmda_cols_pre2007(df) :
    """
    Destring numeric HMDA columns before 2007.

    Parameters
    ----------
    df : pandas DataFrame
        DESCRIPTION.
    
    Returns
    -------
    df : pandas DataFrame
        DESCRIPTION.

    """
    
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
    df : pd.DataFrame, pl.DataFrame, pl.LazyFrame
        DataFrame to destring.

    Returns
    -------
    df : pd.DataFrame, pl.DataFrame, pl.LazyFrame
        DataFrame with destringed columns.

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
    numeric_columns = [
        'activity_year',
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
            df[numeric_column] = pd.to_numeric(df[numeric_column], errors='coerce')

    # Return DataFrame
    return df

# Destring HMDA Columns
def destring_hmda_cols_after_2018(lf) :
    """
    Destring numeric HMDA variables after 2018.

    Parameters
    ----------
    df : pandas DataFrame
        DESCRIPTION.

    Returns
    -------
    df : pandas DataFrame
        DESCRIPTION.

    """

    # Dsplay Progress
    print('Destringing HMDA Variables')

    # Replace Exempt w/ -99999
    exempt_cols = [
        'combined_loan_to_value_ratio',
        'interest_rate',
        'rate_spread',
        'loan_term',
        'prepayment_penalty_term',
        'intro_rate_period',
        'income',
        'multifamily_affordable_units',
        'property_value',
        'total_loan_costs',
        'total_points_and_fees',
        'origination_charges',
        'discount_points',
        'lender_credits',
    ]
    for exempt_col in exempt_cols :
        lf = lf.with_columns(pl.col(exempt_col).replace("Exempt", "-99999").alias(exempt_col))
        lf = lf.cast({exempt_col: pl.Float64}, strict=False)

    # Clean Units
    replace_column = 'total_units'
    lf = lf.with_columns(pl.col(replace_column).replace(["5-24","25-49","50-99","100-149",">149"], [5,6,7,8,9]).alias(replace_column))
    lf = lf.cast({replace_column: pl.Float64}, strict=False)

    # Clean Age
    for replace_column in ['applicant_age', 'co_applicant_age'] :
        lf = lf.with_columns(pl.col(replace_column).replace(["<25","25-34","35-44","45-54","55-64","65-74",">74"], [1,2,3,4,5,6,7]).alias(replace_column))
        lf = lf.cast({replace_column: pl.Float64}, strict=False)

    # Clean Age Dummy Variables
    for replace_column in ['applicant_age_above_62', 'co_applicant_age_above_62'] :
        lf = lf.with_columns(pl.col(replace_column).replace(["No","no","NO","Yes","yes","YES","Na","na","NA"], [0,0,0,1,1,1,None,None,None]).alias(replace_column))
        lf = lf.cast({replace_column: pl.Float64}, strict=False)

	# Clean Debt-to-Income
    replace_column = 'debt_to_income_ratio'
    lf = lf.with_columns(pl.col(replace_column).replace(["<20%","20%-<30%","30%-<36%","50%-60%",">60%","Exempt"], [10,20,30,50,60,-99999]).alias(replace_column))
    lf = lf.cast({replace_column: pl.Float64}, strict=False)

	# Clean Conforming Loan Limit
    replace_column = 'conforming_loan_limit'
    lf = lf.with_columns(pl.col(replace_column).replace(["NC","C","U","NA"], [0,1,1111,-1111]).alias(replace_column))
    lf = lf.cast({replace_column: pl.Float64}, strict=False)

    # Numeric and Categorical Columns
    numeric_columns = [
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
        'applicant_ethnicity_1',
        'applicant_ethnicity_2',
        'applicant_ethnicity_3',
        'applicant_ethnicity_4',
        'applicant_ethnicity_5',
        'co_applicant_ethnicity_1',
        'co_applicant_ethnicity_2',
        'co_applicant_ethnicity_3',
        'co_applicant_ethnicity_4',
        'co_applicant_ethnicity_5',
        'applicant_sex',
        'co_applicant_sex',
        'income',
        'purchaser_type',
        'submission_of_application',
        'initially_payable_to_institution',
        'aus_1',
        'aus_2',
        'aus_3',
        'aus_4',
        'aus_5',
        'denial_reason_1',
        'denial_reason_2',
        'denial_reason_3',
        'denial_reason_4',
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
        if numeric_column in lf.collect_schema().names() :
            lf = lf.cast({numeric_column: pl.Float64}, strict=False)

    # Return DataFrame
    return lf

# Rename HMDA Columns
def split_and_save_tract_variables(df, save_folder, file_name) :
    """
    Split and save tract variables from the HMDA data frame.

    Parameters
    ----------
    df : pd.DataFrame, pl.DataFrame, pl.LazyFrame
        Data with tract variables.
    save_folder : str
        Folder to save the tract variables.
    file_name : str
        File name to save the tract variables.

    Returns
    -------
    df : pd.DataFrame, pl.DataFrame, pl.LazyFrame
        Data frame without the tract variables.

    """

    # Check DataFrame Type
    if not isinstance(df, [pd.DataFrame, pl.DataFrame, pl.LazyFrame]) :
        raise ValueError('The input dataframe must be a pandas DataFrame, polars lazyframe, or polars dataframe.')
    
    # Column Name Dictionary
    tract_variables = [
        'tract_population',
        'tract_minority_population_percent',
        'ffiec_msa_md_median_family_income',
        'tract_to_msa_income_percentage',
        'tract_owner_occupied_units',
        'tract_one_to_four_family_homes',
        'tract_median_age_of_housing_units',
    ]
    tract_variables = [x for x in tract_variables if x in df.columns]

    # Pandas Implementation
    if isinstance(df, pd.DataFrame) :
        # Convert Columns to Numeric
        for tract_variable in tract_variables :
            df[tract_variable] = pd.to_numeric(df[tract_variable], errors='coerce')

        # Separate and DropExisting Tract Variables
        if tract_variables :
            
            # Separate Tract Variables
            df_tract = df[['activity_year','census_tract']+tract_variables].drop_duplicates()
            df_tract.to_parquet(f'{save_folder}/tract_variables/tract_vars_{file_name}.parquet', index=False)
            
            # Drop Tract Variables and Return DataFrame
            df = df.drop(columns=tract_variables)

    # Polars Implementation
    elif isinstance(df, pl.DataFrame) | isinstance(df, pl.LazyFrame) :
        # Convert Columns to Numeric
        for tract_variable in tract_variables :
            df = df.with_columns(pl.col(tract_variable).cast(pl.Float64))

        # Separate and Drop Existing Tract Variables
        if tract_variables :
            
            # Separate Tract Variables
            df_tract = df.select(['activity_year','census_tract']+tract_variables).drop_duplicates()
            df_tract.write_parquet(f'{save_folder}/tract_variables/tract_vars_{file_name}.parquet')
            
            # Drop Tract Variables and Return DataFrame
            df = df.drop(tract_variables)

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
                    file_type = get_file_type_code(file)
                    lf = lf.cast({'HMDAIndex': pl.String}, strict=False)
                    lf = lf.with_columns(pl.col('HMDAIndex').str.zfill(9).alias('HMDAIndex'))
                    lf = lf.with_columns((str(year)+file_type+'_'+pl.col('HMDAIndex')).alias('HMDAIndex'))

                # Save as Parquet (Streaming)
                lf.sink_parquet(save_file)

                # Remove the Temporary Raw File
                if remove_raw_file :
                    time.sleep(1)
                    os.remove(raw_file)

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
                    file_type = get_file_type_code(file)
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
    files = glob.glob(f'{data_folder}/**', recursive=True)
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

    # Get File Type
    df['FileType'] = ''
    df.loc[df['FilePrefix'].str.lower().str.contains('lar'), 'FileType'] = 'lar'
    df.loc[df['FilePrefix'].str.lower().str.contains('panel'), 'FileType'] = 'panel'
    df.loc[df['FilePrefix'].str.lower().str.contains('ts'), 'FileType'] = 'ts'

    # Clean Up
    df['FolderName'] = [os.path.dirname(x) for x in df['FileName']]
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
    df.loc[df['FilePrefix'].str.lower().str.contains('public_panel'), 'VersionType'] = 'SNAP'
    df.loc[df['FilePrefix'].str.lower().str.contains('public_ts'), 'VersionType'] = 'SNAP'
    df.loc[df['FilePrefix'].str.lower().str.contains('one_year'), 'VersionType'] = 'YEAR1'
    df.loc[df['FilePrefix'].str.lower().str.contains('three_year'), 'VersionType'] = 'YEAR3'
    df.VersionType = pd.Categorical(df.VersionType,
                                    categories=['YEAR3','YEAR1','SNAP','MLAR','NARC','LAR'],
                                    ordered=True)

    # Create Master Indicator
    df = df.sort_values(by=['FileType','Year','VersionType'], ascending=True)
    df['VersionRank'] = df.groupby(['Year'])['VersionType'].rank('dense')
    df['i_Master'] = 1*(df['VersionRank'] == 1)
    df = df.drop(columns=['VersionRank'])

    # Re-order Variables and Save
    df = df[['FileType','Year','FilePrefix','VersionType','i_Master','FileParquet','FileCSVGZ','FileDTA','FolderName']]
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
    if 'public_panel' in base_name.lower() :
        file_type_code = 'c'
    if 'public_ts' in base_name.lower() :
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
    # combine_lenders_panel_ts_pre2018(panel_folder, ts_folder, save_folder, min_year = 2007, max_year = 2017)
    # combine_lenders_panel_ts_post2018(panel_folder, ts_folder, save_folder, min_year=2018, max_year=2023)

    # Update File List
    data_folder = CLEAN_DIR
    # update_file_list(data_folder)

#%%
