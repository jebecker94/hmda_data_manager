# -*- coding: utf-8 -*-
"""
Created on Friday Jul 19 10:20:24 2024
Updated On: Wednesday May 21 10:00:00 2025
@author: Jonathan E. Becker
"""

# Import Packages
import pandas as pd
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import config
from typing import Union
import glob
import os
import re

# Set Folder Paths
DATA_DIR = config.DATA_DIR

# Get List of HMDA Files
def get_hmda_files(data_folder=DATA_DIR, file_type='lar', min_year=None, max_year=None, version_type=None, extension=None) :
    """
    Gets the list of most up-to-date HMDA files.

    Parameters
    ----------
    data_folder : str
        The folder where the HMDA files are stored.
    min_year : int, optional
        The first year of HMDA files to return. The default is None.
    max_year : int, optional
        The last year of HMDA files to return. The default is None.
    version_type : str, optional
        The version type of HMDA files to return. The default is None.
    extension : str, optional
        The file extension of HMDA files to return. The default is None.

    Returns
    -------
    files : list
        List of HMDA files.

    """

    # Path of File List
    list_file = f'{data_folder}/file_list_hmda.csv'

    # Load File List
    df = pd.read_csv(list_file)

    # Filter by FileType
    df = df.query(f'FileType.str.lower() == "{file_type.lower()}"')

    # Filter by Years
    if min_year :
        df = df.query(f'Year >= {min_year}')
    if max_year :
        df = df.query(f'Year <= {max_year}')

    # Filter by Extension Type
    if extension :
        if extension.lower() == 'parquet' :
            df = df.query('FileParquet==1')
        if extension.lower() == 'csv.gz' :
            df = df.query('FileCSVGZ==1')
        if extension.lower() == 'dta' :
            df = df.query('FileDTA==1')

    # Filter by Version Type
    if version_type :
        df = df.query(f'VersionType == {version_type}')

    # Keep Most Recent File For Each Year
    df = df.drop_duplicates(subset=['Year'], keep='first')
    
    # Sort by Year
    df = df.sort_values(by=['Year'])

    # Get File Names And Add Extensions
    folders = list(df.FolderName)
    files = list(df.FilePrefix)
    if extension :
        files = [f'{x}/{y}.{extension}' for x,y in zip(folders,files)]

    # Return File List
    return files

# Load HMDA Files
def load_hmda_file(
    data_folder=DATA_DIR,
    file_type='lar',
    min_year=2018,
    max_year=2023,
    columns=None,
    filters=None,
    verbose=False,
    engine='pandas',
    **kwargs,
) -> Union[pd.DataFrame, pl.LazyFrame, pl.DataFrame, pa.Table] :
    """
    Load HMDA files.

    Note that in orrder to load files efficiently, we use only parquet formats. Other formats are not supported at this time, but may be implemented later.

    Parameters
    ----------
    data_folder : str
        The folder where the HMDA files are stored.
    file_type : str, optional
        The type of HMDA file to load. The default is 'lar'.
    min_year : int, optional
            The first year of HMDA files to load. The default is 2018.
    max_year : int, optional
            The last year of HMDA files to load. The default is 2023.
    columns : list, optional
        The columns to load. The default is None.
    filters : list, optional
        The filters to apply. The default is None.
    verbose : bool, optional
        Whether to print progress messages. The default is False.
    engine : str, optional
        The engine to use for loading the data, either 'pandas', 'polars', or 'pyarrow'. The default is 'pandas'.
    **kwargs : optional
        Additional arguments to pass to pd.read_parquet.
        
    Returns
    -------
    df : DataFrame
        The loaded HMDA file.

    """
    
    # Get HMDA Files
    files = get_hmda_files(data_folder=data_folder, file_type=file_type, min_year=min_year, max_year=max_year, extension='parquet')

    # Load File
    df = []
    if engine=='pandas':
        for file in files :
            if verbose :
                print('Adding data from file:', file)
            df_a = pd.read_parquet(file, columns=columns, filters=filters, **kwargs) # Note: Filters must be passed in pyarrow/pandas format
            df.append(df_a)
        df = pd.concat(df)
    if engine=='pyarrow':
        for file in files :
            if verbose :
                print('Adding data from file:', file)
            df_a = pq.read_table(file, columns=columns, filters=filters, **kwargs) # Note: Filters must be passed in pyarrow/pandas format
            df.append(df_a)
        df = pa.concat_tables(df)
    if engine=='polars':
        for file in files :
            if verbose :
                print('Adding data from file:', file)
            df_a = pl.scan_parquet(file, **kwargs) # Note: We'll default to lazy loading when using polars
            # df_a = df_a.filter(filters) # Note: Filters must be passed in polars format
            # df_a = df_a.select(columns)
            df.append(df_a)
        df = pl.concat(df)

    # Return DataFrame
    return df

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
