# -*- coding: utf-8 -*-
"""
Created on Fri Jul 19 10:20:24 2024
@author: jebecker3
"""

# Import Packages
import pandas as pd
import config

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
    extension='parquet',
    columns=None,
    filters=None,
    verbose=False,
    **kwargs,
) -> pd.DataFrame :
    """
    Load HMDA files.

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
    extension : str, optional
            The file extension of HMDA files to load. The default is 'parquet'.
    columns : list, optional
        The columns to load. The default is None.
    filters : list, optional
        The filters to apply. The default is None.
    verbose : bool, optional
        Whether to print progress messages. The default is False.
    **kwargs : optional
        Additional arguments to pass to pd.read_parquet.
        
    Returns
    -------
    df : DataFrame
        The loaded HMDA file.

    """
    
    # Get HMDA Files
    files = get_hmda_files(data_folder=data_folder, file_type=file_type, min_year=min_year, max_year=max_year, extension=extension)

    # Load File
    df = []
    for file in files :
        if verbose :
            print('Adding data from file:', file)
        df_a = pd.read_parquet(file, columns=columns, filters=filters, **kwargs)
        df.append(df_a)
    df = pd.concat(df)

    # Return DataFrame
    return df
