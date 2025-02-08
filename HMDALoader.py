# -*- coding: utf-8 -*-
"""
Created on Fri Jul 19 10:20:24 2024
@author: jebecker3
"""

# Import Packages
import pandas as pd

# Get List of HMDA Files
def get_hmda_files(data_folder, min_year=None, max_year=None, version_type=None, extension=None) :
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
    files = list(df.FilePrefix)
    if extension :
        files = [f'{data_folder}/{x}.{extension}' for x in files]

    # Return File List
    return files
