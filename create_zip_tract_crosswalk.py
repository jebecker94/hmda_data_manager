# -*- coding: utf-8 -*-
"""
Created on Tue Apr 26 14:15:32 2022
@author: jebecker3
"""

# Import Packages
import os
import glob
import pandas as pd

## Main Routine
def combine_crosswalks(data_folder, first_year = 2012, last_year = 2022) :

    # Get All Files
    files = glob.glob(f'{data_folder}/ZIP_TRACT_*.xlsx')

    # Keep Files in Appropriate Years
    yearly_files = []
    for year in range(first_year, last_year + 1) :
        yearly_files += [x for x in files if str(year) in os.path.basename(x)]
    yearly_files.sort()

    # Combine Quarterly Crosswalks
    df = []
    for file in yearly_files :

        # Display Progress
        print('Reading file:', file)

        # Data to append
        usecols=['ZIP', 'TRACT']
        df_a = pd.read_excel(file,
                             engine = 'openpyxl',
                             usecols = lambda x: x.upper() in usecols,
                             dtype = 'str',
                             )
        df_a.columns = df_a.columns.str.upper()

        # Append to df
        df.append(df_a)
        
        # Delete DataFrame
        del df_a

    # Concatenate Files and Drop Duplicates
    df = pd.concat(df)
    df = df.drop_duplicates()

    # Save
    df.to_csv(f'{data_folder}/zip_tract_crosswalk_{first_year}-{last_year}.csv',
              index = False,
              sep = '|',
              )
    df.to_stata(f'{data_folder}/zip_tract_crosswalk_{first_year}-{last_year}.dta',
                write_index = False,
                version = 118,
                )

    # Round and save
    df['ZIPSHORT'] = [x[0:3]+'00' for x in df['ZIP']]
    df = df[['ZIPSHORT','TRACT']].drop_duplicates()
    df.to_csv(f'{data_folder}/zip_tract_crosswalk_rounded_{first_year}-{last_year}.csv',
              index = False,
              sep = '|',
              )
    df.to_stata(f'{data_folder}/zip_tract_crosswalk_rounded_{first_year}-{last_year}.dta',
                write_index = False,
                version = 118,
                )

## Main Routine
if __name__ == '__main__' :

    # Combine Crosswalks
    data_folder = '/project/cl/external_data/census_tract_zipcode_crosswalks'
    # combine_crosswalks(data_folder, first_year = 2010, last_year = 2011)
    # combine_crosswalks(data_folder, first_year = 2012, last_year = 2022)
    combine_crosswalks(data_folder, first_year = 2010, last_year = 2022)
    # combine_crosswalks(data_folder, first_year = 2023, last_year = 2023)
