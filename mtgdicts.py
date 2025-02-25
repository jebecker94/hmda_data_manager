#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Apr 18 11:11:58 2023
@author: jebecker3
"""

# Import Packages
import glob
import os
import pandas as pd
import pyarrow as pa
import numpy as np

# Get Column Names and Data Types
def get_column_names_and_data_types(dictionary_file) :
    """
    Reads CoreLogic variable description file and returns list of column names
    and data types compatible with Pandas command "read_csv".

    Parameters
    ----------
    file : string
        File path of the CoreLogic dictionary containing field names, data
        types, and descriptions

    Returns
    -------
    usecols : list
        List of field names to read in .
    dtype : dict
        Data types associated with fields in usecols.
    schema : pyarrow schema
        Data types for use with Apache PyArrow.

    """

    # Get Header Line
    if '_v' in os.path.basename(dictionary_file) :
        header_line = 5
    else :
        header_line = 0

    # Get data descriptions and drop header lines
    df = pd.read_excel(dictionary_file,
                       sheet_name = 0,
                       header = header_line,
                       )
    df = df[pd.to_numeric(df['FIELD #'], errors='coerce').notnull()]

    # Field names as usecols
    usecols = df['FIELD NAME'].values.tolist()

    # Convert data types to dictionary
    df['dtype'] = 'str'
    df.loc[df['TYPE'] == 'Numeric', 'dtype'] = np.float64
    df.loc[(df['TYPE'] == 'Numeric') & np.isnan(df['DECIMAL']), 'dtype'] = 'Int16'
    df.loc[(df['TYPE'] == 'Numeric') & np.isnan(df['DECIMAL']) & (df['MAX LENGTH'] > 4), 'dtype'] = 'Int32'
    df.loc[(df['TYPE'] == 'Numeric') & np.isnan(df['DECIMAL']) & (df['MAX LENGTH'] > 9), 'dtype'] = 'Int64'
    dtype = pd.DataFrame(df['dtype'].values.tolist(),
                         index=usecols,
                         ).to_dict()[0]

    # Convert Data Types to Schema
    schema = []
    for _,row in df.iterrows() :
        pa_type = pa.string()
        if row['TYPE'] == 'Numeric':
            pa_type = pa.float64()
        if (row['TYPE'] == 'Numeric') & (np.isnan(row['DECIMAL'])):
            pa_type = pa.int16()
        if (row['TYPE'] == 'Numeric') & (np.isnan(row['DECIMAL'])) & (row['MAX LENGTH'] > 4):
            pa_type = pa.int32()
        if (row['TYPE'] == 'Numeric') & (np.isnan(row['DECIMAL'])) & (row['MAX LENGTH'] > 9):
            pa_type = pa.int64()
        schema.append((row['FIELD NAME'], pa_type))
    schema = pa.schema(schema)

    # Return variables
    return usecols, dtype, schema

# Corelogic Dictionaries
class CoreLogicDictionary():
    """
    Contains dictionary information for all CoreLogic file types.
    """

    # Initialize
    def __init__(self, dictionary_folder):

        # Verify Dictionary Folder Exists
        if not os.path.exists(dictionary_folder) :
            raise Exception("CoreLogic dictionary folder does not exist.")

        # Set Dictionary Folder
        self.dictionary_folder = dictionary_folder

        # Get Dictionary Files and Drop Temporary Files
        files = glob.glob(f'{dictionary_folder}/*.xlsx')
        files = [x for x in files if '$' not in x]

        # Initialize File Type Classes
        for file in files :
            if 'Historical Property' in os.path.basename(file):
                self.historical_property = self._HistoricalPropertyDictionary(file)
            if 'MLS Listings_' in os.path.basename(file):
                self.mls_listings = self._MLSDictionary(file)
            if 'MLS Listings Premium' in os.path.basename(file):
                self.mls_premium = self._MLSPremiumDictionary(file)
            if 'Mortgage' in os.path.basename(file):
                self.mortgage = self._MortgageDictionary(file)
            if 'Owner' in os.path.basename(file):
                self.owner_transfer = self._OwnerTransferDictionary(file)
            if 'Property Basic' in os.path.basename(file):
                self.property_basic = self._PropertyDictionary(file)

    # Historical Property Class
    class _HistoricalPropertyDictionary():

        def __init__(self, dictionary_file):

            # Set Name
            self.file_name = dictionary_file

            # Set Names, Data Types, and Schema
            cols, dtype, schema = get_column_names_and_data_types(dictionary_file)
            self.column_names = cols
            self.data_types = dtype
            self.schema = schema

    # MLS Listings Class
    class _MLSDictionary():

        def __init__(self, dictionary_file):

            # Set Name
            self.file_name = dictionary_file

            # Set Names, Data Types, and Schema
            cols, dtype, schema = get_column_names_and_data_types(dictionary_file)
            self.column_names = cols
            self.data_types = dtype
            self.schema = schema

    # MLS Premium Class
    class _MLSPremiumDictionary():

        def __init__(self, dictionary_file):

            # Set Name
            self.file_name = dictionary_file

            # Set Names, Data Types, and Schema
            cols, dtype, schema = get_column_names_and_data_types(dictionary_file)
            self.column_names = cols
            self.data_types = dtype
            self.schema = schema

    # Mortgage Class
    class _MortgageDictionary():

        def __init__(self, dictionary_file):

            # Set Name
            self.file_name = dictionary_file

            # Set Names, Data Types, and Schema
            cols, dtype, schema = get_column_names_and_data_types(dictionary_file)
            self.column_names = cols
            self.data_types = dtype
            self.schema = schema

    # Owner Transfer Class
    class _OwnerTransferDictionary():

        def __init__(self, dictionary_file):

            # Set Name
            self.file_name = dictionary_file

            # Set Names, Data Types, and Schema
            cols, dtype, schema = get_column_names_and_data_types(dictionary_file)
            self.column_names = cols
            self.data_types = dtype
            self.schema = schema

    # Property Class
    class _PropertyDictionary():

        def __init__(self, dictionary_file):

            # Set Name
            self.file_name = dictionary_file

            # Set Names, Data Types, and Schema
            cols, dtype, schema = get_column_names_and_data_types(dictionary_file)
            self.column_names = cols
            self.data_types = dtype
            self.schema = schema

# FHFA Dictionaries
class FHFADictionary():
    """
    Contains dictionary information for all FHA file types.
    """

    # Initialize
    def __init__(self, ):

        # Verify Dictionary Folder Exists
        self.fhfa = self._FHFADictionary()
        self.fhlb_members = self._FHLBMemberDictionary()
        self.fhlb_loans = self._FHLBAcquisitionDictionary()

    # FHFA Loans Class
    class _FHFADictionary():

        def __init__(self, ):            

            # Set Data Types )Dictionary)
            data_types = {'Property State': 'str',
                          'Property City': 'str',
                          'Property County': 'str',
                          'Property Zip': 'Int32',
                          'Originating Mortgagee': 'str',
                          'Originating Mortgagee Number': 'Int32',
                          'Sponsor Name': 'str',
                          'Sponsor Number': 'Int32',
                          'Down Payment Source': 'str',
                          'Non Profit Number': 'Int64',
                          'Product Type': 'str',
                          'Loan Purpose': 'str',
                          'Property Type': 'str',
                          'Interest Rate': np.float64(),
                          'Mortgage Amount': 'Int64',
                          'Year': 'Int16',
                          'Month': 'Int16',
                          'FHA Index': 'str',
                          }
            self.data_types = data_types

            # Set Schema
            schema = [('Property State', pa.string()),
                      ('Property City', pa.string()),
                      ('Property County', pa.string()),
                      ('Property Zip', pa.int32()),
                      ('Originating Mortgagee', pa.string()),
                      ('Originating Mortgagee Number', pa.int32()),
                      ('Sponsor Name', pa.string()),
                      ('Sponsor Number', pa.int32()),
                      ('Down Payment Source', pa.string()),
                      ('Non Profit Number', pa.int64()),
                      ('Product Type', pa.string()),
                      ('Loan Purpose', pa.string()),
                      ('Property Type', pa.string()),
                      ('Interest Rate', pa.float64()),
                      ('Mortgage Amount', pa.int64()),
                      ('Year', pa.int16()),
                      ('Month', pa.int16()),
                      ('FHA Index', pa.string()),
                      ]
            schema = pa.schema(schema)
            self.schema = schema
            
            # Set Column Names
            self.column_names = list(data_types.keys())

    # FHLB Member Class
    class _FHLBMemberDictionary():

        def __init__(self, ):

            # Set Data Types
            self.data_types = 'Missing'
            self.schema = 'Missing'
            self.column_names = 'Missing'
            
    # HECM Class
    class _FHLBAcquisitionDictionary():

        def __init__(self, ):

            self.data_types = 'Missing'
            self.schema = 'Missing'
            self.column_names = 'Missing'

# FHA Dictionaries
class FHADictionary():
    """
    Contains dictionary information for all FHA file types.
    """

    # Initialize
    def __init__(self, ):

        # Verify Dictionary Folder Exists
        self.single_family = self._SingleFamilyDictionary()
        self.hecm = self._HECMDictionary()

    # Single Family Class
    class _SingleFamilyDictionary():

        def __init__(self, ):            

            # Set Data Types )Dictionary)
            data_types = {'Property State': 'str',
                               'Property City': 'str',
                               'Property County': 'str',
                               'Property Zip': 'Int32',
                               'Originating Mortgagee': 'str',
                               'Originating Mortgagee Number': 'Int32',
                               'Sponsor Name': 'str',
                               'Sponsor Number': 'Int32',
                               'Down Payment Source': 'str',
                               'Non Profit Number': 'Int64',
                               'Product Type': 'str',
                               'Loan Purpose': 'str',
                               'Property Type': 'str',
                               'Interest Rate': np.float64,
                               'Mortgage Amount': 'Int64',
                               'Year': 'Int16',
                               'Month': 'Int16',
                               'FHA Index': 'str',
                               }
            self.data_types = data_types

            # Set Schema
            schema = [('Property State', pa.string()),
                      ('Property City', pa.string()),
                      ('Property County', pa.string()),
                      ('Property Zip', pa.int32()),
                      ('Originating Mortgagee', pa.string()),
                      ('Originating Mortgagee Number', pa.int32()),
                      ('Sponsor Name', pa.string()),
                      ('Sponsor Number', pa.int32()),
                      ('Down Payment Source', pa.string()),
                      ('Non Profit Number', pa.int64()),
                      ('Product Type', pa.string()),
                      ('Loan Purpose', pa.string()),
                      ('Property Type', pa.string()),
                      ('Interest Rate', pa.float64()),
                      ('Mortgage Amount', pa.int64()),
                      ('Year', pa.int16()),
                      ('Month', pa.int16()),
                      ('FHA Index', pa.string()),
                      ]
            schema = pa.schema(schema)
            self.schema = schema
            
            # Set Column Names
            self.column_names = list(data_types.keys())

    # HECM Class
    class _HECMDictionary():

        def __init__(self, ):

            # Set Data Types
            data_types = {'Property State': 'str',
                          'Property City': 'str',
                          'Property County': 'str',
                          'Property Zip': 'Int32',
                          'Originating Mortgagee': 'str',
                          'Originating Mortgagee Number': 'Int32',
                          'Sponsor Name': 'str',
                          'Sponsor Number': 'Int32',
                          'Sponsor Originator': 'str',
                          'NMLS': 'Int64',
                          'Standard/Saver': 'str',
                          'Purchase/Refinance': 'str',
                          'Rate Type': 'str',
                          'Interest Rate': np.float64,
                          'Initial Principal Limit': np.float64,
                          'Maximum Claim Amount': np.float64,
                          'Year': 'Int16',
                          'Month': 'Int16',
                          'HECM Type': 'str',
                          'Current Servicer ID': 'Int64',
                          'Previous Servicer ID': 'Int64',
                          }
            self.data_types = data_types
            
            # Set Schema
            schema = [('Property State', pa.string()),
                      ('Property City', pa.string()),
                      ('Property County', pa.string()),
                      ('Property Zip', pa.int32()),
                      ('Originating Mortgagee', pa.string()),
                      ('Originating Mortgagee Number', pa.int32()),
                      ('Sponsor Name', pa.string()),
                      ('Sponsor Number', pa.int32()),
                      ('Sponsor Originator', pa.string()),
                      ('NMLS', pa.int64()),
                      ('Standard/Saver', pa.string()),
                      ('Purchase/Refinance', pa.string()),
                      ('Rate Type', pa.string()),
                      ('Interest Rate', pa.float64()),
                      ('Initial Principal Limit', pa.float64()),
                      ('Maximum Claim Amount', pa.float64()),
                      ('Year', pa.int16()),
                      ('Month', pa.int16()),
                      ('Current Servicer ID', pa.int64()),
                      ('Previous Servicer ID', pa.int64()),
                      ('FHA Index', pa.string()),
                      ]
            schema = pa.schema(schema)
            self.schema = schema
            
            # Set Column Names
            self.column_names = list(data_types.keys())

# InfoUSA Dictionaries
class InfoUSADictionary():
    """
    Contains dictionary information for all CoreLogic file types.
    """

    # Initialize
    def __init__(self):

        # Set Data Types
        data_types = {'FAMILYID': 'Int64',
                      'DOWNGRADE_REASON_CODE': 'str',
                      'DOWNGRADE_DATE': 'Int32',
                      'RECENCY_DATE': 'Int32',
                      'LOCATION_TYPE': 'str',
                      'PRIMARY_FAMILY_IND': 'Int16',
                      'HOUSEHOLDSTATUS': 'str',
                      'TRADELINE_COUNT': 'Int16',
                      'HEAD_HH_AGE_CODE': 'str',
                      'LENGTH_OF_RESIDENCE': 'Int16',
                      'CHILDRENHHCOUNT': 'Int16',
                      'CHILDREN_IND': 'Int16',
                      'ADDRESSTYPE': 'str',
                      'MAILABILITY_SCORE': 'Int16',
                      'WEALTH_FINDER_SCORE': 'Int32',
                      'FIND_DIV_1000': 'Int32',
                      'OWNER_RENTER_STATUS': 'Int16',
                      'ESTMTD_HOME_VAL_DIV_1000': 'Int64',
                      'MARITAL_STATUS': 'Int16',
                      'PPI_DIV_1000': 'Int32',
                      'MSA2000_CODE': 'Int32',
                      'MSA2000_IDENTIFIER': np.float64(),
                      'CSA2000_CODE': 'Int32',
                      'CBSACODE': 'Int32',
                      'CBSATYPE': np.float64(),
                      'CSACODE': 'Int32',
                      'LOCATIONID': np.float64(),
                      'HOUSE_NUM': 'str',
                      'HOUSE_NUM_FRACTION': 'str',
                      'STREET_PRE_DIR': 'str',
                      'STREET_NAME': 'str',
                      'STREET_POST_DIR': 'str',
                      'STREET_SUFFIX': 'str',
                      'UNIT_TYPE': 'str',
                      'UNIT_NUM': 'str',
                      'BOX_TYPE': 'str',
                      'BOX_NUM': 'str',
                      'ROUTE_TYPE': 'str',
                      'ROUTE_NUM': np.float64(),
                      'CITY': 'str',
                      'STATE': 'str',
                      'ZIP': 'Int32',
                      'ZIP4': 'Int16',
                      'DPBC': 'Int16',
                      'VACANT': 'Int16',
                      'USPSNOSTATS': 'Int16',
                      'GE_LATITUDE_2010': np.float64(),
                      'GE_LONGITUDE_2010': np.float64(),
                      'GE_CENSUS_LEVEL_2010': 'str',
                      'GE_CENSUS_STATE_2010': 'Int16',
                      'GE_CENSUS_COUNTY': 'Int16',
                      'GE_CENSUS_TRACT': 'Int32',
                      'GE_CENSUS_BG': 'Int16',
                      'GE_ALS_COUNTY_CODE_2010': 'Int16',
                      'GE_ALS_CENSUS_TRACT_2010': 'Int32',
                      'GE_ALS_CENSUS_BG_2010': 'Int16',
                      'IndividualID_1': 'Int64',
                      'First_Name_1': 'str',
                      'Middle_Name_1': 'str',
                      'Last_Name_1': 'str',
                      'Suffix_Code_1': 'str',
                      'title_code': 'Int16',
                      'Gender_1': 'str',
                      'Age_1': 'Int16',
                      'Ethnicity_Code_1': 'str',
                      'IndividualID_2': 'Int64',
                      'First_Name_2': 'str',
                      'Middle_Name_2': 'str',
                      'Last_Name_2': 'str',
                      'Suffix_Code_2': 'str',
                      'Gender_2': 'str',
                      'Age_2': 'Int16',
                      'Ethnicity_Code_2': 'str',
                      'IndividualID_3': 'Int64',
                      'First_Name_3': 'str',
                      'Middle_Name_3': 'str',
                      'Last_Name_3': 'str',
                      'Suffix_Code_3': 'str',
                      'Gender_3': 'str',
                      'Age_3': 'Int16',
                      'Ethnicity_Code_3': 'str',
                      'IndividualID_4': 'Int64',
                      'First_Name_4': 'str',
                      'Middle_Name_4': 'str',
                      'Last_Name_4': 'str',
                      'Suffix_Code_4': 'str',
                      'Gender_4': 'str',
                      'Age_4': 'Int16',
                      'Ethnicity_Code_4': 'str',
                      'IndividualID_5': 'Int64',
                      'First_Name_5': 'str',
                      'Middle_Name_5': 'str',
                      'Last_Name_5': 'str',
                      'Suffix_Code_5': 'str',
                      'Gender_5': 'str',
                      'Age_5': 'Int16',
                      'Ethnicity_Code_5': 'str',
                      }
        self.data_types = data_types
        self.column_names = list(data_types.keys())
        
        # Set Schema
        schema = [('FAMILYID', pa.int64()),
                  ('DOWNGRADE_REASON_CODE', pa.string()),
                  ('DOWNGRADE_DATE', pa.int32()),
                  ('RECENCY_DATE', pa.int32()),
                  ('LOCATION_TYPE', pa.string()),
                  ('PRIMARY_FAMILY_IND', pa.int8()),
                  ('HOUSEHOLDSTATUS', pa.string()),
                  ('TRADELINE_COUNT', pa.int8()),
                  ('HEAD_HH_AGE_CODE', pa.string()),
                  ('LENGTH_OF_RESIDENCE', pa.int16()),
                  ('CHILDRENHHCOUNT', pa.int8()),
                  ('CHILDREN_IND', pa.int8()),
                  ('ADDRESSTYPE', pa.string()),
                  ('MAILABILITY_SCORE', pa.int16()),
                  ('WEALTH_FINDER_SCORE', pa.int16()),
                  ('FIND_DIV_1000', pa.int16()),
                  ('OWNER_RENTER_STATUS', pa.int8()),
                  ('ESTMTD_HOME_VAL_DIV_1000', pa.int16()),
                  ('MARITAL_STATUS', pa.int8()),
                  ('PPI_DIV_1000', pa.int16()),
                  ('MSA2000_CODE', pa.int32()),
                  ('MSA2000_IDENTIFIER', pa.int8()),
                  ('CSA2000_CODE', pa.int16()),
                  ('CBSACODE', pa.int32()),
                  ('CBSATYPE', pa.int8()),
                  ('CSACODE', pa.int16()),
                  ('LOCATIONID', pa.int64()),
                  ('HOUSE_NUM', pa.string()),
                  ('HOUSE_NUM_FRACTION', pa.string()),
                  ('STREET_PRE_DIR', pa.string()),
                  ('STREET_NAME', pa.string()),
                  ('STREET_POST_DIR', pa.string()),
                  ('STREET_SUFFIX', pa.string()),
                  ('UNIT_TYPE', pa.string()),
                  ('UNIT_NUM', pa.string()),
                  ('BOX_TYPE', pa.string()),
                  ('BOX_NUM', pa.string()),
                  ('ROUTE_TYPE', pa.string()),
                  ('ROUTE_NUM', pa.string()),
                  ('CITY', pa.string()),
                  ('STATE', pa.string()),
                  ('ZIP', pa.int32()),
                  ('ZIP4', pa.int16()),
                  ('DPBC', pa.int16()),
                  ('VACANT', pa.int8()),
                  ('USPSNOSTATS', pa.int8()),
                  ('GE_LATITUDE_2010', pa.float64()),
                  ('GE_LONGITUDE_2010', pa.float64()),
                  ('GE_CENSUS_LEVEL_2010', pa.string()),
                  ('GE_CENSUS_STATE_2010', pa.int8()),
                  ('GE_CENSUS_COUNTY', pa.int16()),
                  ('GE_CENSUS_TRACT', pa.int32()),
                  ('GE_CENSUS_BG', pa.int8()),
                  ('GE_ALS_COUNTY_CODE_2010', pa.int16()),
                  ('GE_ALS_CENSUS_TRACT_2010', pa.int32()),
                  ('GE_ALS_CENSUS_BG_2010', pa.int8()),
                  ('IndividualID_1', pa.int64()),
                  ('First_Name_1', pa.string()),
                  ('Middle_Name_1', pa.string()),
                  ('Last_Name_1', pa.string()),
                  ('Suffix_Code_1', pa.string()),
                  ('title_code', pa.int8()),
                  ('Gender_1', pa.string()),
                  ('Age_1', pa.int8()),
                  ('Ethnicity_Code_1', pa.string()),
                  ('IndividualID_2', pa.int64()),
                  ('First_Name_2', pa.string()),
                  ('Middle_Name_2', pa.string()),
                  ('Last_Name_2', pa.string()),
                  ('Suffix_Code_2', pa.string()),
                  ('Gender_2', pa.string()),
                  ('Age_2', pa.int8()),
                  ('Ethnicity_Code_2', pa.string()),
                  ('IndividualID_3', pa.int64()),
                  ('First_Name_3', pa.string()),
                  ('Middle_Name_3', pa.string()),
                  ('Last_Name_3', pa.string()),
                  ('Suffix_Code_3', pa.string()),
                  ('Gender_3', pa.string()),
                  ('Age_3', pa.int8()),
                  ('Ethnicity_Code_3', pa.string()),
                  ('IndividualID_4', pa.int64()),
                  ('First_Name_4', pa.string()),
                  ('Middle_Name_4', pa.string()),
                  ('Last_Name_4', pa.string()),
                  ('Suffix_Code_4', pa.string()),
                  ('Gender_4', pa.string()),
                  ('Age_4', pa.int8()),
                  ('Ethnicity_Code_4', pa.string()),
                  ('IndividualID_5', pa.int64()),
                  ('First_Name_5', pa.string()),
                  ('Middle_Name_5', pa.string()),
                  ('Last_Name_5', pa.string()),
                  ('Suffix_Code_5', pa.string()),
                  ('Gender_5', pa.string()),
                  ('Age_5', pa.int8()),
                  ('Ethnicity_Code_5', pa.string()),
                  ]
        schema = pa.schema(schema)
        self.schema = schema

# HMDA Dictionaries
class HMDADictionary():
    """
    Contains dictionary information for all HMDA file types.
    """

    # Initialize
    def __init__(self, ):

        # Get dictionaries for each file type
        self.lar = self._LARDictionary()
        self.ts = self._TSDictionary()
        self.panel = self._PanelDictionary()
        self.msamd = self._MSAMDDictionaries()

    # LAR Class
    class _LARDictionary():

        def __init__(self, ):

            # Set Data Types
            cols = []
            dtype = {}
            schema = pa.schema([])
            
            self.column_names = cols
            self.data_types = dtype
            self.schema = schema

    class _TSDictionary():

        def __init__(self, ):

            # Set Data Types
            cols = []
            dtype = {}
            schema = pa.schema([])

            self.column_names = cols
            self.data_types = dtype
            self.schema = schema

    class _PanelDictionary():

        def __init__(self, ):

            # Set Data Types
            cols = []
            dtype = {}
            schema = pa.schema([])

            self.column_names = cols
            self.data_types = dtype
            self.schema = schema
    
    class _MSAMDDictionaries():

        def __init__(self, ):

            # Set Data Types
            cols = []
            dtype = {}
            schema = pa.schema([])

            self.column_names = cols
            self.data_types = dtype
            self.schema = schema

# Get LAR File Schema
def get_hmda_lar_file_schema(schema_file='./hmda_public_lar_schema.html', schema_type='pyarrow') :
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
    if schema_type not in ['pyarrow','pandas'] :
        raise ValueError('The schema type must be either "pyarrow" or "pandas".')

    # Load the schema file
    df = pd.read_html(schema_file)[0]

    # Convert the schema to a PyArrow schema
    if schema_type == 'pyarrow' :
        schema = []
        for _,row in df.iterrows() :
            pa_type = pa.string()
            if (row['Type'] == 'Numeric') & (row['Max Length'] <= 4):
                pa_type = pa.int16()
            if (row['Type'] == 'Numeric') & (row['Max Length'] > 4):
                pa_type = pa.int32()
            if (row['Type'] == 'Numeric') & (row['Max Length'] > 9):
                pa_type = pa.int64()
            schema.append((row['Field'], pa_type))
        schema = pa.schema(schema)

    # Convert the schema to a Pandas schema
    elif schema_type == 'pandas' :
        schema = {}
        for _,row in df.iterrows() :
            pd_type = 'str'
            if (row['Type'] == 'Numeric') & (row['Max Length'] <= 4):
                pd_type = 'Int16'
            if (row['Type'] == 'Numeric') & (row['Max Length'] > 4):
                pd_type = 'Int32'
            if (row['Type'] == 'Numeric') & (row['Max Length'] > 9):
                pd_type = 'Int64'
            schema[row['Field']] = pd_type

    # Return the schema
    return schema