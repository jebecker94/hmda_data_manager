#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Apr 18 11:11:58 2023
@author: jebecker3
"""

# Import Packages
import pandas as pd
import pyarrow as pa
import numpy as np

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
