#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Apr 18 11:11:58 2023
@author: jebecker3
"""

# Import Packages
import pandas as pd
import pyarrow as pa

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
