#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Feb 11 10:45:24 2023
Last updated on: Sat Feb 11 10:45:24 2023
@author: Jonathan E. Becker (jebecker3@wisc.edu)
"""

# Import Packages
import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import HMDALoader
import config

#%% Local Functions
# Get Match Columns
def get_match_columns(file) :
    """
    Get columns used for matching.

    Parameters
    ----------
    file : str
        File to load for columns.

    Returns
    -------
    columns : list
        Columns used for matching.

    """

    # Load File Column Names
    columns = pq.read_metadata(file).schema.names

    # Drop Columns Not Used in Match
    drop_columns = [
        'denial_reason',
        'denial_reason_1',
        'denial_reason_2',
        'denial_reason_3',
        'denial_reason_4',
        'aus',
        'aus_1',
        'aus_2',
        'aus_3',
        'aus_4',
        'aus_5',
        'applicant_credit_score_type',
        'co_applicant_credit_score_type',
        'initially_payable_to_institution',
        'submission_of_application',
        'tract_population',
        'tract_minority_population_percent',
        'ffiec_msa_md_median_family_income',
        'tract_to_msa_income_percentage',
        'tract_owner_occupied_units',
        'tract_one_to_four_family_homes',
        'tract_median_age_of_housing_units',
        'derived_loan_product_type',
        'derived_dwelling_category',
        'derived_ethnicity',
        'derived_race',
        'derived_sex',
    ]
    columns = [x for x in columns if x not in drop_columns]
    
    # Return Columns
    return columns

# Load HMDA Data
def load_data(data_folder, min_year=2018, max_year=2023, added_filters=[]) :
    """
    Combine HMDA data after 2018, keeping originations and purchases only. For
    use primarily in matching after the first round.

    Parameters
    ----------
    data_folder : str
        Folder where HMDA data files are located.
    min_year : int, optional
        Minimum year of data to include (inclusive). The default is 2018.
    max_year : int, optional
        Maximum year of data to include (inclusive). The default is 2023.

    Returns
    -------
    df : pandas DataFrame
        Combined HMDA data.

    """

    # Set Filters
    hmda_filters = [('action_taken','in',[1,6])]
    hmda_filters += added_filters

    # Combine Seller and Purchaser Data
    df = []
    for year in range(min_year, max_year+1) :
        file = HMDALoader.get_hmda_files(data_folder, min_year=year, max_year=year, extension='parquet')[0]
        hmda_columns = get_match_columns(file)
        df_a = pd.read_parquet(file, columns=hmda_columns, filters=hmda_filters)
        df_a = df_a.query('purchaser_type not in [1,2,3,4] | action_taken == 6')
        df.append(df_a)
        del df_a
    df = pd.concat(df)

    # Return DataFrame
    return df

# Replace Missing Values
def replace_missing_values(df) :
    """
    Replace missing numerics with NoneTypes.

    Parameters
    ----------
    df : pandas DataFrame
        Data with numerics for missing values.

    Returns
    -------
    df : pandas DataFrame
        Data with NoneTypes for missing values.

    """

    # Note Loans Exempt from Fee Reporting
    df['i_ExemptFromFeesStrict'] = (df['total_loan_costs'] == 1111) & (df['total_points_and_fees'] == 1111) & (df['origination_charges'] == 1111) & (df['discount_points'] == 1111) & (df['lender_credits'] == 1111)
    df['i_ExemptFromFeesWeak'] = (df['total_loan_costs'] == 1111) | (df['total_points_and_fees'] == 1111) | (df['origination_charges'] == 1111) | (df['discount_points'] == 1111) | (df['lender_credits'] == 1111)

    # Columns to replace missing values
    replace_columns = ['conforming_loan_limit',
                       'construction_method',
                       'income',
                       'total_units',
                       'lien_status',
                       'multifamily_affordable_units',
                       'total_loan_costs',
                       'total_points_and_fees',
                       'discount_points',
                       'lender_credits',
                       'origination_charges',
                       'interest_rate',
                       'intro_rate_period',
                       'loan_term',
                       'property_value',
                       'balloon_payment',
                       'interest_only_payment',
                       'negative_amortization',
                       'open_end_line_of_credit',
                       'other_nonamortizing_features',
                       'prepayment_penalty_term',
                       'reverse_mortgage',
                       'business_or_commercial_purpose',
                       'manufactured_home_land_property_',
                       'manufactured_home_secured_proper',
                       ]

    # Rplace Missing Values
    for col in replace_columns :
        if col in df.columns :
            df.loc[df[col].isin([-1111,1111,99999,-99999]), col] = None
            df.loc[df[col] <= 0, col] = None

    # Replace Weird Introductory Rate Periods
    df.loc[df['intro_rate_period'] == df['loan_term'], 'intro_rate_period'] = None

    # Return DataFrame
    return df

# Convert Numerics
def convert_numerics(df) :
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

    # Replace Exempt w/ -99999
    exempt_cols = ['combined_loan_to_value_ratio',
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
    for col in exempt_cols :
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
    numeric_columns = [
        'activity_year',
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
            df[numeric_column] = pd.to_numeric(df[numeric_column], errors='coerce')

    # Return DataFrame
    return df

# Keep Only Observations with Potential Matches on Match Columns
def keep_potential_matches(df, match_columns) :
    """
    Before splitting, keep only loans which have at least one candidate match.

    Parameters
    ----------
    df : pandas DataFrame
        Data.
    match_columns : list
        Columns for match.

    Returns
    -------
    df : pandas DataFrame
        Data with only observations that have potential match.

    """

    # Keep Potential Matches Based on County or Census Tract
    df['i_HasPurchase'] = df.groupby(match_columns)['action_taken'].transform(lambda x: max(x == 6))
    df['i_HasSale'] = df.groupby(match_columns)['action_taken'].transform(lambda x: max(x == 1))

    # Keep Loans With Potential Matches and Drop Potential Match Indicators
    df = df.query('i_HasSale and i_HasPurchase')

    # Drop Dummies
    df = df.drop(columns = ['i_HasSale', 'i_HasPurchase'])

    # Return DataFrame
    return df

# Split Sellers and Purchasers
def split_sellers_and_purchasers(df, crosswalk_folder, match_round=1, file_suffix=None) :
    """
    Split data into sellers and purchasers.

    Parameters
    ----------
    df : TYPE
        DESCRIPTION.
    cw : TYPE, optional
        DESCRIPTION. The default is None.

    Returns
    -------
    df_seller : TYPE
        DESCRIPTION.
    df_purchaser : TYPE
        DESCRIPTION.

    """

    # If Crosswalk is Provided, Drop Existing Matches
    if match_round > 1 :

        # Load Crosswalk
        cw = pq.read_table(f'{crosswalk_folder}/hmda_seller_purchaser_matches_round{match_round-1}{file_suffix}.parquet')

        # Drop Sellers and Purchasers Already Matched
        df = pa.Table.from_pandas(df, preserve_index=False, safe=False)
        df = df.join(cw, keys=['HMDAIndex'], right_keys=['HMDAIndex_s'], join_type='left anti')
        df = df.join(cw, keys=['HMDAIndex'], right_keys=['HMDAIndex_p'], join_type='left anti')
        df = df.to_pandas()
        
    # Separate Out by Action Taken
    df_purchaser = df.query('action_taken == 6')
    df = df.query('action_taken == 1')
    df_seller = df

    # Return Sellers and Purchasers
    return df_seller, df_purchaser

# Match Sex
def match_sex(df) :
    """
    Match on Applicant and Co-applicant Sex.

    Parameters
    ----------
    df : pandas DataFrame
        DESCRIPTION.

    Returns
    -------
    df : pandas DataFrame
        DESCRIPTION.

    """
    
    # Replace Mismatches on Applicant and Co-Applicant Sex
    for sex_column in ['applicant_sex', 'co_applicant_sex'] :
        indexDrop = df[(df[f'{sex_column}_s'] == 1) & df[f'{sex_column}_p'].isin([2, 3, 5, 6])].index
        df = df.drop(indexDrop)
        indexDrop = df[(df[f'{sex_column}_s'] == 2) & df[f'{sex_column}_p'].isin([1, 3, 5, 6])].index
        df = df.drop(indexDrop)
        indexDrop = df[(df[f'{sex_column}_s'] == 3) & df[f'{sex_column}_p'].isin([1, 2, 5, 6])].index
        df = df.drop(indexDrop)
        indexDrop = df[(df[f'{sex_column}_s'] == 5) & df[f'{sex_column}_p'].isin([1, 2, 3, 6])].index
        df = df.drop(indexDrop)
        indexDrop = df[(df[f'{sex_column}_s'] == 6) & df[f'{sex_column}_p'].isin([1, 2, 3, 5])].index
        df = df.drop(indexDrop)

    # Return Matched DataFrame
    return df

# Match Age
def match_age(df) :
    """
    Match on Applicant and Co-applicant Age.

    Parameters
    ----------
    df : pandas DataFrame
        Data with unmatched ages.

    Returns
    -------
    df : pandas DataFrame
        Data with matched ages.

    """
    
    # Replace Mismatches on Applicant and Co-Applicant Sex
    df = df.query('applicant_age_s == applicant_age_p or applicant_age_s in [8888,9999] or applicant_age_p in [8888,9999]')
    df = df.query('co_applicant_age_s == co_applicant_age_p or co_applicant_age_s in [8888,9999] or co_applicant_age_p in [8888,9999]')

    # Co Applicant Age
    df = df.query('co_applicant_age_s != 9999 or co_applicant_age_p in [8888,9999]')
    df = df.query('co_applicant_age_p != 9999 or co_applicant_age_s in [8888,9999]')

    # Return Matched DataFrame
    return df

# Match Race
def match_race(df, strict = False) :
    """
    Perform race matches

    Parameters
    ----------
    df : pandas DataFrame
        Data with unmatched races.

    Returns
    -------
    df : pandas DataFrame
        Data with matched races.

    """

    # Replace Race Subcategories
    for race_column in ['applicant_race', 'co_applicant_race'] :
        for race_number in range(1, 5+1) :
            df.loc[df[f'{race_column}_{race_number}_s'].isin([21, 22, 23, 24, 25, 26, 27]), f'{race_column}_{race_number}_s'] = 2
            df.loc[df[f'{race_column}_{race_number}_p'].isin([21, 22, 23, 24, 25, 26, 27]), f'{race_column}_{race_number}_p'] = 2
            df.loc[df[f'{race_column}_{race_number}_s'].isin([41, 42, 43, 44]), f'{race_column}_{race_number}_s'] = 4
            df.loc[df[f'{race_column}_{race_number}_p'].isin([41, 42, 43, 44]), f'{race_column}_{race_number}_p'] = 4

    # Applicant Race Match
    for race_number in range(1, 6+1) :
        df = df.query(f'applicant_race_1_s != {race_number} or applicant_race_1_p in [{race_number}, 7, 8] or applicant_race_2_p == {race_number} or applicant_race_3_p == {race_number} or applicant_race_4_p == {race_number} or applicant_race_5_p == {race_number}')
        df = df.query(f'applicant_race_1_p != {race_number} or applicant_race_1_s in [{race_number}, 7, 8] or applicant_race_2_s == {race_number} or applicant_race_3_s == {race_number} or applicant_race_4_s == {race_number} or applicant_race_5_s == {race_number}')

    # Co-Applicant Race Match
    df = df.query('co_applicant_race_1_s != 8 or co_applicant_race_1_p in [7, 8]')
    df = df.query('co_applicant_race_1_p != 8 or co_applicant_race_1_s in [7, 8]')
    for race_number in range(1, 6+1) :
        df = df.query(f'co_applicant_race_1_s != {race_number} or co_applicant_race_1_p in [{race_number}, 7, 8] or co_applicant_race_2_p == {race_number} or co_applicant_race_3_p == {race_number} or co_applicant_race_4_p == {race_number} or co_applicant_race_5_p == {race_number}')
        df = df.query(f'co_applicant_race_1_p != {race_number} or co_applicant_race_1_s in [{race_number}, 7, 8] or co_applicant_race_2_s == {race_number} or co_applicant_race_3_s == {race_number} or co_applicant_race_4_s == {race_number} or co_applicant_race_5_s == {race_number}')

    # Strict Race Matches
    if strict :
        df = df.query('applicant_race_1_s == applicant_race_1_p or applicant_race_1_s in [7,8] or applicant_race_1_p in [7,8]')
        df = df.query('co_applicant_race_1_s == co_applicant_race_1_p or co_applicant_race_1_s in [7,8] or co_applicant_race_1_p in [7,8]')

    # Return Matched DataFrame
    return df

# Match Ethnicity
def match_ethnicity(df, strict = False) :
    """
    Perform ethnicity matches

    Parameters
    ----------
    df : pandas DataFrame
        Data with unmatched ethnicities.

    Returns
    -------
    df : pandas DataFrame
        Data with matched ethnicities.

    """

    # Replace Race Subcategories
    for ethnicity_column in ['applicant_ethnicity', 'co_applicant_ethnicity'] :
        for ethnicity_number in range(1, 5+1) :
            df.loc[df[f'{ethnicity_column}_{ethnicity_number}_s'].isin([11, 12, 13, 14]), f'{ethnicity_column}_{ethnicity_number}_s'] = 1
            df.loc[df[f'{ethnicity_column}_{ethnicity_number}_p'].isin([11, 12, 13, 14]), f'{ethnicity_column}_{ethnicity_number}_p'] = 1
        
    # Applicant Race Match
    for ethnicity_number in range(1, 3+1) :
        df = df.query(f'applicant_ethnicity_1_s != {ethnicity_number} or applicant_ethnicity_1_p in [{ethnicity_number}, 4, 5] or applicant_ethnicity_2_p == {ethnicity_number} or applicant_ethnicity_3_p == {ethnicity_number} or applicant_ethnicity_4_p == {ethnicity_number} or applicant_ethnicity_5_p == {ethnicity_number}')
        df = df.query(f'applicant_ethnicity_1_p != {ethnicity_number} or applicant_ethnicity_1_s in [{ethnicity_number}, 4, 5] or applicant_ethnicity_2_s == {ethnicity_number} or applicant_ethnicity_3_s == {ethnicity_number} or applicant_ethnicity_4_s == {ethnicity_number} or applicant_ethnicity_5_s == {ethnicity_number}')

    # Co-Applicant Race Match
    df = df.query('co_applicant_ethnicity_1_s != 5 or co_applicant_ethnicity_1_p in [4, 5]')
    df = df.query('co_applicant_ethnicity_1_p != 5 or co_applicant_ethnicity_1_s in [4, 5]')
    for ethnicity_number in range(1, 3+1) :
        df = df.query(f'co_applicant_ethnicity_1_s != {ethnicity_number} or co_applicant_ethnicity_1_p in [{ethnicity_number}, 4, 5] or co_applicant_ethnicity_2_p == {ethnicity_number} or co_applicant_ethnicity_3_p == {ethnicity_number} or co_applicant_ethnicity_4_p == {ethnicity_number} or co_applicant_ethnicity_5_p == {ethnicity_number}')
        df = df.query(f'co_applicant_ethnicity_1_p != {ethnicity_number} or co_applicant_ethnicity_1_s in [{ethnicity_number}, 4, 5] or co_applicant_ethnicity_2_s == {ethnicity_number} or co_applicant_ethnicity_3_s == {ethnicity_number} or co_applicant_ethnicity_4_s == {ethnicity_number} or co_applicant_ethnicity_5_s == {ethnicity_number}')

    # Replace Mismatches on Applicant and Co-Applicant Sex
    if strict :
        df = df.query('applicant_ethnicity_1_s == applicant_ethnicity_1_p or applicant_ethnicity_1_s in [4] or applicant_ethnicity_1_p in [4]')
        df = df.query('co_applicant_ethnicity_1_s == co_applicant_ethnicity_1_p or co_applicant_ethnicity_1_s in [4] or co_applicant_ethnicity_1_p in [4]')

    # Return Matched DataFrame
    return df

# Perform Numeric Matches
def perform_income_matches(df) :
    """
    Matches with alternative income variables.

    Parameters
    ----------
    df : pandas DataFrame
        Data before income differences are removed.

    Returns
    -------
    df : ppandas DataFrame
        Data after income differences are removed.

    """

    # Alternative Income Variables
    df['income_fix_s'] = df['income_s']/1000
    df.loc[df['income_s'] > 10000, 'income_fix_s'] = None
    df['income_fix_p'] = df['income_p']/1000
    df.loc[df['income_p'] > 10000, 'income_fix_p'] = None

    #
    df['income_diff'] = df['income_s'] - df['income_p']
    df['income_diff_fix_1'] = df['income_fix_s'] - df['income_p']
    df['income_diff_fix_2'] = df['income_s'] - df['income_fix_p']
    # gen i_ExactIncomeMatch = abs(income_diff) == 0 | abs(income_diff_fix_1) < 1 | abs(income_diff_fix_2) < 1 if ~missing(income_diff)

    # Drop New Columns
    df = df.drop(columns = ['income_fix_s','income_fix_p','income_diff','income_diff_fix_1','income_diff_fix_2'])

    # Return DataFrame
    return df

# Numeric Matches
def numeric_matches(df, match_tolerances, verbose = False, drop_differences = True) :
    """
    Matches for numeric columns.

    Parameters
    ----------
    df : pandas DataFrame
        Data.
    match_tolerances : dictionary
        Dictionary of match columns and tolerances.
    verbose : boolean, optional
        Whether to display number of dropped observations. The default is False.
    drop_differences : boolean, optional
        Whether to drop the created value differences. The default is True.

    Returns
    -------
    df : pandas DataFrame
        Data.

    """

    # Drop One Column at a Time
    for column,tolerance in match_tolerances.items() :

        if (f'{column}_s' in df.columns) and (f'{column}_p' in df.columns) :

            # Count for Dropped Observations
            start_obs = df.shape[0]

            # Compute Numeric Differences
            df[f'{column}_difference'] = df[f'{column}_s'] - df[f'{column}_p']

            # Drop Large Numeric Differences
            # df = df.loc[~(np.abs(df[f'{column}_difference']) > tolerance)]
            df = df.query(f'abs({column}_difference) <= {tolerance} or {column}_difference.isnull()')

            # Display Progress
            if verbose :
                print('Matching on', column, 'drops',  start_obs-df.shape[0], 'observations')

            # Drop Difference Columns
            if drop_differences :
                df = df.drop(columns = [f'{column}_difference'])

    # Return DataFrame
    return df

# Numeric Matches
def weak_numeric_matches(df, match_tolerances, verbose = False, drop_differences = True) :
    """
    Matches for numeric columns.

    Parameters
    ----------
    df : pandas DataFrame
        Data.
    match_tolerances : dictionary
        Dictionary of match columns and tolerances.
    verbose : boolean, optional
        Whether to display number of dropped observations. The default is False.
    drop_differences : boolean, optional
        Whether to drop the created value differences. The default is True.

    Returns
    -------
    df : pandas DataFrame
        Data.

    """

    # Drop One Column at a Time
    for column,tolerance in match_tolerances.items() :
        
        if (f'{column}_s' in df.columns) and (f'{column}_p' in df.columns) :

            # Count for Dropped Observations
            start_obs = df.shape[0]

            # Compute Numeric Differences
            df[f'{column}_difference'] = df[f'{column}_s'] - df[f'{column}_p']

            # Compute Minimum Differences
            df['temp'] = np.abs(df[f'{column}_difference'])
            df[f'min_{column}_difference_s'] = df.groupby(['HMDAIndex_s'])['temp'].transform('min')
            df[f'min_{column}_difference_p'] = df.groupby(['HMDAIndex_p'])['temp'].transform('min')
            df = df.drop(columns = ['temp'])

            # Drop Large Numeric Differences
            df = df.loc[~(np.abs(df[f'{column}_difference']) > tolerance) | (df[f'min_{column}_difference_s'] > 0) | pd.isna(df[f'min_{column}_difference_s'])]
            df = df.loc[~(np.abs(df[f'{column}_difference']) > tolerance) | (df[f'min_{column}_difference_p'] > 0) | pd.isna(df[f'min_{column}_difference_p'])]

            # Display Progress
            if verbose :
                print('Matching weakly on', column, 'drops',  start_obs-df.shape[0], 'observations')

            # Drop Difference Columns
            if drop_differences :
                df = df.drop(columns = [f'{column}_difference',f'min_{column}_difference_s',f'min_{column}_difference_p'], errors='ignore')

    return df

# Numeric Matches after Uniques
def numeric_matches_post_unique(df, match_tolerances, verbose = False, drop_differences = True) :
    """
    Matches for numeric columns.

    Parameters
    ----------
    df : pandas DataFrame
        Data.
    match_tolerances : dictionary
        Dictionary of match columns and tolerances.
    verbose : boolean, optional
        Whether to display number of dropped observations. The default is False.
    drop_differences : boolean, optional
        Whether to drop the created value differences. The default is True.

    Returns
    -------
    df : pandas DataFrame
        Data.

    """
    
    # Count for Dropped Observations
    start_obs = df.shape[0]
    
    df['i_DropObservation'] = 0

    # Drop One Column at a Time
    for column,tolerance in match_tolerances.items() :

        # Compute Numeric Differences
        df[f'{column}_difference'] = df[f'{column}_s'] - df[f'{column}_p']

        # Drop Large Numeric Differences
        df.loc[np.abs(df[f'{column}_difference']) > tolerance, 'i_DropObservation'] = 1

        # Drop Difference Columns
        if drop_differences :
            df = df.drop(columns = [f'{column}_difference'])

    # Drop All Sold Loans When One Bad Match Exists
    df['i_DropSale'] = df.groupby(['HMDAIndex_s'])['i_DropObservation'].transform('max')
    df = df.query('i_DropSale == 0')
    df = df.drop(columns = ['i_DropObservation','i_DropSale'])

    # Display Progress
    if verbose :
        print('Matching on', column, 'drops',  start_obs-df.shape[0], 'observations')

    # Return DataFrame
    return df

# Perform Fee Matches
def perform_fee_matches(df) :
    """
    Count the number of fee variables with nonmissing values and matches.

    Parameters
    ----------
    df : pandas DataFrame
        Data without match or nonmissing counts.

    Returns
    -------
    df : pandas DataFrame
        Data with match and nonmissing counts.

    """

    # Initialize Fee Match Variables
    df['NumberFeeMatches'] = 0 #if i_ExemptFromFeesStrict != 1
    df['NumberNonmissingFees_s'] = 0 #if i_ExemptFromFeesStrict != 1
    df['NumberNonmissingFees_p'] = 0 #if i_ExemptFromFeesStrict != 1

    # Update Fee Match Variables
    for fee_column in ['total_loan_costs', 'total_points_and_fees', 'origination_charges', 'discount_points', 'lender_credits'] :
        df['NumberFeeMatches'] = df['NumberFeeMatches'] + (df[f'{fee_column}_s'] == df[f'{fee_column}_p'])*(df[f'{fee_column}_s'] is not None)
        df['NumberNonmissingFees_s'] = df['NumberNonmissingFees_s'] + ~pd.isna(df[f'{fee_column}_s'])
        df['NumberNonmissingFees_p'] = df['NumberNonmissingFees_p'] + ~pd.isna(df[f'{fee_column}_p'])

    # Generous Fee Match
    df['i_GenerousFeeMatch'] = 0 #if i_ExemptFromFeesStrict != 1
    for var1 in ['total_loan_costs', 'total_points_and_fees', 'origination_charges', 'discount_points', 'lender_credits'] :
    	for var2 in ['total_loan_costs', 'total_points_and_fees', 'origination_charges', 'discount_points', 'lender_credits'] :
            df.loc[((df[f'{var1}_s'] == df[f'{var2}_p']) & ~pd.isna(df[f'{var1}_s'])), 'i_GenerousFeeMatch'] = 1

    # Return DataFrame
    return df

# Keep Uniques
def keep_uniques(df, one_to_one = True, verbose = True) :
    """
    Keep unique matches or matches where a single origination matches to many
    purchasers where one purchaser has a secondary sale.

    Parameters
    ----------
    df : pandas DataFrame
        Data before unique matches are enforced.
    one_to_one : Boolean, optional
        Whether to only keep unique seller matches. The default is True.
    verbose : Boolean, optional
        Whether to display match counts before dropping. The default is True.

    Returns
    -------
    df : pandas DataFrame
        Data after unique matches are enforced.

    """

    # Keep Unique Loans
    df['count_index_s'] = df.groupby(['HMDAIndex_s'])['HMDAIndex_s'].transform('count')
    df['count_index_p'] = df.groupby(['HMDAIndex_p'])['HMDAIndex_p'].transform('count')

    # Display
    if verbose :
        print(df[['count_index_s','count_index_p']].value_counts())

    # Keep Purchased Loans w/ Unique Match
    df = df.query('count_index_p == 1')

    # Keep Uniques
    if one_to_one :
        df = df.query('count_index_s == 1')

    # Keep Loans Where Sale Matches Multiple Purchases if One Known to Be Secondary Sale
    else :
        
        # Keep Unique Loans
        df['temp'] = 1*(df['purchaser_type_p'] > 4)
        df['i_LoanHasSecondarySale'] = df.groupby(['HMDAIndex_s'])['temp'].transform('max')
        df = df.query('count_index_s == 1 or (count_index_s == 2 & i_LoanHasSecondarySale == 1)')
        df = df.drop(columns = ['i_LoanHasSecondarySale'])

    # Drop Index Counts
    df = df.drop(columns = ['count_index_s','count_index_p'])

    # Return DataFrame
    return df

# Save Crosswalk
def save_crosswalk(df, save_folder, match_round = 1, file_suffix=None) :
    """
    Create and save a crosswalk from the data.

    Parameters
    ----------
    df : TYPE
        DESCRIPTION.
    save_folder : TYPE
        DESCRIPTION.
    match_round : TYPE, optional
        DESCRIPTION. The default is 1.

    Returns
    -------
    None.

    """
    
    # Add Previous Round Crosswalk
    cw = []
    if match_round > 1 :
        cw.append(pq.read_table(f'{save_folder}/hmda_seller_purchaser_matches_round{match_round-1}{file_suffix}.parquet').to_pandas())

    # Extract HMDA Index variables    
    cw_a = df[['HMDAIndex_s','HMDAIndex_p']]

    # Add Match Round Variable
    cw_a['match_round'] = match_round

    # Append and Concatenate
    cw.append(cw_a)
    cw = pd.concat(cw)

    # Save Crosswalk
    print(cw.match_round.value_counts())
    cw = pa.Table.from_pandas(cw, preserve_index=False)
    pq.write_table(cw, f'{save_folder}/hmda_seller_purchaser_matches_round{match_round}{file_suffix}.parquet')

#%% Match Functions
# Post-2018 Match
def match_hmda_sellers_purchasers_round1(data_folder, save_folder, min_year=2018, max_year=2023, file_suffix=None) :
    """
    Match originations and purchases for HMDA data after 2018.

    Parameters
    ----------
    data_folder : str
        Folder where raw HMDA files are stored.
    save_folder : str
        Folder where matches and match candidates will be saved.
    min_year : int, optional
        First year of data to be matched. The default is 2018.
    max_year : int, optional
        Last year of data to be matched. The default is 2023.

    Returns
    -------
    None.

    """

    # Match Year-by-Year
    df = []
    for year in range(min_year, max_year+1) :

        print(year)

        # Load Data
        df_a = load_data(data_folder, min_year=year, max_year=year)

        # Convert Numerics
        df_a = convert_numerics(df_a)

        # Replace Missings
        df_a = replace_missing_values(df_a)

        # Drop Observations with Missing Match Variables
        match_columns = [
            'loan_type',
            'loan_amount',
            'census_tract',
            'occupancy_type',
            'loan_purpose',
        ]
        df_a = df_a.dropna(subset=match_columns)
        df_a = df_a.query('census_tract not in ["","NA"]')

        # Split into Sellers/Purchasers and Merge
        df_a, df_purchaser = split_sellers_and_purchasers(df_a, save_folder)
        df_a = df_a.merge(df_purchaser, on=match_columns, suffixes=('_s','_p'))
        del df_purchaser

        # Keep Close Matches with tolerances
        match_tolerances = {
            'income': 1,
            'interest_rate': .0625,
        }
        df_a = numeric_matches(df_a, match_tolerances, verbose=False)

        # Weak Numeric Matches
        match_tolerances = {'interest_rate': .01}
        df_a = weak_numeric_matches(df_a, match_tolerances, verbose=True)

        # Check for Matches On Any Fee Variables
        df_a = perform_fee_matches(df_a)
        df_a = df_a.query('NumberFeeMatches >= 1 | NumberNonmissingFees_s == 0 | NumberNonmissingFees_p == 0')

        # Keep Unique Matches
        df_a = keep_uniques(df_a)

        # Use Demographics to Trim Matches
        df_a = match_age(df_a)
        df_a = match_sex(df_a)
        df_a = match_race(df_a)
        df_a = match_ethnicity(df_a)

        # Keep Close Matches with tolerances
        match_tolerances = {
            'conforming_loan_limit': 0,
            'construction_method': 0,
            'discount_points': 5,
            'income': 1,
            'interest_rate': .0625,
            'intro_rate_period': 6,
            'lender_credits': 5,
            'lien_status': 0,
            'loan_term': 12,
            'open_end_line_of_credit': 0,
            'origination_charges': 5,
            'property_value': 20000,
            'total_units': 0,
            'applicant_age_above_62': 0,
            'co_applicant_age_above_62': 0,
        }
        df_a = numeric_matches(df_a, match_tolerances, verbose=True)

        # Clean Up
        df_a['i_DropObservation'] = (np.abs(df_a['interest_rate_s'] - df_a['interest_rate_p']) >= .005) | pd.isna(df_a['income_s']) | pd.isna(df_a['interest_rate_s']) | pd.isna(df_a['property_value_s'])
        df_a['i_DropSale'] = df_a.groupby(['HMDAIndex_s'])['i_DropObservation'].transform('max')
        df_a = df_a.query('i_DropSale != 1')

        # Add to Crosswalks
        df.append(df_a)
        del df_a

    # Combine Matches and Save Crosswalk
    df = pd.concat(df)
    save_crosswalk(df, save_folder, match_round=1, file_suffix=file_suffix)

# Round 2: Cross-year matches
def match_hmda_sellers_purchasers_round2(data_folder, save_folder, min_year=2018, max_year=2023, file_suffix=None) :
    """
    Round 2 of seller-purchaser matches for 2018 onward.

    Parameters
    ----------
    save_folder : str
        Folder where crosswalks are saved.

    Returns
    -------
    None.

    """

    # Combine Seller and Purchaser Data
    df = load_data(data_folder, min_year=min_year, max_year=max_year)

    # Replace Missings
    df = replace_missing_values(df)

    # Drop Observations with Missing Match Variables
    match_columns = ['loan_type',
                     'loan_amount',
                     'census_tract',
                     'occupancy_type',
                     'loan_purpose']
    df = df.dropna(subset = match_columns)
    df = df.query('census_tract != ""')

    # Split into Sellers/Purchasers
    df, df_purchaser = split_sellers_and_purchasers(df, save_folder, match_round=2, file_suffix=file_suffix)
    df = df.merge(df_purchaser, on=match_columns, suffixes=('_s','_p'))
    del df_purchaser

    # Year Matches
    df = df.query('activity_year_s <= activity_year_p')

    # Keep Close Matches with tolerances
    match_tolerances = {'income': 1,
                        'interest_rate': .0625,
                        }
    df = numeric_matches(df, match_tolerances, verbose=True)

    # Weak Numeric Matches
    match_tolerances = {'interest_rate': .01}
    df = weak_numeric_matches(df, match_tolerances, verbose=True)

    # Check for Matches On Any Fee Variables
    df = perform_fee_matches(df)
    df = df.query('NumberFeeMatches >= 1 | NumberNonmissingFees_s == 0 | NumberNonmissingFees_p == 0')

    # Keep Unique Matches
    df = keep_uniques(df)

    # Use Demographics to Trim Matches
    df = match_age(df)
    df = match_sex(df)
    df = match_race(df)
    df = match_ethnicity(df)

    # Keep Close Matches with tolerances
    match_tolerances = {'conforming_loan_limit': 0,
                        'construction_method': 0,
                        'discount_points': 5,
                        'income': 1,
                        'interest_rate': .01,
                        'intro_rate_period': 6,
                        'lender_credits': 5,
                        'lien_status': 0,
                        'loan_term': 12,
                        'open_end_line_of_credit': 0,
                        'origination_charges': 5,
                        'property_value': 20000,
                        'total_units': 0,
                        'applicant_age_above_62': 0,
                        'co_applicant_age_above_62': 0,
                        }
    df = numeric_matches(df, match_tolerances, verbose=True)

    # Clean Up
    df['i_DropObservation'] = (np.abs(df['interest_rate_s'] - df['interest_rate_p']) >= .005) | pd.isna(df['income_s']) | pd.isna(df['interest_rate_s']) | pd.isna(df['property_value_s'])
    df['i_DropSale'] = df.groupby(['HMDAIndex_s'])['i_DropObservation'].transform('max')
    df = df.query('i_DropSale != 1')
    df = df.drop(columns=['i_DropObservation','i_DropSale'], errors='ignore')

    # Save Crosswalk
    save_crosswalk(df, save_folder, match_round=2, file_suffix=file_suffix)

# Round 3: Match Across Years w/
def match_hmda_sellers_purchasers_round3(data_folder, save_folder, min_year=2018, max_year=2023, file_suffix=None) :
    """
    Round 3 of seller-purchaser matches for 2018 onward.

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

    # Combine Seller and Purchaser Data
    df = load_data(data_folder, min_year = min_year, max_year = max_year)

    # Replace Missings
    df = replace_missing_values(df)

    # Drop Observations with Missing Match Variables
    match_columns = ['loan_type',
                     'loan_amount',
                     'census_tract',
                     'occupancy_type',
                     'loan_purpose']
    df = df.dropna(subset = match_columns)
    df = df.query('census_tract != ""')

    # Split Sellers and Purchasers
    df, df_purchaser = split_sellers_and_purchasers(df, save_folder, match_round=3, file_suffix=file_suffix)
    df = df.merge(df_purchaser, on=match_columns, suffixes = ('_s','_p'))
    del df_purchaser

    # Year Matches
    df = df.query('activity_year_s <= activity_year_p')

    # Keep Close Matches with tolerances
    match_tolerances = {'income': 1,
                        'interest_rate': .0625,
                        'conforming_loan_limit': 0,
                        'construction_method': 0,
                        'intro_rate_period': 6,
                        'lien_status': 0,
                        'open_end_line_of_credit': 0,
                        'total_units': 0,
                        }
    df = numeric_matches(df, match_tolerances, verbose=True)

    # Age, Sex, Ethnicity, and Race Matches
    df = match_age(df)
    df = match_sex(df)
    df = match_race(df)
    df = match_ethnicity(df)

    # Perform Numeric and Fee Matches
    df = perform_fee_matches(df)
    df = df.query('i_GenerousFeeMatch == 1 | NumberNonmissingFees_s == 0 | NumberNonmissingFees_p == 0')

    # Weak Numeric Matches
    match_tolerances = {'interest_rate': .01}
    df = weak_numeric_matches(df, match_tolerances, verbose=True)

    # Keep Unique Matches
    df = keep_uniques(df, one_to_one=False)

    # Numeric Matches Post Uniques
    match_tolerances = {'income': 1,
                        'interest_rate': .0625,
                        'loan_term': 12,
                        'property_value': 20000,
                        }
    df = numeric_matches_post_unique(df, match_tolerances, verbose=True)

    # Save Crosswalk
    save_crosswalk(df, save_folder, match_round=3, file_suffix=file_suffix)

# Round 4: Match without Loan Purpose Match; Keep Tight Fee/Rate/Income Matches
def match_hmda_sellers_purchasers_round4(data_folder, save_folder, min_year=2018, max_year=2023, verbose=False, file_suffix=None) :
    """
    Round 4 of seller-purchaser matches for 2018 onward.

    Parameters
    ----------
    data_folder : str
        DESCRIPTION.
    save_folder : str
        DESCRIPTION.
    min_year : int, optional
        DESCRIPTION. The default is 2018.
    max_year : int, optional
        DESCRIPTION. The default is 2023.
    verbose : boolean, optional
        DESCRIPTION. The default is False.

    Returns
    -------
    None.

    """

    # Combine Seller and Purchaser Data
    df = load_data(data_folder, min_year=min_year, max_year=max_year)

    # Replace Missings
    df = replace_missing_values(df)

    # Drop Observations with Missing Match Variables
    df['i_Purchase'] = 1*(df['loan_purpose'] == 1)
    match_columns = ['loan_type',
                     'loan_amount',
                     'census_tract',
                     'occupancy_type',
                     'i_Purchase']
    df = df.dropna(subset=match_columns)
    df = df.query('census_tract != ""')

    #
    df, df_purchaser = split_sellers_and_purchasers(df, save_folder, match_round=4, file_suffix=file_suffix)
    df = df.merge(df_purchaser, on=match_columns, suffixes=('_s','_p'))
    del df_purchaser

    # Year Matches
    df = df.query('activity_year_s <= activity_year_p')

    # Keep Close Matches with tolerances
    match_tolerances = {'income': 1,
                        'interest_rate': .0625,
                        'conforming_loan_limit': 0,
                        'construction_method': 0,
                        'intro_rate_period': 6,
                        'lien_status': 0,
                        'open_end_line_of_credit': 0,
                        'total_units': 0,
                        }
    df = numeric_matches(df, match_tolerances, verbose=True)

    # Allow Non-matching refi types
    df = df.query('loan_purpose_s == loan_purpose_p | loan_purpose_s in [31,32] | loan_purpose_p in [31,32]')

    # Age, Sex, Ethnicity, and Race Matches
    df = match_age(df)
    df = match_sex(df)
    df = match_race(df)
    df = match_ethnicity(df)

    # Perform Numeric and Fee Matches
    df = perform_fee_matches(df)
    df = df.query('i_GenerousFeeMatch == 1 | NumberNonmissingFees_s == 0 | NumberNonmissingFees_p == 0')

    # Weak Numeric Matches
    match_tolerances = {'interest_rate': .01}
    df = weak_numeric_matches(df, match_tolerances, verbose=True)

    # Keep Unique Matches
    df = keep_uniques(df, one_to_one=False)

    # Numeric Matches Post Uniques
    match_tolerances = {'income': 1,
                        'interest_rate': .0625,
                        'loan_term': 12,
                        'property_value': 20000,
                        }
    df = numeric_matches_post_unique(df, match_tolerances, verbose=True)

    # Save Crosswalk
    save_crosswalk(df, save_folder, match_round=4, file_suffix=file_suffix)
    
# Round 5: Allow for slight loan amount mismatches
def match_hmda_sellers_purchasers_round5(data_folder, save_folder, min_year=2018, max_year=2023, verbose=False, file_suffix=None) :
    """
    Round 5 of seller-purchaser matches for 2018 onward.

    Parameters
    ----------
    data_folder : TYPE
        DESCRIPTION.
    save_folder : TYPE
        DESCRIPTION.
    min_year : TYPE, optional
        DESCRIPTION. The default is 2018.
    max_year : TYPE, optional
        DESCRIPTION. The default is 2022.
    verbose : TYPE, optional
        DESCRIPTION. The default is False.

    Returns
    -------
    None.

    """

    df = []
    for year in range(min_year, max_year+1) :

        # Load HMDA Data
        df_a = load_data(data_folder, min_year=year, max_year=year)

        # Replace Missings
        df_a = replace_missing_values(df_a)

        # Drop Observations with Missing Match Variables
        df_a['i_Purchase'] = 1*(df_a['loan_purpose'] == 1)
        df_a['LoanAmountMatch'] = df_a['loan_amount']
        match_columns = ['loan_type',
                         'census_tract',
                         'occupancy_type',
                         'i_Purchase',
                         'LoanAmountMatch']
        df_a = df_a.dropna(subset = match_columns)
        df_a = df_a.query('census_tract != ""')

        # Split Sold and Purchased Loans
        df_a, df_purchaser = split_sellers_and_purchasers(df_a, save_folder, match_round=5, file_suffix=file_suffix)

        # Create Seller Match Candidates
        df_a1 = df_a.copy()
        df_a1['LoanAmountMatch'] = df_a1['LoanAmountMatch']-10000
        df_a2 = df_a.copy()
        df_a2['LoanAmountMatch'] = df_a2['LoanAmountMatch']+10000
        df_a = pd.concat([df_a1, df_a, df_a2])
        del df_a1, df_a2

        # Match
        df_a = df_a.merge(df_purchaser, on=match_columns, suffixes=('_s','_p'))
        del df_purchaser

        # Keep Close Matches with tolerances
        match_tolerances = {'income': 1,
                            'interest_rate': .0625,
                            }
        df_a = numeric_matches(df_a, match_tolerances, verbose=True)

        # Weak Numeric Matches
        match_tolerances = {'interest_rate': .01}
        df_a = weak_numeric_matches(df_a, match_tolerances, verbose=True)

        # Check for Matches On Any Fee Variables
        df_a = perform_fee_matches(df_a)
        df_a = df_a.query('NumberFeeMatches >= 1 | NumberNonmissingFees_s == 0 | NumberNonmissingFees_p == 0')

        # Keep Unique Matches
        df_a = keep_uniques(df_a)

        # Use Demographics to Trim Matches
        df_a = match_age(df_a)
        df_a = match_sex(df_a)
        df_a = match_race(df_a)
        df_a = match_ethnicity(df_a)

        # Keep Close Matches with tolerances
        match_tolerances = {'conforming_loan_limit': 0,
                            'construction_method': 0,
                            'discount_points': 5,
                            'income': 1,
                            'interest_rate': .0625,
                            'intro_rate_period': 6,
                            'lender_credits': 5,
                            'lien_status': 0,
                            'loan_term': 12,
                            'open_end_line_of_credit': 0,
                            'origination_charges': 5,
                            'property_value': 20000,
                            'total_units': 0,
                            'applicant_age_above_62': 0,
                            'co_applicant_age_above_62': 0,
                            'loan_amount': 10000,
                            }
        df_a = numeric_matches(df_a, match_tolerances, verbose=True)
        
        # Clean Up
        df_a['i_DropObservation'] = (np.abs(df_a['interest_rate_s'] - df_a['interest_rate_p']) >= .005) | pd.isna(df_a['income_s']) | pd.isna(df_a['interest_rate_s']) | pd.isna(df_a['property_value_s'])
        df_a['i_DropSale'] = df_a.groupby(['HMDAIndex_s'])['i_DropObservation'].transform('max')
        df_a = df_a.query('i_DropSale != 1')

        # Add to Crosswalks
        df.append(df_a)
        del df_a

    # Combine Matches
    df = pd.concat(df)

    # Drop Negative Loan Differences
    df['LoanDiff'] = df.loan_amount_s-df.loan_amount_p
    df = df.query('LoanDiff >= 0')
    
    # Drop Secondary Sales
    df = df.query('purchaser_type_p in [0,1,2,3,4]')

    # Save Crosswalk
    save_crosswalk(df, save_folder, match_round=5, file_suffix=file_suffix)

# Round 6: Affiliate Matches
def match_hmda_sellers_purchasers_round6(data_folder, save_folder, min_year=2018, max_year=2023, verbose=False, file_suffix=None) :
    """
    Round 5 of seller-purchaser matches for 2018 onward. Match for affiliates.

    Parameters
    ----------
    data_folder : TYPE
        DESCRIPTION.
    save_folder : TYPE
        DESCRIPTION.
    min_year : TYPE, optional
        DESCRIPTION. The default is 2018.
    max_year : TYPE, optional
        DESCRIPTION. The default is 2022.
    verbose : TYPE, optional
        DESCRIPTION. The default is False.

    Returns
    -------
    None.

    """

    # Get Affiliates
    affiliated_leis = get_affiliates(data_folder, save_folder, 5, min_year=2018, max_year=2023, strict=False, file_suffix=file_suffix)

    # Load HMDA Data
    df = load_data(data_folder, min_year=min_year, max_year=max_year)

    # Keep Only Affiliate Sales among Originations
    df = df.query('action_taken == 6 | purchaser_type == 8')

    # Replace Missings
    df = replace_missing_values(df)

    # Drop Observations with Missing Match Variables
    df['i_Purchase'] = 1*(df['loan_purpose'] == 1)
    match_columns = ['loan_type',
                     'loan_amount',
                     'county_code',
                     'occupancy_type',
                     'i_Purchase']
    df = df.dropna(subset = match_columns)

    # Split Sold and Purchased Loans
    df, df_purchaser = split_sellers_and_purchasers(df, save_folder, match_round=5, file_suffix=file_suffix)
    df = df.merge(df_purchaser, on=match_columns, suffixes=('_s','_p'))
    del df_purchaser

    # Year Matches
    df = df.query('activity_year_s <= activity_year_p')

    # Keep Close Matches with tolerances
    match_tolerances = {'interest_rate': .0625,
                        'conforming_loan_limit': 0,
                        'construction_method': 0,
                        'intro_rate_period': 6,
                        'lien_status': 0,
                        'open_end_line_of_credit': 0,
                        'total_units': 0,
                        }
    df = numeric_matches(df, match_tolerances, verbose = True)

    # Allow Non-matching refi types
    df = df.query('loan_purpose_s == loan_purpose_p or loan_purpose_s in [31,32] or loan_purpose_p in [31,32]')

    # Age, Sex, Ethnicity, and Race Matches
    df = match_age(df)
    df = match_sex(df)
    df = match_race(df)
    df = match_ethnicity(df)

    # Perform Numeric and Fee Matches
    df = perform_fee_matches(df)
    df = df.query('i_GenerousFeeMatch == 1 or NumberNonmissingFees_s == 0 | NumberNonmissingFees_p == 0')

    # Drop Candidate Matches with No Generous Fee Match where Fees have Any Match
    df['i_GoodMatch'] = 1*(df['NumberFeeMatches'] >= 2)
    df['i_SaleHasFeeMatch'] = df.groupby(['HMDAIndex_s'])['i_GoodMatch'].transform('max')
    df['i_PurchaseHasFeeMatch'] = df.groupby(['HMDAIndex_p'])['i_GoodMatch'].transform('max')
    df = df.query('i_GenerousFeeMatch == 1 | i_SaleHasFeeMatch == 0')
    df = df.query('i_GenerousFeeMatch == 1 | i_PurchaseHasFeeMatch == 0')

    # Drop Candidate Matches with No Generous Fee Match where Fees have Any Match
    df['IncomeDifference'] = df['income_s'] - df['income_p']
    df['i_PerfectIncomeMatch'] = df['income_s'] == df['income_p']
    df.loc[pd.isna(df['income_s']) | pd.isna(df['income_p']), 'i_PerfectIncomeMatch'] = None
    df['i_SaleHasIncomeMatch'] = df.groupby(['HMDAIndex_s'])['i_PerfectIncomeMatch'].transform('max')
    df['i_PurchaseHasIncomeMatch'] = df.groupby(['HMDAIndex_p'])['i_PerfectIncomeMatch'].transform('max')
    df = df.query('abs(IncomeDifference) <= 1 | i_SaleHasIncomeMatch != 1')
    df = df.query('abs(IncomeDifference) <= 1 | i_PurchaseHasIncomeMatch != 1')

    # Weak Numeric Matches
    match_tolerances = {'interest_rate': .01}
    df = weak_numeric_matches(df, match_tolerances, verbose=True)

    # Keep Only Affiliate Matches
    df = df.merge(affiliated_leis, on=['lei_s','lei_p'])

    # Keep Unique Matches
    df = keep_uniques(df, one_to_one=False)

    # Numeric Matches Post Uniques
    match_tolerances = {'income': 1,
                        'interest_rate': .0625,
                        'loan_term': 12,
                        'property_value': 20000,
                        }
    df = numeric_matches_post_unique(df, match_tolerances, verbose=True)

    # Save Crosswalk
    save_crosswalk(df, save_folder, match_round=6, file_suffix=file_suffix)

# Round 7: Match with Purchaser Type
def match_hmda_sellers_purchasers_round7(data_folder, save_folder, min_year=2018, max_year=2023, verbose=False, file_suffix=None) :
    """
    Round 7 of seller-purchaser matches for 2018 onward.

    Parameters
    ----------
    data_folder : TYPE
        DESCRIPTION.
    save_folder : TYPE
        DESCRIPTION.
    min_year : TYPE, optional
        DESCRIPTION. The default is 2018.
    max_year : TYPE, optional
        DESCRIPTION. The default is 2022.
    verbose : TYPE, optional
        DESCRIPTION. The default is False.

    Returns
    -------
    None.

    """

    # Create Crosswalks for Sellers, Purchasers, and Types
    previous_match_round = 5
    purchaser_types = pd.read_parquet(f"{save_folder}/hmda_seller_purchaser_relationships_w_types_round{previous_match_round}{file_suffix}.parquet")

    # Load Data
    df = load_data(data_folder, min_year=min_year, max_year=max_year)

    # Replace Missings
    df = replace_missing_values(df)

    # Drop Observations with Missing Match Variables
    df['i_Purchase'] = 1*(df['loan_purpose'] == 1)
    match_columns = ['loan_type',
                     'loan_amount',
                     'census_tract',
                     'occupancy_type',
                     'i_Purchase']
    df = df.dropna(subset = match_columns)
    df = df.query('census_tract != ""')

    # Split Sold and Purchased Loans
    df, df_purchaser = split_sellers_and_purchasers(df, save_folder, match_round=7, file_suffix=file_suffix)
    df = df.merge(df_purchaser, on=match_columns, suffixes=('_s','_p'))
    del df_purchaser

    # Year Matches
    df = df.query('activity_year_s <= activity_year_p')

    # Drop Purchaser Type Mismatches
    df = df.merge(purchaser_types[['lei_s','lei_p','activity_year_s','purchaser_type_s']],
                  on = ['lei_s','lei_p','activity_year_s','purchaser_type_s'],
                  how = 'left',
                  indicator = True,
                  )
    df['i_PurchaserTypeMatch'] = 1*(df._merge == 'both')
    df = df.drop(columns = ['_merge'])
    df = df.query('i_PurchaserTypeMatch == 1 | purchaser_type_s == 0')

    # Allow Non-matching refi types
    df = df.query('loan_purpose_s == loan_purpose_p or (loan_purpose_s in [31,32] and loan_purpose_p in [31,32])')

    # Keep Close Matches with tolerances
    match_tolerances = {'interest_rate': .0625,
                        'conforming_loan_limit': 0,
                        'construction_method': 0,
                        'intro_rate_period': 6,
                        'lien_status': 0,
                        'open_end_line_of_credit': 0,
                        'total_units': 0,
                        }
    df = numeric_matches(df, match_tolerances, verbose=True)

    # Age, Sex, Ethnicity, and Race Matches
    df = match_age(df)
    df = match_sex(df)
    df = match_race(df)
    df = match_ethnicity(df)

    # Perform Numeric and Fee Matches
    df = perform_fee_matches(df)
    df = df.query('i_GenerousFeeMatch == 1 or NumberNonmissingFees_s == 0 | NumberNonmissingFees_p == 0')
    
    # Drop Candidate Matches with No Generous Fee Match where Fees have Any Match
    df['RateDifference'] = df['interest_rate_s'] - df['interest_rate_p']
    df['i_GoodRateMatch'] = abs(df['RateDifference']) < .001
    df.loc[pd.isna(df['interest_rate_s']) | pd.isna(df['interest_rate_p']), 'i_GoodRateMatch'] = None
    df['i_SaleHasRateMatch'] = df.groupby(['HMDAIndex_s'])['i_GoodRateMatch'].transform('max')
    df['i_PurchaseHasRateMatch'] = df.groupby(['HMDAIndex_p'])['i_GoodRateMatch'].transform('max')
    df = df.query('abs(RateDifference) < .001 | i_SaleHasRateMatch != 1')
    df = df.query('abs(RateDifference) < .001 | i_PurchaseHasRateMatch != 1')

    # Drop Candidate Matches with No Generous Fee Match where Fees have Any Match
    df['i_GoodMatch'] = 1*(df['NumberFeeMatches'] >= 2)
    df['i_SaleHasFeeMatch'] = df.groupby(['HMDAIndex_s'])['i_GoodMatch'].transform('max')
    df['i_PurchaseHasFeeMatch'] = df.groupby(['HMDAIndex_p'])['i_GoodMatch'].transform('max')
    df = df.query('i_GenerousFeeMatch == 1 | i_SaleHasFeeMatch == 0')
    df = df.query('i_GenerousFeeMatch == 1 | i_PurchaseHasFeeMatch == 0')

    # Drop Candidate Matches with No Generous Fee Match where Fees have Any Match
    df['IncomeDifference'] = df['income_s'] - df['income_p']
    df['i_PerfectIncomeMatch'] = df['income_s'] == df['income_p']
    df.loc[pd.isna(df['income_s']) | pd.isna(df['income_p']), 'i_PerfectIncomeMatch'] = None
    df['i_SaleHasIncomeMatch'] = df.groupby(['HMDAIndex_s'])['i_PerfectIncomeMatch'].transform('max')
    df['i_PurchaseHasIncomeMatch'] = df.groupby(['HMDAIndex_p'])['i_PerfectIncomeMatch'].transform('max')
    df = df.query('abs(IncomeDifference) <= 1 | i_SaleHasIncomeMatch != 1')
    df = df.query('abs(IncomeDifference) <= 1 | i_PurchaseHasIncomeMatch != 1')
    
    # Keep Unique Matches
    df = keep_uniques(df, one_to_one=False)
    
    # Numeric Matches Post Uniques
    match_tolerances = {'income': 1,
                        'interest_rate': .01,
                        'loan_term': 0,
                        'property_value': 10000,
                        }
    df = numeric_matches_post_unique(df, match_tolerances, verbose=True)

    # Save Crosswalk
    save_crosswalk(df, save_folder, match_round=7, file_suffix=file_suffix)

# Round 8: Match without PurchaserType=0 Originations
def match_hmda_sellers_purchasers_round8(data_folder, save_folder, min_year=2018, max_year=2023, verbose=False, file_suffix=None) :
    """
    Round 8 of seller-purchaser matches for 2018 onward.

    Parameters
    ----------
    data_folder : TYPE
        DESCRIPTION.
    save_folder : TYPE
        DESCRIPTION.
    min_year : TYPE, optional
        DESCRIPTION. The default is 2018.
    max_year : TYPE, optional
        DESCRIPTION. The default is 2022.
    verbose : TYPE, optional
        DESCRIPTION. The default is False.

    Returns
    -------
    None.

    """

    # Load Data
    df = load_data(data_folder, min_year=min_year, max_year=max_year)

    df = df.query('action_taken == 6 | purchaser_type != 0')

    # Replace Missings
    df = replace_missing_values(df)

    # Drop Observations with Missing Match Variables
    df['i_Purchase'] = 1*(df['loan_purpose'] == 1)
    match_columns = ['loan_type',
                     'loan_amount',
                     'census_tract',
                     'occupancy_type',
                     'i_Purchase']
    df = df.dropna(subset = match_columns)
    df = df.query('census_tract != ""')

    # Split Sold and Purchased Loans
    df, df_purchaser = split_sellers_and_purchasers(df, save_folder, match_round=8, file_suffix=file_suffix)
    df = df.merge(df_purchaser, on=match_columns, suffixes=('_s','_p'))
    del df_purchaser

    # Drop Type Mismatches
    # mmerge lei rt_lei activity_year using "$match_folder/seller_purchaser_match_types.dta", umatch(s_lei p_lei s_activity_year) unmatched(master)
    # drop if ~missing(s_purchaser_type) & purchaser_type != 0 & purchaser_type != s_purchaser_type

    # Keep LEIs with Known Connections
    # mmerge lei rt_lei using "$match_folder/large_seller_purchaser_relationships.dta", umatch(s_lei p_lei) unmatched(master)
    # gen i_LEIMatch = (_merge == 3)
    # bys lei: egen i_SellerLEIHasMatch = max(i_LEIMatch)
    # bys rt_lei: egen i_PurchaserLEIHasMatch = max(i_LEIMatch)
    # tab i_LEIMatch i_SellerLEIHasMatch
    # tab i_LEIMatch i_PurchaserLEIHasMatch
    # drop if i_LEIMatch == 0 & i_SellerLEIHasMatch == 1 & i_PurchaserLEIHasMatch == 1

    # Year Matches
    df = df.query('activity_year_s <= activity_year_p')

    # Allow Non-matching refi types
    df = df.query('loan_purpose_s == loan_purpose_p or (loan_purpose_s in [31,32] and loan_purpose_p in [31,32])')

    # Keep Close Matches with tolerances
    match_tolerances = {'interest_rate': .0625,
                        'conforming_loan_limit': 0,
                        'construction_method': 0,
                        'intro_rate_period': 6,
                        'lien_status': 0,
                        'open_end_line_of_credit': 0,
                        'total_units': 0,
                        }
    df = numeric_matches(df, match_tolerances, verbose=True)

    # Age, Sex, Ethnicity, and Race Matches
    df = match_age(df)
    df = match_sex(df)
    df = match_race(df)
    df = match_ethnicity(df)

    # Perform Numeric and Fee Matches
    df = perform_fee_matches(df)
    df = df.query('i_GenerousFeeMatch == 1 or NumberNonmissingFees_s == 0 | NumberNonmissingFees_p == 0')

    # Drop Candidate Matches with No Generous Fee Match where Fees have Any Match
    df['RateDifference'] = df['interest_rate_s'] - df['interest_rate_p']
    df['i_GoodRateMatch'] = abs(df['RateDifference']) < .001
    df.loc[pd.isna(df['interest_rate_s']) | pd.isna(df['interest_rate_p']), 'i_GoodRateMatch'] = None
    df['i_SaleHasRateMatch'] = df.groupby(['HMDAIndex_s'])['i_GoodRateMatch'].transform('max')
    df['i_PurchaseHasRateMatch'] = df.groupby(['HMDAIndex_p'])['i_GoodRateMatch'].transform('max')
    df = df.query('abs(RateDifference) < .001 | i_SaleHasRateMatch != 1')
    df = df.query('abs(RateDifference) < .001 | i_PurchaseHasRateMatch != 1')

    # Drop Candidate Matches with No Generous Fee Match where Fees have Any Match
    df['i_GoodMatch'] = 1*(df['NumberFeeMatches'] >= 2)
    df['i_SaleHasFeeMatch'] = df.groupby(['HMDAIndex_s'])['i_GoodMatch'].transform('max')
    df['i_PurchaseHasFeeMatch'] = df.groupby(['HMDAIndex_p'])['i_GoodMatch'].transform('max')
    df = df.query('i_GenerousFeeMatch == 1 | i_SaleHasFeeMatch == 0')
    df = df.query('i_GenerousFeeMatch == 1 | i_PurchaseHasFeeMatch == 0')

    # Drop Candidate Matches with No Generous Fee Match where Fees have Any Match
    df['IncomeDifference'] = df['income_s'] - df['income_p']
    df['i_PerfectIncomeMatch'] = df['income_s'] == df['income_p']
    df.loc[pd.isna(df['income_s']) | pd.isna(df['income_p']), 'i_PerfectIncomeMatch'] = None
    df['i_SaleHasIncomeMatch'] = df.groupby(['HMDAIndex_s'])['i_PerfectIncomeMatch'].transform('max')
    df['i_PurchaseHasIncomeMatch'] = df.groupby(['HMDAIndex_p'])['i_PerfectIncomeMatch'].transform('max')
    df = df.query('abs(IncomeDifference) <= 1 | i_SaleHasIncomeMatch != 1')
    df = df.query('abs(IncomeDifference) <= 1 | i_PurchaseHasIncomeMatch != 1')

    # Keep Unique Matches
    df = keep_uniques(df, one_to_one=False)

    # Numeric Matches Post Uniques
    match_tolerances = {'income': 2,
                        'interest_rate': .01,
                        'loan_term': 12,
                        'property_value': 30000,
                        }
    df = numeric_matches_post_unique(df, match_tolerances, verbose=True)

    # Save Crosswalk
    save_crosswalk(df, save_folder, match_round=8, file_suffix=file_suffix)

# Create Matched File
def create_matched_file(data_folder, match_folder, min_year=2018, max_year=2023, match_round=1, file_suffix=None) :
    """
    Creates a file with all HMDA data fields for matched sold/purchased loans.

    Parameters
    ----------
    data_folder : TYPE
        DESCRIPTION.
    match_folder : TYPE
        DESCRIPTION.
    min_year : TYPE, optional
        DESCRIPTION. The default is 2018.
    max_year : TYPE, optional
        DESCRIPTION. The default is 2022.
    match_round : TYPE, optional
        DESCRIPTION. The default is 1.

    Returns
    -------
    None.

    """

    # Load Crosswalk
    cw = pq.read_table(f"{match_folder}/hmda_seller_purchaser_matches_round{match_round}{file_suffix}.parquet").to_pandas()

    # Combine Seller and Purchaser Data
    df_seller = []
    for year in range(min_year, max_year+1) :
        print('Keeping matched sold loans from year:', year)
        file = HMDALoader.get_hmda_files(data_folder, min_year=year, max_year=year, extension='parquet')[0]
        df_a = pq.read_table(file, filters=[('action_taken','in',[1])]).to_pandas(date_as_object = False)
        df_a = df_a.merge(cw, left_on = ['HMDAIndex'], right_on = ['HMDAIndex_s'], how = 'inner')
        df_seller.append(df_a)
        del df_a
    df_seller = pd.concat(df_seller)
    df_seller = df_seller.drop(columns = ['HMDAIndex'])

    # Combine Seller and Purchaser Data
    df_purchaser = []
    for year in range(min_year, max_year+1) :
        print('Keeping matched purchased loans from year:', year)
        file = HMDALoader.get_hmda_files(data_folder, min_year=year, max_year=year, extension='parquet')[0]
        df_a = pq.read_table(file, filters=[('action_taken','in',[6])]).to_pandas(date_as_object = False)
        df_a = df_a.merge(cw, left_on=['HMDAIndex'], right_on=['HMDAIndex_p'])
        df_purchaser.append(df_a)
        del df_a
    df_purchaser = pd.concat(df_purchaser)
    df_purchaser = df_purchaser.drop(columns = ['HMDAIndex'])

    # Merge Sellers and Purchasers
    match_columns = ['HMDAIndex_s','HMDAIndex_p','match_round']
    df = df_seller.merge(df_purchaser, on=match_columns, suffixes=('_s','_p'))
    del df_seller, df_purchaser

    # Sort and Save Combined Data
    df = df[df.columns.sort_values()]
    df = df[match_columns+[x for x in df.columns if x not in match_columns]]
    df = pa.Table.from_pandas(df, preserve_index = False)
    pq.write_table(df, f'{match_folder}/hmda_seller_purchaser_matched_loans_round{match_round}{file_suffix}.parquet')

#%% Match Support Functions
# Get Purchaser Type Counts
def get_affiliates(data_folder, match_folder, match_round, min_year=2018, max_year=2023, strict=False, file_suffix=None) :
    """
    Get list of affiliate institutions for sellers and purchasers

    Parameters
    ----------
    data_folder : str
        DESCRIPTION.
    match_folder : str
        DESCRIPTION.
    match_round : int
        DESCRIPTION.
    min_year : int, optional
        DESCRIPTION. The default is 2018.
    max_year : int, optional
        DESCRIPTION. The default is 2023.
    strict : boolean, optional
        DESCRIPTION. The default is False.

    Returns
    -------
    cw : pandas DataFrame
        DESCRIPTION.

    """

    # Load Crosswalk
    df = pd.read_parquet(f'{match_folder}/hmda_seller_purchaser_matched_loans_round{match_round}{file_suffix}.parquet',
                         columns = ['HMDAIndex_s','HMDAIndex_p','activity_year_s','purchaser_type_s','purchaser_type_p','lei_s','lei_p'])

    # # Count Sold Loans
    df['CountSoldLoan'] = df.groupby(['HMDAIndex_s'])['HMDAIndex_s'].transform('count')

    # Keep Unique Matches Only
    if strict :
        df = df.query('CountSoldLoan == 1')
        df = df.drop(columns = ['CountSoldLoan'])
    else :
        df = df.query('CountSoldLoan == 1 or purchaser_type_s not in [1,2,3,4]')
        df = df.drop(columns = ['CountSoldLoan'])
        df['CountSoldLoan'] = df.groupby(['HMDAIndex_s'])['HMDAIndex_s'].transform('count')
        df = df.query('CountSoldLoan == 1')
        df = df.drop(columns = ['CountSoldLoan'])

    # Drop Loans With Unknown Purchaser
    df = df.query('purchaser_type_s != 0')

    # Count Matches between LEIs at the Year Level
    df['CountLEIMatches'] = df.groupby(['lei_s','lei_p','activity_year_s'])['activity_year_s'].transform('count')
    df['CountLEIPurchaserTypeMatches'] = df.groupby(['lei_s','lei_p','activity_year_s','purchaser_type_s'])['activity_year_s'].transform('count')

    #
    df = df.query('purchaser_type_s == 8')
    df['index'] = df.groupby(['lei_s','lei_p','activity_year_s'])['HMDAIndex_s'].rank('dense')
    df = df.drop_duplicates(subset = ['lei_s','lei_p','activity_year_s'])

    # Keep Only Good Matches for Common LEIs
    df = df.loc[df['CountLEIPurchaserTypeMatches']/df['CountLEIMatches'] >= .95]
    df = df.query('CountLEIPurchaserTypeMatches >= 10')

    # Keep Unique Matches
    df = df[['lei_s','lei_p']].drop_duplicates()

    # Sort and Save
    df = df.sort_values(by = ['lei_s','lei_p'])
    df.to_csv(f'{match_folder}/affiliate_lei_matches_round{match_round}{file_suffix}.csv', index = False)

    # Load TS/Panel Data and Merge in Names
    lender_folder = DATA_DIR
    lei_names = pd.read_csv(f'{lender_folder}/hmda_lenders_combined_2018-2022.csv',
                            sep = '|',
                            usecols = ['lei', 'respondent_name_panel', 'respondent_name_ts'],
                            )
    for column in lei_names.columns :
        lei_names[column] = lei_names[column].str.upper()
    lei_names = lei_names.drop_duplicates()

    # Merge in Names
    df = df.merge(lei_names, left_on = ['lei_s'], right_on = ['lei'])
    df = df.drop(columns = ['lei'])
    df = df.merge(lei_names, left_on = ['lei_p'], right_on = ['lei'], suffixes = ('_s','_p'))
    df = df.drop(columns = ['lei'])
    df = df.drop_duplicates()

    # Keep Unique LEI Combinations
    df_u = df[['lei_s','lei_p']].drop_duplicates()

    # Return Crosswalk with Names
    return df_u

# Get Purchaser Type Counts
def get_purchaser_type_counts(data_folder, match_folder, match_round, min_year=2018, max_year=2023, strict=False, file_suffix=None) :
    """
    Purchaser types are not unique, but appear to be unique (or almost unique)
    within a given seller-year

    Parameters
    ----------
    data_folder : TYPE
        DESCRIPTION.
    match_folder : TYPE
        DESCRIPTION.
    match_round : TYPE
        DESCRIPTION.
    min_year : TYPE, optional
        DESCRIPTION. The default is 2018.
    max_year : TYPE, optional
        DESCRIPTION. The default is 2022.

    Returns
    -------
    None.

    """

    # Load Crosswalk
    df = pd.read_parquet(f'{match_folder}/hmda_seller_purchaser_matched_loans_round{match_round}{file_suffix}.parquet',
                         columns=['HMDAIndex_s','HMDAIndex_p','activity_year_s','purchaser_type_s','purchaser_type_p','lei_s','lei_p'])

    # # Count Sold Loans
    df['CountSoldLoan'] = df.groupby(['HMDAIndex_s'])['HMDAIndex_s'].transform('count')

    # Keep Unique Matches Only
    if strict :
        df = df.query('CountSoldLoan == 1')
        df = df.drop(columns = ['CountSoldLoan'])
    else :
        df = df.query('CountSoldLoan == 1 or purchaser_type_s not in [1,2,3,4]')
        df = df.drop(columns = ['CountSoldLoan'])
        df['CountSoldLoan'] = df.groupby(['HMDAIndex_s'])['HMDAIndex_s'].transform('count')
        df = df.query('CountSoldLoan == 1')
        df = df.drop(columns = ['CountSoldLoan'])

    # Drop Loans With Unknown Purchaser
    df = df.query('purchaser_type_s != 0')

    # Count Matches between LEIs at the Year Level
    df['CountLEIMatches'] = df.groupby(['lei_s','lei_p','activity_year_s'])['activity_year_s'].transform('count')
    df['CountLEIPurchaserTypeMatches'] = df.groupby(['lei_s','lei_p','activity_year_s','purchaser_type_s'])['activity_year_s'].transform('count')

    # Keep Good Matches for Frequent Lender Matches
    df = df.loc[df['CountLEIPurchaserTypeMatches']/df['CountLEIMatches'] >= .99]
    df = df.query('CountLEIPurchaserTypeMatches >= 50')

    # Save
    df = df[['lei_s', 'lei_p', 'purchaser_type_s', 'activity_year_s','CountLEIMatches', 'CountLEIPurchaserTypeMatches']]
    df = df.drop_duplicates(subset = ['lei_s','lei_p','activity_year_s','purchaser_type_s'])
    df = df.sort_values(by = ['lei_s','lei_p','activity_year_s','purchaser_type_s'])
    df = pa.Table.from_pandas(df, preserve_index = False)
    pq.write_table(df, f'{match_folder}/hmda_seller_purchaser_relationships_w_types_round{match_round}{file_suffix}.parquet')

# Get Purchaser Type Counts
def get_lei_match_counts(data_folder, match_folder, match_round, min_year=2018, max_year=2023, strict=False) :
    """
    Gets seller-purchaser relationships between large originators and investors.

    Parameters
    ----------
    data_folder : TYPE
        DESCRIPTION.
    match_folder : TYPE
        DESCRIPTION.
    match_round : TYPE
        DESCRIPTION.
    min_year : TYPE, optional
        DESCRIPTION. The default is 2018.
    max_year : TYPE, optional
        DESCRIPTION. The default is 2022.
    strict : TYPE, optional
        DESCRIPTION. The default is False.

    Returns
    -------
    None.

    """

    # Load Crosswalk
    df = pd.read_parquet(f'{match_folder}/hmda_seller_purchaser_matched_loans_round{match_round}.parquet',
                     columns = ['HMDAIndex_s','HMDAIndex_p','activity_year_s','purchaser_type_s','purchaser_type_p','lei_s','lei_p'],
                     )

    # # Count Sold Loans
    df['CountSoldLoan'] = df.groupby(['HMDAIndex_s'])['HMDAIndex_s'].transform('count')

    # Keep Unique Matches Only
    if strict :
        df = df.query('CountSoldLoan == 1')
        df = df.drop(columns = ['CountSoldLoan'])
    else :
        df = df.query('CountSoldLoan == 1 or purchaser_type_s not in [1,2,3,4]')
        df = df.drop(columns = ['CountSoldLoan'])
        df['CountSoldLoan'] = df.groupby(['HMDAIndex_s'])['HMDAIndex_s'].transform('count')
        df = df.query('CountSoldLoan == 1')
        df = df.drop(columns = ['CountSoldLoan'])

    # Count Seller Loans, Purchaser Loans, and Matches
    df['CountSeller'] = df.groupby(['lei_s'])['lei_s'].transform('count')
    df['CountPurchaser'] = df.groupby(['lei_p'])['lei_p'].transform('count')
    df['CountMatches'] = df.groupby(['lei_s','lei_p'])['lei_s'].transform('count')

    # Keep Single Match Observations
    df = df[['lei_s','lei_p','CountSeller','CountPurchaser','CountMatches']].drop_duplicates()

    # Drop Extremely Poor Matches
    df = df.loc[(df['CountMatches']/df['CountSeller'] >= 0.001) | (df['CountMatches']/df['CountPurchaser'] >= 0.001)]
    df = df.loc[(df['CountMatches']/df['CountSeller'] >= 0.005) | (df['CountMatches']/df['CountPurchaser'] >= 0.0005)]
    df = df.loc[(df['CountMatches']/df['CountSeller'] >= 0.0005) | (df['CountMatches']/df['CountPurchaser'] >= 0.005)]
    df = df.loc[(df['CountMatches']/df['CountSeller'] >= 0.0001) | (df['CountMatches']/df['CountPurchaser'] >= 0.01)]
    df = df.loc[(df['CountMatches']/df['CountSeller'] >= 0.01) | (df['CountMatches']/df['CountPurchaser'] >= 0.0001)]
    df = df.query('CountSeller >= 1000 and CountPurchaser >= 1000')

    # Create Crosswalk for Large Sellers and Purchasers
    df = df[['lei_s','lei_p']]
    df = pa.Table.from_pandas(df, preserve_index = False)
    pq.write_table(df, f"{match_folder}/large_seller_purchaser_relationships_round{match_round}.parquet")

#%% Main Routine
if __name__ == '__main__' :
    
    # Unzip HMDA Data
    DATA_DIR = config.DATA_DIR
    DATA_FOLDER = DATA_DIR / 'clean'
    SAVE_FOLDER = DATA_DIR / 'match_data/match_sellers_purchasers_post2018'
    file_suffix = '_202409'

    # Conduct Matches in Rounds
    # match_hmda_sellers_purchasers_round1(DATA_FOLDER, SAVE_FOLDER, min_year=2018, max_year=2023, file_suffix=file_suffix)
    # match_hmda_sellers_purchasers_round2(DATA_FOLDER, SAVE_FOLDER, min_year=2018, max_year=2023, file_suffix=file_suffix)
    # match_hmda_sellers_purchasers_round3(DATA_FOLDER, SAVE_FOLDER, min_year=2018, max_year=2023, file_suffix=file_suffix)
    # match_hmda_sellers_purchasers_round4(DATA_FOLDER, SAVE_FOLDER, min_year=2018, max_year=2023, file_suffix=file_suffix)
    # match_hmda_sellers_purchasers_round5(DATA_FOLDER, SAVE_FOLDER, min_year=2018, max_year=2023, file_suffix=file_suffix)
    
    # create_matched_file(DATA_FOLDER, SAVE_FOLDER, min_year=2018, max_year=2023, match_round=5, file_suffix=file_suffix)
    # get_purchaser_type_counts(DATA_FOLDER, SAVE_FOLDER, 5, min_year=2018, max_year=2023, file_suffix=file_suffix)

    # match_hmda_sellers_purchasers_round6(DATA_FOLDER, SAVE_FOLDER, min_year=2018, max_year=2023, file_suffix=file_suffix)
    # match_hmda_sellers_purchasers_round7(DATA_FOLDER, SAVE_FOLDER, min_year=2018, max_year=2023, file_suffix=file_suffix)
    # match_hmda_sellers_purchasers_round8(DATA_FOLDER, SAVE_FOLDER, min_year=2018, max_year=2023, file_suffix=file_suffix)

    # create_matched_file(DATA_FOLDER, SAVE_FOLDER, min_year=2018, max_year=2023, match_round=8, file_suffix=file_suffix)
    
    # ## Examine Match Data
    # match_folder = SAVE_FOLDER
    # match_round = 8
    # df = pd.read_parquet(f'{match_folder}/hmda_seller_purchaser_matched_loans_round{match_round}{file_suffix}.parquet')
