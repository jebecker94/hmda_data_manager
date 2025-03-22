# Import Packages
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import pyod
import config
import HMDALoader

# Set Folder Paths
DATA_DIR = config.DATA_DIR

# Plot Lender Averages
for column in ['income','loan_amount','interest_rate'] :
    columns = ['activity_year','lei',column]
    filters = [('action_taken','==',1)]
    files = HMDALoader.get_hmda_files(DATA_DIR/'clean', file_type='lar', min_year=2018, max_year=2023, extension='parquet')
    df = []
    for file in files :
        df_a = pd.read_parquet(file, columns=columns, filters=filters)
        df_a.loc[df_a[column]=='1111', column] = None
        df_a[column] = pd.to_numeric(df_a[column], errors='coerce')
        df_a = df_a.dropna(subset=[column])
        df_a[f'average_{column}'] = df_a.groupby(['activity_year','lei'])[column].transform('mean')
        df_a['count_observations'] = df_a.groupby(['activity_year','lei'])[column].transform('count')
        df_a = df_a.drop(columns=[column]).drop_duplicates()
        df.append(df_a)
        del df_a
    df = pd.concat(df)

    # Plot Distributions by Year
    p01 = df[f'average_{column}'].quantile(.01)
    p99 = df[f'average_{column}'].quantile(.99)
    plt.figure(1)
    for year in list(df.activity_year.unique()) :
        df_year = df.query(f'activity_year=={year} & {p01}<=average_{column}<{p99}')
        plt.hist(df_year[f'average_{column}'], bins=100, alpha=.25, label=year, density=True)
    plt.legend()
    plt.xlabel(f'Average {column} for Lender')
    plt.ylabel('Density')
    plt.savefig(f'./output/average_{column}_by_lender.png', dpi=250)
    plt.show()
