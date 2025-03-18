# Import Packages
import pandas as pd
import glob
import config

# Main Routine
if __name__=='__main__':

    # Set Data Directory
    LOANS_DIR = config.CLEAN_DIR / 'loans'

    # Load Combined DC Data
    df = []
    files = glob.glob(f'{LOANS_DIR}/*_temp.parquet')
    for file in files :
        print('Adding data from file:', file)
        df_a = pd.read_parquet(file, filters=[('state_code','==','DC')])
        df.append(df_a)
    df = pd.concat(df)


