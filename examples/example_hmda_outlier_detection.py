# Import Packages
import os
import numpy as np
import pandas as pd
import polars as pl
import matplotlib.pyplot as plt
import config
import HMDALoader
from scipy import stats
from pyod.models.knn import KNN
from pathlib import Path

os.chdir(Path(__file__).resolve().parent.parent)
OUTPUT_DIR = Path("output")
OUTPUT_DIR.mkdir(exist_ok=True)

# Set Folder Paths
DATA_DIR = config.DATA_DIR

## Plot Lender Averages
for column in ["income", "loan_amount", "interest_rate"]:
    columns = ["activity_year", "lei", column]
    filters = [("action_taken", "==", 1)]
    files = HMDALoader.get_hmda_files(
        DATA_DIR / "clean",
        file_type="lar",
        min_year=2018,
        max_year=2023,
        extension="parquet",
    )
    df = []
    for file in files:
        df_a = pl.read_parquet(file, columns=columns)
        df_a = df_a.filter(pl.col("action_taken") == 1).to_pandas()
        df_a.loc[df_a[column] == "1111", column] = None
        df_a[column] = pd.to_numeric(df_a[column], errors="coerce")
        df_a = df_a.dropna(subset=[column])
        df_a[f"average_{column}"] = df_a.groupby(["activity_year", "lei"])[
            column
        ].transform("mean")
        df_a["count_observations"] = df_a.groupby(["activity_year", "lei"])[
            column
        ].transform("count")
        df_a = df_a.drop(columns=[column]).drop_duplicates()
        df.append(df_a)
        del df_a
    df = pd.concat(df)

    # Plot Distributions by Year
    p01 = df[f"average_{column}"].quantile(0.01)
    p99 = df[f"average_{column}"].quantile(0.99)
    plt.figure(1)
    for year in list(df.activity_year.unique()):
        df_year = df.query(f"activity_year=={year} & {p01}<=average_{column}<{p99}")
        plt.hist(
            df_year[f"average_{column}"], bins=100, alpha=0.25, label=year, density=True
        )
    plt.legend()
    plt.xlabel(f"Average {column} for Lender")
    plt.ylabel("Density")
    plt.savefig(OUTPUT_DIR / f"average_{column}_by_lender.png", dpi=250)
    plt.show()

## PyOD Example
# Import Data
columns = [
    "activity_year",
    "lei",
    "income",
    "loan_amount",
    "interest_rate",
    "combined_loan_to_value_ratio",
    "debt_to_income_ratio",
    "discount_points",
    "loan_term",
    "property_value",
    "loan_type",
    "loan_purpose",
]
filters = [("action_taken", "==", 1), ("state_code", "==", "DC")]
files = HMDALoader.get_hmda_files(
    DATA_DIR / "clean",
    file_type="lar",
    min_year=2018,
    max_year=2023,
    extension="parquet",
)
df = []
for file in files:
    df_a = pl.read_parquet(file, columns=columns)
    df_a = df_a.filter(
        (pl.col("action_taken") == 1) & (pl.col("state_code") == "DC")
    ).to_pandas()
    df.append(df_a)
    del df_a
df = pd.concat(df)

# Clean Data
for column in [
    "income",
    "loan_amount",
    "interest_rate",
    "combined_loan_to_value_ratio",
    "debt_to_income_ratio",
    "discount_points",
    "loan_term",
    "property_value",
]:
    df.loc[df[column] == "1111", column] = None
    df[column] = pd.to_numeric(df[column], errors="coerce")

# Sample Selection
df = df.query("loan_term==360 & loan_type==1")

# Define Classification Columns
classification_columns = [
    "loan_amount",
    "property_value",
    "combined_loan_to_value_ratio",
    "loan_purpose",
    "activity_year",
]

# Create Training Data
df = df.dropna(subset=classification_columns)
df_train = df.copy()
# df_train = df_train.sample(10000)
X_train = df_train[classification_columns].values

outlier_fraction = 0.005
number_neighbors = 15
clf = KNN(contamination=outlier_fraction, n_neighbors=number_neighbors)

# fit the dataset to the model
clf.fit(X_train)

# predict raw anomaly score
anomaly_scores = clf.decision_function(X_train)

# prediction of a datapoint category outlier or inlier
y_pred = clf.predict(X_train)

# threshold value to consider a datapoint inlier or outlier
outlier_threshold = stats.scoreatpercentile(
    anomaly_scores, 100 * (1 - outlier_fraction)
)

# Prediction Score Histogram
step_size = 10000
plt.figure(2)
plt.hist(anomaly_scores, bins=np.arange(0, outlier_threshold + step_size, step_size))
plt.axvline(x=[outlier_threshold], color="red")
plt.show()

# Grab Outliers
outliers = df_train[y_pred == 1]
score_outliers = df_train[anomaly_scores > outlier_threshold]
