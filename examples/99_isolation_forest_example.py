"""
Isolation Forest Anomaly Detection for HMDA Data
=================================================

This script uses Isolation Forest to detect systematic data quality issues
in HMDA loan data by identifying anomalous reporting patterns at the lender (LEI) level.

The analysis identified several types of systematic errors:
- Unit/scale errors (raw values instead of thousands, decimals instead of percentages)
- Invalid placeholder codes (e.g., 8888 for discount points)
- Suspicious patterns (unusual but potentially legitimate)

Results are documented in docs/DATA_QUALITY_ISSUES.md for use in the gold layer pipeline.

Detection Method:
- Isolation Forest with contamination=0.0025 (0.25% anomaly rate)
- Features: interest_rate, loan_amount, income, CLTV, DTI, discount_points,
  lender_credits, loan_type, loan_purpose, purchaser_type, loan_term
- Data: 2020 originated loans (action_taken=1)

"""

from sklearn.ensemble import IsolationForest
from sklearn.model_selection import train_test_split
import matplotlib.pyplot as plt
import pandas as pd
import polars as pl
import numpy as np
from hmda_data_manager.core import SILVER_DIR

# =============================================================================
# Step 1: Load Data
# =============================================================================

print("Step 1: Loading HMDA data from silver layer...")

# Load silver data for 2020 originations
# Using snapshot file (file_type=c) for most complete annual data
data_path = SILVER_DIR / "loans" / "post2018" / "activity_year=2023" / "file_type=b" / "*.parquet"

# Define features for anomaly detection
X_columns = [
    'interest_rate',
    'loan_amount',
    'income',
    'combined_loan_to_value_ratio',
    'debt_to_income_ratio',
    'activity_year',
    'discount_points',
    'lender_credits',
    'property_value',
    'loan_type',
    'loan_purpose',
    'purchaser_type',
    'loan_term',
]
y_columns = ['lei']

# Load with Polars and convert to pandas for sklearn
dataset_pl = (
    pl.scan_parquet(data_path)
    .filter(pl.col("action_taken") == 1)  # Originations only
    .select(X_columns + y_columns)
    .collect()
)

# Replace -99999 values with Nones
for col in X_columns:
    dataset_pl = dataset_pl.with_columns(pl.col(col).replace(-99999, None))

# Convert to pandas for sklearn compatibility
dataset = dataset_pl.to_pandas()

print(f"Loaded {len(dataset):,} originated loans from 2024")

# =============================================================================
# Step 2: Prepare Data for Modeling
# =============================================================================

print("\nStep 2: Preparing data for modeling...")

# Convert to numeric types (handle any remaining string values)
for col in X_columns:
    dataset[col] = pd.to_numeric(dataset[col], errors='coerce')

# Fill missing values with the median of each column
for col in X_columns:
    dataset[col] = dataset[col].fillna(dataset[col].median())

# Drop any remaining missing values
dataset = dataset.dropna(subset=X_columns)

print(f"After cleaning: {len(dataset):,} loans")

# Set X (features) and y (LEI for grouping)
X = dataset[X_columns].values
y = dataset[y_columns].values

# Split the data into training and testing sets
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.3, random_state=42
)

print(f"Training set: {len(X_train):,} loans")
print(f"Test set: {len(X_test):,} loans")

# =============================================================================
# Step 3: Fit Isolation Forest Model
# =============================================================================

print("\nStep 3: Fitting Isolation Forest model...")

# Initialize and fit the model
# contamination=0.0025 means we expect 0.25% of the data to be anomalies
clf = IsolationForest(
    contamination=0.0025,
    random_state=42,
    n_jobs=-1,
    n_estimators=100
)

clf.fit(X_train)
print("Model fitted successfully")

# =============================================================================
# Step 4: Make Predictions
# =============================================================================

print("\nStep 4: Detecting anomalies...")

# Predict anomalies (-1 = anomaly, 1 = normal)
y_pred_train = clf.predict(X_train)
y_pred_test = clf.predict(X_test)

# Calculate anomaly scores (lower = more anomalous)
y_train_scores = clf.decision_function(X_train)
y_test_scores = clf.decision_function(X_test)

# Print summary statistics
train_anomalies = (y_pred_train == -1).sum()
test_anomalies = (y_pred_test == -1).sum()

print(f"\nTraining set anomalies: {train_anomalies:,} ({train_anomalies/len(y_pred_train)*100:.2f}%)")
print(f"Test set anomalies: {test_anomalies:,} ({test_anomalies/len(y_pred_test)*100:.2f}%)")
print(f"Mean anomaly score (train): {np.mean(y_train_scores):.4f}")
print(f"Mean anomaly score (test): {np.mean(y_test_scores):.4f}")

# =============================================================================
# Step 5: Visualize Anomaly Score Distribution
# =============================================================================

print("\nStep 5: Generating visualizations...")

# Plot the distribution of outlier scores
fig, axes = plt.subplots(1, 2, figsize=(12, 6))

axes[0].hist(
    y_train_scores[y_pred_train == 1],
    bins=50,
    color='blue',
    alpha=0.7,
    label='Train Scores (Normal)'
)
axes[0].hist(
    y_train_scores[y_pred_train == -1],
    bins=50,
    color='red',
    alpha=0.7,
    label='Train Scores (Anomalies)'
)
axes[0].set_title('Train Anomaly Scores')
axes[0].set_xlabel('Anomaly Score')
axes[0].set_ylabel('Frequency')
axes[0].legend()

axes[1].hist(
    y_test_scores[y_pred_test == 1],
    bins=50,
    color='blue',
    alpha=0.7,
    label='Test Scores (Normal)'
)
axes[1].hist(
    y_test_scores[y_pred_test == -1],
    bins=50,
    color='red',
    alpha=0.7,
    label='Test Scores (Anomalies)'
)
axes[1].set_title('Test Anomaly Scores')
axes[1].set_xlabel('Anomaly Score')
axes[1].set_ylabel('Frequency')
axes[1].legend()

plt.tight_layout()

# =============================================================================
# Step 6: Scatter Plots of Anomalies
# =============================================================================

def create_scatter_plots(X1, y1, title1, X2, y2, title2):
    """Create scatter plots comparing features for normal vs anomalous loans."""
    fig, axes = plt.subplots(nrows=2, ncols=2, figsize=(12, 6), sharex='col')

    # Set limits for figures by truncating 1st and 99th percentiles
    x0_min = np.quantile(X1[:, 0], .01)
    x0_max = np.quantile(X1[:, 0], .99)
    x1_min = np.quantile(X1[:, 1], .01)
    x1_max = np.quantile(X1[:, 1], .99)
    x2_min = np.quantile(X1[:, 2], .01)
    x2_max = np.quantile(X1[:, 2], .99)
    x3_min = np.quantile(X1[:, 3], .01)
    x3_max = np.quantile(X1[:, 3], .99)

    # Interest Rate vs Loan Amount (training)
    axes[0, 0].scatter(
        X1[y1 == 1, 0], X1[y1 == 1, 1],
        color='green', label='Normal', s=1, alpha=0.1
    )
    axes[0, 0].scatter(
        X1[y1 == -1, 0], X1[y1 == -1, 1],
        color='red', label='Anomaly', s=1, alpha=0.1
    )
    axes[0, 0].set_title(title1)
    axes[0, 0].set_xlim(x0_min, x0_max)
    axes[0, 0].set_ylim(x1_min, x1_max)
    axes[0, 0].legend()

    # Interest Rate vs Loan Amount (test)
    axes[1, 0].scatter(
        X2[y2 == 1, 0], X2[y2 == 1, 1],
        color='green', label='Normal', s=1, alpha=0.1
    )
    axes[1, 0].scatter(
        X2[y2 == -1, 0], X2[y2 == -1, 1],
        color='red', label='Anomaly', s=1, alpha=0.1
    )
    axes[1, 0].set_title(title2)
    axes[1, 0].set_xlim(x0_min, x0_max)
    axes[1, 0].set_ylim(x1_min, x1_max)
    axes[1, 0].legend()

    # Income vs CLTV (training)
    axes[0, 1].scatter(
        X1[y1 == 1, 2], X1[y1 == 1, 3],
        color='green', label='Normal', s=1, alpha=0.1
    )
    axes[0, 1].scatter(
        X1[y1 == -1, 2], X1[y1 == -1, 3],
        color='red', label='Anomaly', s=1, alpha=0.1
    )
    axes[0, 1].set_title(title1)
    axes[0, 1].set_xlim(x2_min, x2_max)
    axes[0, 1].set_ylim(x3_min, x3_max)
    axes[0, 1].legend()

    # Income vs CLTV (test)
    axes[1, 1].scatter(
        X2[y2 == 1, 2], X2[y2 == 1, 3],
        color='green', label='Normal', s=1, alpha=0.1
    )
    axes[1, 1].scatter(
        X2[y2 == -1, 2], X2[y2 == -1, 3],
        color='red', label='Anomaly', s=1, alpha=0.1
    )
    axes[1, 1].set_title(title2)
    axes[1, 1].set_xlim(x2_min, x2_max)
    axes[1, 1].set_ylim(x3_min, x3_max)
    axes[1, 1].legend()

    plt.suptitle('Isolation Forest Anomaly Detection', fontsize=16)
    plt.tight_layout()

# Create scatter plots
create_scatter_plots(
    X_train, y_pred_train, 'Training Data',
    X_test, y_pred_test, 'Test Data'
)

# =============================================================================
# Step 7: Analyze Anomalies by LEI
# =============================================================================

print("\nStep 7: Analyzing anomalies by lender (LEI)...")

# Put data back into DataFrame for analysis
X_train_df = pd.DataFrame(X_train, columns=X_columns)
y_train_df = pd.DataFrame(y_train, columns=y_columns)
train_df = pd.concat([X_train_df, y_train_df], axis=1)
train_df['anomaly'] = y_pred_train
train_df['anomaly_score'] = y_train_scores

X_test_df = pd.DataFrame(X_test, columns=X_columns)
y_test_df = pd.DataFrame(y_test, columns=y_columns)
test_df = pd.concat([X_test_df, y_test_df], axis=1)
test_df['anomaly'] = y_pred_test
test_df['anomaly_score'] = y_test_scores

# Combine train and test
train_df['i_Train'] = 1
combined_df = pd.concat([train_df, test_df], ignore_index=True)
combined_df['i_Train'] = combined_df['i_Train'].fillna(0)

# Calculate LEI-level statistics
combined_df['lei_anomaly_score'] = combined_df.groupby(['lei'])['anomaly_score'].transform('mean')
combined_df['lei_anomaly_count'] = combined_df.groupby(['lei'])['anomaly'].transform(lambda x: (x == -1).sum())
combined_df['count_lei'] = combined_df.groupby(['lei'])['anomaly'].transform('count')

# Get LEI summary
lei_df = combined_df[['lei', 'lei_anomaly_score', 'lei_anomaly_count', 'count_lei']].drop_duplicates()

# Find highly anomalous LEIs (low anomaly score, multiple anomalous loans)
anomalous_leis = lei_df.query('lei_anomaly_score <= 0.01 & count_lei >= 10').sort_values('lei_anomaly_score')

print(f"\nFound {len(anomalous_leis)} lenders with systematic anomalies")
print("\nTop 10 most anomalous lenders:")
print(anomalous_leis.head(10).to_string(index=False))

# =============================================================================
# Step 8: Collect anomalous loans for viewing
# =============================================================================
anomalous_loans = combined_df.query('anomaly == -1')
anomalous_loans = anomalous_loans.sort_values(by=['lei_anomaly_score','lei'])
anomalous_loans = anomalous_loans.reset_index(drop=True)

# =============================================================================
# Summary
# =============================================================================

print("\n" + "="*60)
print("Analysis Complete!")
print("="*60)
print(f"\nTotal loans analyzed: {len(dataset):,}")
print(f"Anomalous loans detected: {train_anomalies + test_anomalies:,}")
print(f"Lenders with systematic issues: {len(anomalous_leis)}")
print(f"\nDetailed findings documented in:")
print("  - docs/DATA_QUALITY_ISSUES.md")
