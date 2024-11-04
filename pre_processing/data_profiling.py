import pandas
import pandas as pd
import numpy as np
from sklearn.ensemble import IsolationForest


# Basic Data Summary
def data_summary(df):
    """
    Prints a summary of the DataFrame including descriptive statistics.
    """
    print("Data Summary:")
    print(df.describe(include='all'))


# Uniqueness Check
def uniqueness_check(df, fields=None):
    """
    Checks and prints the number of unique values for specified fields in the DataFrame.
    """
    fields = fields or ['Transaction ID']
    unique_counts = {col: df[col].nunique() for col in fields}
    print("\nUniqueness Check:")
    for col, unique_count in unique_counts.items():
        print(f"{col}: {unique_count} unique values ({unique_count / len(df) * 100:.2f}% of total)")


# Anomaly Detection using Isolation Forest (handles mixed data types better)
def isolation_forest_anomaly_detection(df):
    """
    Detects anomalies in the DataFrame using Isolation Forest.
    """
    print("\nAnomalies detected by Isolation Forest:")
    iso_forest = IsolationForest(contamination=0.05, random_state=42)
    df['anomaly_score'] = iso_forest.fit_predict(df.select_dtypes(include=[np.number]).fillna(0))
    anomalies = df[df['anomaly_score'] == -1]
    print(anomalies)
    return anomalies


# 1. Detect Unusual Quantities or Prices Using Thresholds
def detect_unusual_values(df, column, low_percentile=0.01, high_percentile=0.99):
    """
    Detects and prints unusual values in a specified column based on percentile thresholds.
    """
    low_threshold = df[column].quantile(low_percentile)
    high_threshold = df[column].quantile(high_percentile)
    anomalies = df[(df[column] < low_threshold) | (df[column] > high_threshold)]
    print(f"\nUnusual values in '{column}' outside {low_percentile * 100}% - {high_percentile * 100}% range:")
    print(anomalies[[column, "Transaction ID"]])
    return anomalies


# 2. Validate Date Range
def detect_date_anomalies(df, start_date="2023-01-01", end_date="2023-12-31"):
    """
    Detects and prints transactions outside the specified date range.
    """
    anomalies = df[(df['Transaction Date'] < start_date) | (df['Transaction Date'] > end_date)]
    print("\nTransactions outside expected date range:")
    print(anomalies[['Transaction ID', 'Transaction Date']])
    return anomalies


# 3. Frequency-Based Detection for Categorical Columns
def detect_rare_categories(df, column, min_frequency=1):
    """
    Detects and prints rare categories in a specified column based on minimum frequency.
    """
    category_counts = df[column].value_counts()
    rare_categories = category_counts[category_counts < min_frequency].index
    anomalies = df[df[column].isin(rare_categories)]
    print(f"\nRare categories in '{column}':")
    print(anomalies[[column]])
    return anomalies


df = pd.read_csv("~/Downloads/Sales_Data.csv")

data_summary(df)
uniqueness_check(df)
iso_forest_anomalies = isolation_forest_anomaly_detection(df)
rare_payment_method_anomalies = detect_rare_categories(df, "Payment Method", min_frequency=1)
date_anomalies = detect_date_anomalies(df)
quantity_anomalies = detect_unusual_values(df, "Quantity Sold")
price_anomalies = detect_unusual_values(df, "Price per Unit")
