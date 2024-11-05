import warnings
from datetime import datetime

import matplotlib.pyplot as plt
import polars as pl
from scipy import stats
from sklearn.ensemble import IsolationForest

warnings.filterwarnings('ignore')


class SalesDataLoader:
    """Class to handle data loading and sampling using Polars"""

    @staticmethod
    def load_csv(file_path, sample_size=None):
        """
        Load CSV file with optional sampling using Polars
        """
        # Use Polars scan_csv for lazy evaluation
        df = pl.scan_csv(file_path).collect()
        sample = sample_size or 0.6
        df = df.sample(fraction=sample)

        return df


class SalesDataProfiler:
    def __init__(self, df):
        """Initialize with a sales dataframe"""
        self.df = df.clone()
        self.prepare_data()

    def prepare_data(self):
        """Prepare data for analysis"""
        # Convert string to date and flag invalid dates
        self.df = self.df.with_columns([
            pl.col("Transaction Date").str.strptime(pl.Date, format="%m/%d/%y", strict=False).alias("Transaction Date"),
            (pl.col("Quantity Sold") * pl.col("Price per Unit")).alias("Total Amount")
        ])

    def rule_based_check(self):
        """Identify records with invalid data formats or incorrect values"""
        invalid_records = {}

        # Check for invalid dates
        invalid_dates = self.df.filter(pl.col("Transaction Date").is_null())
        if invalid_dates.height > 0:
            invalid_records['Invalid Dates'] = invalid_dates.to_dicts()

        # Check for future dates
        current_date = datetime.now().date()
        future_dates = self.df.filter(pl.col("Transaction Date") > current_date)
        if future_dates.height > 0:
            invalid_records['Future Dates'] = future_dates.to_dicts()

        # Check for negative or zero quantities and prices
        invalid_amounts = self.df.filter(
            (pl.col("Quantity Sold") <= 0) | (pl.col("Price per Unit") <= 0)
        )
        if invalid_amounts.height > 0:
            invalid_records['Invalid Amounts'] = invalid_amounts.to_dicts()

        # Check for empty strings or NaN in 'Quantity Sold'
        invalid_quantity = self.df.filter(
            (pl.col("Quantity Sold").is_null())
        )
        if invalid_quantity.height > 0:
            invalid_records['Invalid Quantity Sold'] = invalid_quantity.to_dicts()

        # Check for empty strings or NaN in 'Price per Unit'
        invalid_price = self.df.filter(
            (pl.col("Price per Unit").is_null())
        )
        if invalid_price.height > 0:
            invalid_records['Invalid Price per Unit'] = invalid_price.to_dicts()

        return invalid_records

    def anomaly_detection(self):
        # Select numerical columns and fill NaN values with 0
        numerical_features = self.df.select(["Quantity Sold", "Price per Unit"]).fill_null(0.0)

        # Check if numerical_features is empty
        if numerical_features.height == 0:
            return pl.DataFrame()  # Return an empty DataFrame if no data is available

        features_np = numerical_features.to_numpy()

        # Isolation Forest Anomaly Detection
        iso_forest = IsolationForest(contamination=0.05, random_state=42)

        # Fit and predict anomaly scores
        anomaly_scores = iso_forest.fit_predict(features_np)

        # Create a new column for the anomaly score
        self.df = self.df.with_columns(pl.Series(values=anomaly_scores, name='anomaly_score'))

        # Filter anomalies where score is -1
        anomalies = self.df.filter(pl.col('anomaly_score') == -1)

        self.plot_anomalies(numerical_features.to_numpy(), anomalies)

        return anomalies

    def plot_anomalies(self, features_np, anomalies):
        # Extract anomaly data for plotting
        anomaly_points = anomalies.select(["Quantity Sold", "Price per Unit"]).to_numpy()

        # Create a scatter plot
        plt.figure(figsize=(10, 6))
        plt.scatter(features_np[:, 0], features_np[:, 1], label='Normal Data', color='blue', alpha=0.5)
        plt.scatter(anomaly_points[:, 0], anomaly_points[:, 1], label='Anomalies', color='red', alpha=0.7)

        plt.title('Anomaly Detection using Isolation Forest')
        plt.xlabel('Quantity Sold')
        plt.ylabel('Price per Unit')
        plt.legend()
        plt.grid(True)
        plt.show()

    def calculate_correlations(self):
        # Calculate correlation matrix for numerical columns using Polars' pearson_correlation
        correlation_matrix = (
            self.df.select(["Quantity Sold", "Price per Unit", "Total Amount"])
            .fill_null(0.0)
            .corr()
        )
        return correlation_matrix.to_dict()

    def generate_summary(self):
        """Generate detailed summary of the dataset using Polars"""
        summary = {
            'numerical_summary': {},
            'categorical_summary': {},
            'temporal_summary': {},
            'correlation_analysis': {},
            'frequency_patterns': {}
        }

        # Numerical summary
        numerical_cols = ["Quantity Sold", "Price per Unit", "Total Amount"]
        for col in numerical_cols:
            stats_df = self.df.select([
                pl.col(col).mean().alias("mean"),
                pl.col(col).std().alias("std"),
                pl.col(col).min().alias("min"),
                pl.col(col).max().alias("max"),
                pl.col(col).quantile(0.25).alias("25%"),
                pl.col(col).quantile(0.5).alias("50%"),
                pl.col(col).quantile(0.75).alias("75%"),
                pl.col(col).count().alias("count"),
                pl.col(col).null_count().alias("null_count"),
                pl.col(col).n_unique().alias("unique_values")
            ])

            stats_dict = stats_df.to_dicts()[0]
            values = self.df.get_column(col).to_numpy()
            stats_dict["skewness"] = float(stats.skew(values))
            stats_dict["kurtosis"] = float(stats.kurtosis(values))
            summary['numerical_summary'][col] = stats_dict

        # Temporal summary
        date_stats = self.df.select([
            pl.col("Transaction Date").min().alias("min_date"),
            pl.col("Transaction Date").max().alias("max_date")
        ]).to_dicts()[0]

        if date_stats['min_date'] is not None and date_stats['max_date'] is not None:
            time_span = (date_stats['max_date'] - date_stats['min_date']).days
        else:
            time_span = None

        summary['temporal_summary'] = {
            'date_range': {
                'start': date_stats['min_date'],
                'end': date_stats['max_date']
            },
            'time_span': time_span,
        }

        return summary

    def generate_profile_report(self):
        """Generate a comprehensive profile report"""
        summary = self.generate_summary()
        invalid_records = self.rule_based_check()

        report = {
            'dataset_summary': {
                'total_records': self.df.height,
                'date_range': {
                    'start': self.df.get_column("Transaction Date").min().strftime('%Y-%m-%d') if self.df.get_column("Transaction Date").min() else None,
                    'end': self.df.get_column("Transaction Date").max().strftime('%Y-%m-%d') if self.df.get_column("Transaction Date").max() else None
                },
                'total_customers': self.df.get_column("Customer ID").n_unique(),
                'total_products': self.df.get_column("Product ID").n_unique(),
                'total_stores': self.df.get_column("Store ID").n_unique(),
                'total_salespeople': self.df.get_column("Salesperson ID").n_unique()
            },
            'data_summary': summary,
            'invalid_records': invalid_records,
            'anomalies': self.anomaly_detection().to_dicts(),
            'correlations': self.calculate_correlations()
        }

        return report


def main():
    # Load data with sampling
    loader = SalesDataLoader()
    df = loader.load_csv("~/Downloads/Sales_Data.csv", sample_size=0.8)

    # Initialize profiler
    profiler = SalesDataProfiler(df)

    # Generate and print report
    report = profiler.generate_profile_report()

    # Print formatted report
    print("\nSales Data Profile Report")
    print("=" * 50)

    print("\n1. Dataset Summary:")
    for key, value in report['dataset_summary'].items():
        print(f"  {key.replace('_', ' ').title()}: {value}")

    print("\n2. Data Summary Highlights:")
    # Print numerical summaries
    print("\n  Numerical Variables:")
    for var, stats in report['data_summary']['numerical_summary'].items():
        print(f"\n    {var}:")
        print(f"      Mean: {stats['mean']:.2f}")
        print(f"      Std Dev: {stats['std']:.2f}")
        print(f"      Skewness: {stats['skewness']:.2f}")

    # Display invalid records, if any
    print("\n3. Invalid Records:")
    if report['invalid_records']:
        for issue_type, records in report['invalid_records'].items():
            print(f"\n  {issue_type}:")
            for record in records:
                print(f"    {record}")
    else:
        print("  No invalid records found.")

    # Display correlations
    print("\n5. Correlation Matrix:")
    print(report['correlations'])

    # Display anomalies
    if report['anomalies']:
        print(f"\n4. Anomalies Detected in {len(report['anomalies'])} rows.")
        for r in report['anomalies']:
            print(f"\n  {r}")
    else:
        print("  No anomalies found.")


if __name__ == "__main__":
    main()
