"""
Project Runner: Apache Spark E-Commerce Analytics
=================================================
Runs data generation (if needed) and executes the full PySpark pipeline
with high-level summaries and educational tips.
"""

import os
import sys

def main():
    project_root = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(project_root, "data")
    csv_file = os.path.join(data_dir, "raw_transactions.csv")
    parquet_dir = os.path.join(project_root, "output", "analytics_parquet")
    
    print("=" * 80)
    print("       APACHE SPARK BASICS: E-COMMERCE ANALYTICS PIPELINE")
    print("=" * 80)
    print("This script demonstrates end-to-end distributed data processing in Spark.")
    
    # Step 1: Ensure raw dataset exists
    if not os.path.exists(csv_file):
        print("\nDataset not found. Generating synthetic dataset now...")
        from data.generate_dataset import generate_ecommerce_data
        generate_ecommerce_data(csv_file, num_records=5000)
    else:
        print(f"\nFound existing dataset: {csv_file}")
        
    # Step 2: Run PySpark Pipeline
    sys.path.insert(0, os.path.join(project_root, "src"))
    from spark_pipeline import run_full_pipeline
    
    run_full_pipeline(csv_path=csv_file, parquet_output_dir=parquet_dir)
    
    print("\nNext steps:")
    print("1. Read CONCEPT_GUIDE.md to master the theoretical concepts behind this pipeline.")
    print("2. Check the 'output/analytics_parquet' folder to see how Spark partitions big data.")
    print("=" * 80)

if __name__ == "__main__":
    main()
