"""
Project Runner: Apache Spark E-Commerce Analytics
=================================================
Runs data generation (if needed) and executes the full PySpark pipeline
with high-level summaries and educational tips.

Changelog:
- Added logging module for structured output (replaces raw print statements)
- Added error handling for missing data directory
- Added runtime duration tracking
"""

import os
import sys
import logging
import time

# Configure structured logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)


def main():
    start_time = time.time()
    project_root = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(project_root, "data")
    csv_file = os.path.join(data_dir, "raw_transactions.csv")
    parquet_dir = os.path.join(project_root, "output", "analytics_parquet")

    logger.info("=" * 60)
    logger.info("APACHE SPARK BASICS: E-COMMERCE ANALYTICS PIPELINE")
    logger.info("=" * 60)
    logger.info("This script demonstrates end-to-end distributed data processing in Spark.")

    # Validate data directory exists
    if not os.path.isdir(data_dir):
        logger.error("Data directory not found: %s", data_dir)
        sys.exit(1)

    # Step 1: Ensure raw dataset exists
    if not os.path.exists(csv_file):
        logger.info("Dataset not found. Generating synthetic dataset now...")
        from data.generate_dataset import generate_ecommerce_data
        generate_ecommerce_data(csv_file, num_records=5000)
        logger.info("Dataset generated at: %s", csv_file)
    else:
        logger.info("Found existing dataset: %s", csv_file)

    # Step 2: Run PySpark Pipeline
    sys.path.insert(0, os.path.join(project_root, "src"))
    from spark_pipeline import run_full_pipeline

    try:
        run_full_pipeline(csv_path=csv_file, parquet_output_dir=parquet_dir)
    except Exception as exc:
        logger.error("Pipeline failed with error: %s", exc, exc_info=True)
        sys.exit(1)

    elapsed = round(time.time() - start_time, 2)
    logger.info("Pipeline completed in %.2f seconds", elapsed)
    logger.info("Next steps:")
    logger.info("  1. Read CONCEPT_GUIDE.md to master the theoretical concepts.")
    logger.info("  2. Check 'output/analytics_parquet' to see how Spark partitions data.")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()