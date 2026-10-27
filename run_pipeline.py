"""
Project Runner: Apache Spark E-Commerce Analytics
=================================================
Entry-point script that orchestrates data generation (when needed) and
executes the full PySpark pipeline with structured logging and runtime
duration tracking.

Responsibilities
----------------
1. Validate that the ``data/`` directory exists before proceeding.
2. Auto-generate a synthetic ``raw_transactions.csv`` dataset if none is
   found — useful for first-time setup without requiring manual data prep.
3. Insert ``src/`` onto ``sys.path`` so that ``spark_pipeline`` can be
   imported without installing the package.
4. Delegate all Spark work to ``spark_pipeline.run_full_pipeline``.
5. Log total elapsed wall-clock time and useful next-step hints on success.

Logging strategy
----------------
We use Python's standard ``logging`` module instead of bare ``print`` calls
because:
* Log levels (DEBUG / INFO / WARNING / ERROR / CRITICAL) let users control
  verbosity without editing source code.
* Timestamps in every log line make it easy to correlate events with Spark UI
  stage timelines.
* The structured format is friendly to log aggregation tools (CloudWatch,
  Datadog, ELK, etc.) in production environments.

Author      : Rajapakshaminindu
Last Updated: 2026-10-21
Version     : 1.2.0

Changelog
---------
v1.2.0 (2026-10-21)
    - Replaced bare print statements with structured logging
    - Added data directory existence validation with early exit
    - Added wall-clock runtime tracking
    - Added informative next-step hints after successful run
v1.1.0
    - Added automatic synthetic dataset generation via generate_dataset.py
v1.0.0
    - Initial pipeline runner
"""

import os
import sys
import logging
import time

# ---------------------------------------------------------------------------
# LOGGING CONFIGURATION
# ---------------------------------------------------------------------------
# Format: "2026-10-21 09:00:00 [INFO] message"
# Using datefmt to strip milliseconds keeps lines concise in terminal output.
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)


def main() -> None:
    """
    Main entry point for the pipeline runner.

    Execution flow
    --------------
    1. Record start time for total elapsed measurement.
    2. Resolve absolute paths for ``data/``, the CSV file, and Parquet output.
    3. Validate the data directory exists (fail-fast if misconfigured).
    4. Generate a synthetic CSV dataset if one is not already present.
    5. Import and call ``run_full_pipeline`` from ``src/spark_pipeline.py``.
    6. Catch and log any unhandled exceptions so the user sees a clean error
       message instead of a raw Python traceback.
    7. Log total elapsed time and helpful guidance for exploring the output.

    Exit codes
    ----------
    * 0 – Pipeline completed successfully.
    * 1 – Unrecoverable error (missing directory or pipeline exception).
    """
    start_time   = time.time()
    project_root = os.path.dirname(os.path.abspath(__file__))
    data_dir     = os.path.join(project_root, "data")
    csv_file     = os.path.join(data_dir, "raw_transactions.csv")
    parquet_dir  = os.path.join(project_root, "output", "analytics_parquet")

    logger.info("=" * 60)
    logger.info("APACHE SPARK BASICS: E-COMMERCE ANALYTICS PIPELINE")
    logger.info("=" * 60)
    logger.info("This script demonstrates end-to-end distributed data processing in Spark.")

    # ------------------------------------------------------------------
    # Pre-flight check: ensure the data directory exists
    # ------------------------------------------------------------------
    # We check for the directory rather than the file so the error message
    # is more actionable — the user likely needs to check their project
    # structure if the directory is missing entirely.
    if not os.path.isdir(data_dir):
        logger.error(
            "Data directory not found: %s  "
            "Please ensure the 'data/' folder exists at the project root.",
            data_dir
        )
        sys.exit(1)

    # ------------------------------------------------------------------
    # Step 1: Ensure the raw CSV dataset exists
    # ------------------------------------------------------------------
    if not os.path.exists(csv_file):
        logger.info("Dataset not found at '%s'.", csv_file)
        logger.info("Generating synthetic e-commerce dataset (5 000 records)...")
        # Lazy import: only needed when the dataset is absent
        from data.generate_dataset import generate_ecommerce_data
        generate_ecommerce_data(csv_file, num_records=5000)
        logger.info("Synthetic dataset generated successfully at: %s", csv_file)
    else:
        file_size_kb = round(os.path.getsize(csv_file) / 1024, 1)
        logger.info("Found existing dataset: %s  (%.1f KB)", csv_file, file_size_kb)

    # ------------------------------------------------------------------
    # Step 2: Import and run the PySpark pipeline
    # ------------------------------------------------------------------
    # Insert src/ directory so Python can find spark_pipeline without
    # requiring a package install step (pip install -e .).
    sys.path.insert(0, os.path.join(project_root, "src"))
    from spark_pipeline import run_full_pipeline

    try:
        run_full_pipeline(csv_path=csv_file, parquet_output_dir=parquet_dir)
    except Exception as exc:
        # Log the full traceback at ERROR level for debugging, then exit cleanly
        logger.error(
            "Pipeline failed with an unexpected error: %s",
            exc,
            exc_info=True
        )
        sys.exit(1)

    # ------------------------------------------------------------------
    # Step 3: Post-run summary
    # ------------------------------------------------------------------
    elapsed = round(time.time() - start_time, 2)
    logger.info("Pipeline completed successfully in %.2f seconds.", elapsed)
    logger.info("-" * 60)
    logger.info("Next steps:")
    logger.info("  1. Read CONCEPT_GUIDE.md to master the theoretical concepts.")
    logger.info("  2. Check 'output/analytics_parquet/' to inspect how Spark partitions data.")
    logger.info("  3. Open http://localhost:4040 in your browser during a run to see the Spark UI.")
    logger.info("  4. Try tweaking spark.sql.shuffle.partitions in create_spark_session().")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()