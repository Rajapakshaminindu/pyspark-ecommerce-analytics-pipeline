# Apache Spark E-Commerce Analytics Pipeline

A hands-on Apache Spark (PySpark) project demonstrating foundational and intermediate distributed data processing, query optimization, and enterprise data lake practices.

---

## What This Project Demonstrates

1. **SparkSession Initialization**:
   - Creating the entry point with local cluster mode (`local[*]`) and tuning shuffle partitions (`spark.sql.shuffle.partitions`).
2. **Explicit Schema Enforcement (`StructType`)**:
   - Avoiding costly `inferSchema` scans and enforcing strict schema contracts.
3. **Narrow vs. Wide Transformations**:
   - **Narrow**: Column derivations (`unit_price * quantity`), discount calculations, timestamp parsing, handling nulls.
   - **Wide (Shuffle)**: `groupBy()` aggregations (Total revenue, order counts, average order value by category and payment method).
4. **Advanced Window Functions (`pyspark.sql.window.Window`)**:
   - Ranking top 2 best-selling products per category using `dense_rank()`.
   - Calculating running cumulative spend per customer over time without collapsing rows.
5. **Spark SQL Integration**:
   - Registering Spark DataFrames as temporary views (`createOrReplaceTempView`).
   - Running ANSI SQL queries directly against Spark's Catalyst engine.
6. **Catalyst Query Plan Inspection**:
   - Using `.explain()` to inspect physical execution stages: `FileScan`, `Filter`, `HashAggregate` (partial and final), and `Exchange` (Shuffle).
7. **Columnar Data Lake Storage**:
   - Writing data partitioned by `category` in columnar **Parquet** format with Snappy compression for query pruning.

---

## Project Structure

```
spark-basics-project/
├── data/
│   ├── generate_dataset.py       # Generates 5,000 synthetic transaction records
│   └── raw_transactions.csv      # Raw CSV transaction dataset
├── hadoop/bin/                   # Windows Hadoop binaries (winutils.exe, hadoop.dll)
├── src/
│   └── spark_pipeline.py         # Complete PySpark pipeline with all steps
├── output/
│   ├── analytics_parquet/        # Partitioned Parquet data lake output
│   └── preview_sample.csv        # Quick-preview CSV export
├── CONCEPT_GUIDE.md              # In-depth guide explaining all Spark concepts
├── run_pipeline.py               # Main entry point to run everything
├── view_output.py                # Inspect Parquet partitions & dataset summary
└── README.md                     # Project overview and instructions
```

---

## Quick Start

### 1. Run the Pipeline
Open your terminal in this directory and execute:
```bash
python run_pipeline.py
```

This will automatically:
1. Validate or generate the synthetic dataset.
2. Launch the PySpark pipeline.
3. Execute all transformations and analytical metrics.
4. Output partitioned Parquet files to `output/analytics_parquet/`.

### 2. Inspect Parquet Output
```bash
python view_output.py
```
This reads the partitioned Parquet lakehouse files, displays partition counts and summary statistics, and saves a 50-row preview to `output/preview_sample.csv`.

---

## Deep Dive Learning Guide

For a thorough breakdown of:
- What Spark is doing under the hood
- Driver vs. Executor architecture
- Why Lazy Evaluation is powerful
- The mechanics of Data Shuffling
- How the Catalyst Optimizer works
- Top 5 Spark interview questions & answers

👉 Check out the [CONCEPT_GUIDE.md](CONCEPT_GUIDE.md) document included in this repository!
