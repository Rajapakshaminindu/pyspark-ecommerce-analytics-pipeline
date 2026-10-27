# Apache Spark E-Commerce Analytics Pipeline

[![Python](https://img.shields.io/badge/Python-3.8%2B-blue?logo=python)](https://www.python.org/)
[![PySpark](https://img.shields.io/badge/PySpark-3.3%2B-orange?logo=apachespark)](https://spark.apache.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

A hands-on Apache Spark (PySpark) project demonstrating foundational and
intermediate distributed data processing, query optimisation, and enterprise
data lake practices — built around a realistic e-commerce transactions dataset.

---

## What This Project Demonstrates

| # | Concept | Key APIs |
|---|---------|----------|
| 1 | **SparkSession Initialisation** | `SparkSession.builder`, `local[*]`, shuffle partitions config |
| 2 | **Explicit Schema Enforcement** | `StructType`, `StructField`, nullable flags |
| 3 | **Narrow Transformations** | `filter`, `withColumn`, `when`, `to_date`, `date_format` |
| 4 | **Wide Transformations (Shuffle)** | `groupBy`, `agg`, `sum`, `avg`, `countDistinct` |
| 5 | **Window Functions** | `Window.partitionBy`, `dense_rank`, `rowsBetween`, running totals |
| 6 | **Spark SQL Integration** | `createOrReplaceTempView`, `spark.sql()` |
| 7 | **Catalyst Plan Inspection** | `.explain(mode="simple")` |
| 8 | **Columnar Data Lake Export** | Partitioned Parquet, SNAPPY compression, PyArrow fallback |

---

## Project Structure

```
pyspark-ecommerce-analytics-pipeline/
├── data/
│   ├── generate_dataset.py       # Synthetic 5 000-record transaction generator
│   └── raw_transactions.csv      # Auto-generated raw dataset (gitignored if large)
├── hadoop/
│   └── bin/                      # Minimal Hadoop binaries for Windows (winutils.exe)
├── src/
│   └── spark_pipeline.py         # Full 8-stage PySpark pipeline with docstrings
├── output/
│   ├── analytics_parquet/        # Partitioned Parquet data lake (gitignored)
│   └── preview_sample.csv        # 50-row quick-preview CSV
├── CHANGELOG.md                  # Versioned change history
├── CONCEPT_GUIDE.md              # Deep-dive Spark architecture guide
├── CONTRIBUTING.md               # How to contribute to this project
├── GITHUB_ISSUES.md              # Issue tracking notes
├── run_pipeline.py               # Project entry-point / orchestrator
├── view_output.py                # Parquet output inspector
└── README.md                     # This file
```

---

## Quick Start

### Prerequisites

| Tool       | Minimum Version | Install |
|------------|-----------------|---------|
| Python     | 3.8+            | [python.org](https://www.python.org/downloads/) |
| Java (JDK) | 11 or 17        | [adoptium.net](https://adoptium.net/) |
| PySpark    | 3.3+            | `pip install pyspark` |
| PyArrow    | 10+             | `pip install pyarrow` |

```bash
# Install all Python dependencies
pip install pyspark pyarrow pandas
```

### 1. Run the Full Pipeline

```bash
python run_pipeline.py
```

This automatically:
1. Validates the `data/` directory exists.
2. Generates a synthetic CSV dataset (5 000 records) if none is found.
3. Launches PySpark and executes all 8 pipeline stages.
4. Writes partitioned Parquet files to `output/analytics_parquet/`.

### 2. Inspect Parquet Output

```bash
python view_output.py
```

Reads the partitioned lakehouse files, shows partition counts and summary
statistics, and saves a 50-row preview to `output/preview_sample.csv`.

### 3. Monitor via Spark UI

While the pipeline is running, open your browser at:
```
http://localhost:4040
```
to inspect DAG stages, task timelines, and shuffle read/write metrics.

---

## Key Pipeline Stages Explained

### Stage 3 — Narrow Transformations (Data Cleaning)

Filters invalid rows (non-positive quantity/price, null order IDs), imputes
missing `customer_name` and `city` values, and derives three financial columns:

```
gross_amount    = unit_price × quantity
discount_amount = gross_amount × discount_percent
net_revenue     = gross_amount − discount_amount   (COMPLETED orders only)
```

### Stage 4 — Wide Transformations (Business KPIs)

Triggers a distributed **shuffle** to compute revenue, order counts, average
order value, and unique customers per product category, and total revenue
breakdown by payment method.

### Stage 5 — Window Functions

Uses `dense_rank()` over a `Window.partitionBy("category")` to find the
**top-2 revenue-generating products per category** without losing individual
row detail. Separately computes a **running cumulative spend per customer**
using an unbounded-preceding frame.

### Stage 8 — Partitioned Parquet Export

Writes the cleaned dataset as columnar Parquet files partitioned by `category`
(e.g. `category=Electronics/`, `category=Clothing/`). Future queries filtered
on a single category will skip all other partitions entirely (**partition
pruning**).

---

## Learning Resources

For a thorough breakdown of Spark internals, check the included docs:

| Resource | Contents |
|----------|----------|
| [`CONCEPT_GUIDE.md`](CONCEPT_GUIDE.md) | Driver vs. Executor, Lazy Evaluation, Catalyst Optimiser, top interview Q&A |
| [`CONTRIBUTING.md`](CONTRIBUTING.md) | Dev setup, coding standards, commit conventions, PR process |
| [`CHANGELOG.md`](CHANGELOG.md) | Full version history with Added / Fixed / Changed sections |

---

## Contributing

Contributions are welcome! Please read [`CONTRIBUTING.md`](CONTRIBUTING.md) for
coding standards, the commit message convention, and the PR workflow before
opening a pull request.

---

## License

This project is released under the [MIT License](LICENSE).
