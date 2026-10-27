# Changelog

All notable changes to **PySpark E-Commerce Analytics Pipeline** are documented here.

This project adheres to [Semantic Versioning](https://semver.org/) and the
[Conventional Commits](https://www.conventionalcommits.org/) specification.

---

## [Unreleased]

> Changes staged for the next release.

### Planned
- Add `pytest` test suite covering schema validation and revenue calculation logic.
- Add `view_output.py` inline documentation and category-level summary table.
- Explore Delta Lake integration for ACID-compliant data lake writes.

---

## [1.2.0] — 2026-10-22

### Added
- `CONTRIBUTING.md` — coding standards, commit convention, PR workflow, and
  issue reporting guidelines.
- `CHANGELOG.md` — this file, tracking all project changes by version.
- Module-level docstring to `run_pipeline.py` with versioned changelog block,
  logging strategy explanation, and author metadata.
- Detailed `main()` docstring documenting execution flow and exit codes.
- File size logging when an existing CSV dataset is detected at startup.
- Spark UI URL hint (`http://localhost:4040`) in post-run next-steps guidance.

### Changed
- `src/spark_pipeline.py` — expanded all function docstrings to include
  rationale, parameter/return types, and conceptual explanations.
- `run_pipeline.py` — replaced bare `print` calls with structured `logging`
  (see v1.1.0 notes); improved error messages to be more actionable.
- Added `countDistinct("customer_id")` to `compute_business_aggregations` to
  expose unique-customer count per category as an additional KPI.
- Improved inline comments throughout `clean_and_transform_data` explaining
  the bitwise `&` operator requirement for Spark Column expressions.

---

## [1.1.0] — 2026-10-21 *(logging & error handling release)*

### Added
- Structured logging via Python's `logging` module in `run_pipeline.py`
  (replaces raw `print` statements).  Format includes timestamp + log level.
- Error handling in `run_pipeline.py`: catches pipeline exceptions, logs a
  full traceback at ERROR level, and exits with code `1`.
- Data-directory existence pre-flight check — exits early with a clear error
  message if `data/` is missing rather than propagating a cryptic Spark error.
- Wall-clock runtime tracking: total elapsed seconds logged on success.
- Informative next-step hints logged after a successful pipeline run.

### Changed
- `run_pipeline.py` refactored from a flat script to a `main()` function with
  a guard (`if __name__ == "__main__"`) for testability.

---

## [1.0.1] — 2026-10-21 *(revenue calculation fix)*

### Fixed
- **Critical**: `net_revenue` was calculated for ALL order statuses, causing
  CANCELLED and RETURNED orders to inflate revenue totals.
- `clean_and_transform_data` now applies a `when(order_status == "COMPLETED")`
  guard so that only completed sales contribute to `net_revenue`; all other
  statuses receive `0.0`.

---

## [1.0.0] — 2026-10-20 *(initial release)*

### Added
- Full 8-stage PySpark pipeline demonstrating core distributed computing
  concepts:
  1. SparkSession initialisation with Windows Hadoop auto-configuration.
  2. Explicit schema enforcement with `StructType` / `StructField`.
  3. Narrow transformations: filtering, null imputation, financial derivations.
  4. Wide transformations: category sales KPIs and payment method breakdown.
  5. Window functions: top-2 products per category and customer running total.
  6. Spark SQL: temporary view registration and ANSI SQL queries.
  7. Catalyst execution plan inspection via `.explain(mode="simple")`.
  8. Partitioned Parquet export with PyArrow fallback for Windows.
- `data/generate_dataset.py` — synthetic e-commerce dataset generator
  (configurable record count, realistic distributions).
- `README.md` — project overview, quickstart guide, and concept summary.
- `CONCEPT_GUIDE.md` — in-depth theoretical guide covering Spark architecture,
  RDD internals, DAG execution, and the Catalyst optimiser.
- `GITHUB_ISSUES.md` — issue tracking notes for the project.
- `hadoop/bin/winutils.exe` — minimal Hadoop binary bundle for Windows.
- `.gitignore` — excludes `output/`, `__pycache__/`, `.venv/`, etc.

---

[Unreleased]: https://github.com/Rajapakshaminindu/pyspark-ecommerce-analytics-pipeline/compare/v1.2.0...HEAD
[1.2.0]: https://github.com/Rajapakshaminindu/pyspark-ecommerce-analytics-pipeline/compare/v1.1.0...v1.2.0
[1.1.0]: https://github.com/Rajapakshaminindu/pyspark-ecommerce-analytics-pipeline/compare/v1.0.1...v1.1.0
[1.0.1]: https://github.com/Rajapakshaminindu/pyspark-ecommerce-analytics-pipeline/compare/v1.0.0...v1.0.1
[1.0.0]: https://github.com/Rajapakshaminindu/pyspark-ecommerce-analytics-pipeline/releases/tag/v1.0.0
