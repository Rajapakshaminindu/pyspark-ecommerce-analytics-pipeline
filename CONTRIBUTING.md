# Contributing to PySpark E-Commerce Analytics Pipeline

Thank you for your interest in contributing! This document outlines the
conventions, workflow, and code standards for this project.

---

## Table of Contents

1. [Getting Started](#getting-started)
2. [Project Structure](#project-structure)
3. [Development Setup](#development-setup)
4. [Coding Standards](#coding-standards)
5. [Commit Message Convention](#commit-message-convention)
6. [Pull Request Process](#pull-request-process)
7. [Reporting Issues](#reporting-issues)

---

## Getting Started

1. **Fork** this repository on GitHub.
2. **Clone** your fork locally:
   ```bash
   git clone https://github.com/<your-username>/pyspark-ecommerce-analytics-pipeline.git
   cd pyspark-ecommerce-analytics-pipeline
   ```
3. Create a feature branch:
   ```bash
   git checkout -b feature/your-feature-name
   ```

---

## Project Structure

```
pyspark-ecommerce-analytics-pipeline/
├── data/                    # Raw & generated datasets (gitignored large files)
│   └── generate_dataset.py  # Synthetic data generator
├── hadoop/                  # Minimal Hadoop binaries for Windows (winutils)
├── output/                  # Pipeline output directory (gitignored)
├── src/
│   └── spark_pipeline.py    # Main PySpark pipeline logic (all 8 stages)
├── run_pipeline.py          # Project entry-point / orchestrator
├── view_output.py           # Utility to inspect Parquet output files
├── CONCEPT_GUIDE.md         # Deep-dive on Spark architecture concepts
├── GITHUB_ISSUES.md         # Issue tracking notes
├── CONTRIBUTING.md          # This file
└── README.md                # Project overview and quickstart
```

---

## Development Setup

### Prerequisites

| Tool       | Minimum Version | Notes                                    |
|------------|-----------------|------------------------------------------|
| Python     | 3.8+            | 3.10 recommended                         |
| PySpark    | 3.3+            | Install via `pip install pyspark`        |
| Java (JDK) | 11 or 17        | Required by the JVM that Spark runs on   |
| PyArrow    | 10+             | Parquet fallback writer on Windows       |

### Install dependencies

```bash
pip install pyspark pyarrow pandas
```

### Run the pipeline

```bash
# Option 1: Full pipeline (auto-generates data if missing)
python run_pipeline.py

# Option 2: Run the Spark script directly
python src/spark_pipeline.py

# Option 3: Inspect Parquet output after a run
python view_output.py
```

---

## Coding Standards

### Python Style

- Follow **PEP 8** — 4-space indentation, max 99 characters per line.
- Use **type hints** for all public function signatures.
- Write **Google-style docstrings** for every public function, class, and module.

### PySpark-Specific Guidelines

- Always define **explicit schemas** (`StructType`) — never rely on `inferSchema=True`
  in production code.
- Use **bitwise operators** (`&`, `|`, `~`) rather than Python boolean operators
  (`and`, `or`, `not`) when combining Spark `Column` expressions.
- Prefer `spark_round(expr, 2)` alias pattern to avoid conflicts with Python's
  built-in `round`.
- Add a brief inline comment explaining *why* a transformation is applied, not just what.

### Documentation

- Every new function must have a docstring covering:
  - **What** it does (one-line summary).
  - **Why** the approach was chosen (rationale, trade-offs).
  - **Parameters** with types and descriptions.
  - **Returns** with type and description.
  - **Raises** (if applicable).

---

## Commit Message Convention

We follow the [Conventional Commits](https://www.conventionalcommits.org/) standard:

```
<type>(<optional scope>): <short summary>

[optional body — explain WHY, not WHAT]

[optional footer — references, breaking changes]
```

### Types

| Type       | Use when…                                              |
|------------|--------------------------------------------------------|
| `feat`     | Adding a new feature                                   |
| `fix`      | Fixing a bug                                           |
| `docs`     | Documentation-only changes                             |
| `refactor` | Code restructuring without behaviour change            |
| `perf`     | Performance improvement                                |
| `test`     | Adding or updating tests                               |
| `chore`    | Build system, dependency, or tooling changes           |

### Examples

```
feat(pipeline): add hourly revenue trend aggregation stage

Adds a new pipeline stage between window analytics and Spark SQL that
buckets completed orders by hour-of-day and computes total revenue per
bucket. Useful for identifying peak trading hours.

Closes #12
```

```
fix(schema): correct discount_percent field nullability

discount_percent was declared nullable=False but the CSV contains null
values for orders created before the discount system was introduced.
Changed to nullable=True to prevent AnalysisException on ingestion.

Fixes #8
```

---

## Pull Request Process

1. Ensure your branch is up to date with `main`:
   ```bash
   git fetch upstream
   git rebase upstream/main
   ```
2. Run the pipeline end-to-end and confirm no errors:
   ```bash
   python run_pipeline.py
   ```
3. Open a Pull Request against `main` and fill in the PR template.
4. Address all review comments before merging.
5. PRs are squash-merged to keep `main` history clean.

---

## Reporting Issues

When filing a bug report, please include:

- **Python version** (`python --version`)
- **PySpark version** (`pip show pyspark`)
- **OS** (Windows 10/11, macOS, Ubuntu, etc.)
- **Full error traceback** (copy from terminal)
- **Steps to reproduce** (minimal reproduction preferred)

Feature requests are welcome — open a GitHub Issue with the label `enhancement`
and describe the use case clearly.

---

*Happy contributing! 🚀*
