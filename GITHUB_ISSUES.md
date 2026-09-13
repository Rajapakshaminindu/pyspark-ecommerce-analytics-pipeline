# GitHub Issues Guide: Portfolio & Interview Prep

This document contains two realistic issues designed for your GitHub repository.
Having well-documented issues and pull requests on GitHub demonstrates strong engineering practices, business domain awareness, and clean problem-solving skills to recruiters and interviewers.

---

## Issue 1 (Bug Report) — [RESOLVED & FIXED IN CODE]

> **Copy & paste the title and body below when creating Issue #1 on GitHub.**  
> Then link your commit to this issue using the keyword: `Fixes #1`.

### **Issue Title:**
```text
[BUG] Inaccurate financial metrics: CANCELLED and RETURNED orders calculate positive net_revenue in output dataset
```

### **Issue Body (Markdown):**
```markdown
### 🐛 Description of Bug
In `src/spark_pipeline.py`, the `clean_and_transform_data()` function currently calculates `net_revenue` naively as:
`net_revenue = gross_amount - discount_amount`

This formula runs across **all** records without checking the `order_status`. As a result:
- Orders marked as `CANCELLED` still show positive net revenue even though no money was collected.
- Orders marked as `RETURNED` still show positive revenue instead of zero or refunded amounts.
- When `df_cleaned` is exported to the Parquet data lake (`output/analytics_parquet`), any downstream analytics tool (such as PowerBI, Tableau, or Athena) executing `SELECT SUM(net_revenue)` will produce artificially inflated and incorrect financial metrics.

---

### 📋 Steps to Reproduce
1. Ingest raw transactions with statuses `COMPLETED`, `CANCELLED`, `RETURNED`, and `PENDING`.
2. Run `spark_pipeline.py`.
3. Inspect `output/preview_sample.csv` or query the exported Parquet lake.
4. Filter by `order_status == 'CANCELLED'` and observe that `net_revenue > 0`.

---

### 🎯 Expected Behavior
1. `net_revenue` should only be positive for `COMPLETED` orders.
2. If `order_status` is `CANCELLED` or `RETURNED`, `net_revenue` must be set to `0.00`.
3. Introduce an explicit boolean flag column `is_successful_sale = (order_status == 'COMPLETED')` so downstream data consumers can easily filter records without complex string comparisons.

---

### 🛠️ Resolution Implemented
- Updated `src/spark_pipeline.py` using PySpark's conditional `when()` function:
  ```python
  .withColumn(
      "net_revenue",
      when(
          col("order_status") == "COMPLETED",
          spark_round(col("gross_amount") - col("discount_amount"), 2)
      ).otherwise(0.0)
  )
  .withColumn("is_successful_sale", col("order_status") == "COMPLETED")
  ```
- Verified with Catalyst execution plan pushdown (`CASE WHEN (order_status = 'COMPLETED') ...`).
- Re-generated Parquet files and refreshed preview dataset.
```

---

## Issue 2 (Feature / Enhancement) — [OPEN FOR FUTURE ENHANCEMENT]

> **Copy & paste the title and body below when creating Issue #2 on GitHub.**  
> Keep this issue **OPEN** on your repository. It shows interviewers that you think about production scale, data lake architectures, and long-term improvements.

### **Issue Title:**
```text
[FEATURE] Support incremental daily partition loading instead of full dataset overwrite
```

### **Issue Body (Markdown):**
```markdown
### 💡 Feature Proposal
Currently, in `export_partitioned_parquet()`, the pipeline writes to the Parquet directory using:
`.mode("overwrite")`

While this is fine for batch-processing small synthetic datasets, in production enterprise pipelines:
1. Daily transaction volumes can reach tens of millions of rows.
2. Overwriting the entire data lake on every daily run is computationally expensive, slow, and causes high cloud storage I/O costs.
3. Overwriting prevents concurrent reads and disrupts downstream queries.

---

### 🚀 Proposed Solution
1. **Incremental Partition Appends:**
   - Partition data by both `category` and `order_date` (e.g., `output/analytics_parquet/order_date=YYYY-MM-DD/category=...`).
   - Switch the write mode to `.mode("append")` for new dates or implement partition overwriting (`spark.sql.sources.partitionOverwriteMode = dynamic`).
2. **Delta Lake or Iceberg Integration:**
   - Adopt ACID table formats like **Delta Lake** (`delta-spark`) or **Apache Iceberg**.
   - Use `MERGE INTO` (Upsert) to handle late-arriving data and order status updates (e.g., updating a `PENDING` order to `COMPLETED`) without rewriting historical files.

---

### 📌 Impact
- Greatly reduces daily ETL compute time and cost.
- Enables true incremental batch pipelines and continuous historical analytics.
```
