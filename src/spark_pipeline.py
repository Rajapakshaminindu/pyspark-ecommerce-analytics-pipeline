"""
Apache Spark E-Commerce Analytics Pipeline
===========================================
This script demonstrates foundational and intermediate PySpark concepts:
1. SparkSession Initialization & Configuration
2. Schema Enforcement with StructType
3. Narrow Transformations (select, filter, withColumn, string/date functions)
4. Wide Transformations & Aggregations (groupBy, sum, avg, countDistinct)
5. Advanced Window Functions (partitionBy, orderBy, dense_rank, cumulative sum)
6. Spark SQL Integration (Temporary Views & SQL Queries)
7. Query Plan Analysis (.explain() and Catalyst Optimizer)
8. Big Data Lake Export (Partitioned Parquet Format)

Author      : Rajapakshaminindu
Last Updated: 2026-10-23
Version     : 1.2.0

Usage:
    Run directly via: python src/spark_pipeline.py
    Or via runner  : python run_pipeline.py
"""

import os
import sys
import time
from typing import Tuple

# ---------------------------------------------------------------------------
# HADOOP HOME auto-configuration (Windows only)
# ---------------------------------------------------------------------------
# On Windows, PySpark requires winutils.exe in HADOOP_HOME/bin to perform
# filesystem operations. We bundle a minimal hadoop folder in the project root
# and set the environment variable here so users don't need a system-level
# Hadoop installation.
# ---------------------------------------------------------------------------
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
hadoop_home = os.path.join(project_root, "hadoop")
if os.path.exists(hadoop_home):
    os.environ["HADOOP_HOME"] = hadoop_home
    hadoop_bin = os.path.join(hadoop_home, "bin")
    if hadoop_bin not in os.environ.get("PATH", ""):
        os.environ["PATH"] = hadoop_bin + os.pathsep + os.environ.get("PATH", "")

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    TimestampType
)
from pyspark.sql.functions import (
    col,
    round as spark_round,
    when,
    to_date,
    date_format,
    hour,
    count,
    sum as spark_sum,
    avg as spark_avg,
    countDistinct,
    dense_rank,
    desc,
    lit
)
from pyspark.sql.window import Window


# ===========================================================================
# 1.  SPARK SESSION INITIALISATION
# ===========================================================================

def create_spark_session(app_name: str = "EcommerceSparkDemo") -> SparkSession:
    """
    Creates and configures a local SparkSession.

    Configuration notes
    -------------------
    * ``local[*]``  – Uses ALL available CPU cores on the host machine as
      Spark executor threads (equivalent to a single-node "cluster").
    * ``spark.sql.shuffle.partitions = 4`` – The default is 200, which causes
      200 empty partition tasks when shuffling small local datasets.  Setting
      it to 4 (or the number of CPU cores) avoids that overhead.
    * ``spark.ui.showConsoleProgress = false`` – Suppresses the per-task
      progress bars that clutter console output when running interactively.

    Parameters
    ----------
    app_name : str
        Human-readable application name shown in the Spark UI (default port
        4040). Useful when running multiple Spark apps simultaneously.

    Returns
    -------
    SparkSession
        A fully configured, ready-to-use SparkSession instance.
    """
    print("\n[STEP 1] Initializing Apache Spark Session...")
    start_time = time.time()

    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )

    # Reduce Spark's own verbose log output to WARN so our pipeline messages
    # are easy to read in the terminal.
    spark.sparkContext.setLogLevel("WARN")

    elapsed = round(time.time() - start_time, 2)
    print(f" SparkSession created successfully in {elapsed}s")
    print(f"   -> Spark Version : {spark.version}")
    print(f"   -> Master URL    : {spark.sparkContext.master}")
    print(f"   -> Application ID: {spark.sparkContext.applicationId}")
    return spark


# ===========================================================================
# 2.  SCHEMA DEFINITION
# ===========================================================================

def get_transactions_schema() -> StructType:
    """
    Returns the explicit schema for the raw e-commerce transactions CSV.

    Why explicit schema instead of ``inferSchema=True``?
    ----------------------------------------------------
    1. **Performance** – Schema inference requires a full extra scan of the
       dataset before the actual read (2× I/O cost).
    2. **Correctness** – Inference uses heuristics and can silently miscast
       columns (e.g. reading an integer ID as ``LongType`` or a decimal price
       as ``StringType`` when a value contains a comma).
    3. **Contracts** – Explicit schemas act as a data contract between the
       upstream producer and this pipeline, making schema drift immediately
       visible as a read-time error rather than a silent downstream bug.

    Nullable flags
    --------------
    * ``nullable=False`` is set for columns that are *structurally required*
      (``order_id``, ``customer_id``, ``category``, ``product_name``).
    * All other columns are nullable to gracefully handle partial records.

    Returns
    -------
    StructType
        PySpark schema ready to be passed to ``spark.read.schema()``.
    """
    return StructType([
        StructField("order_id",        StringType(),    False),  # Primary key – must not be null
        StructField("customer_id",     StringType(),    False),  # Foreign key to customers table
        StructField("customer_name",   StringType(),    True),   # May be anonymised or absent
        StructField("city",            StringType(),    True),   # Shipping / billing city
        StructField("category",        StringType(),    False),  # Product taxonomy level 1
        StructField("product_name",    StringType(),    False),  # Human-readable product label
        StructField("unit_price",      DoubleType(),    True),   # Price per single unit (USD)
        StructField("quantity",        IntegerType(),   True),   # Number of units ordered
        StructField("discount_percent",DoubleType(),    True),   # Fractional discount (0.0–1.0)
        StructField("payment_method",  StringType(),    True),   # e.g. CREDIT_CARD, PAYPAL, COD
        StructField("order_status",    StringType(),    True),   # COMPLETED | CANCELLED | RETURNED
        StructField("order_timestamp", TimestampType(), True),   # UTC timestamp of order creation
    ])


# ===========================================================================
# 3.  DATA INGESTION
# ===========================================================================

def ingest_raw_data(spark: SparkSession, csv_path: str) -> DataFrame:
    """
    Reads the raw CSV transactions file into a Spark DataFrame.

    Key reader options
    ------------------
    * ``header=true``  – First row is treated as column names (must match
      schema field names exactly, case-insensitively).
    * ``timestampFormat`` – Tells the CSV parser how to parse the
      ``order_timestamp`` column into a proper ``TimestampType`` value.
      Without this, Spark would fall back to ``StringType``.

    Parameters
    ----------
    spark    : SparkSession  Active Spark session.
    csv_path : str           Absolute path to the raw CSV file.

    Returns
    -------
    DataFrame
        Raw (unvalidated) Spark DataFrame with the enforced schema applied.

    Raises
    ------
    AnalysisException
        If the CSV file does not exist or is inaccessible.
    """
    print(f"\n[STEP 2] Ingesting Raw Data from CSV ({os.path.basename(csv_path)})...")
    schema = get_transactions_schema()

    df_raw = (
        spark.read
        .option("header", "true")
        .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
        .schema(schema)
        .csv(csv_path)
    )

    total_raw_rows = df_raw.count()
    print(f" Ingested {total_raw_rows} raw records into Spark DataFrame.")
    print("\n--- Inferred / Enforced Schema Tree ---")
    df_raw.printSchema()

    print("\n--- Sample Raw Records (First 5) ---")
    df_raw.show(5, truncate=False)

    return df_raw


# ===========================================================================
# 4.  DATA CLEANING & NARROW TRANSFORMATIONS
# ===========================================================================

def clean_and_transform_data(df_raw: DataFrame) -> DataFrame:
    """
    Applies a sequence of *narrow transformations* to clean and enrich the
    raw DataFrame.

    Narrow vs. Wide transformations
    --------------------------------
    A **narrow** transformation maps each input partition to exactly one
    output partition — no data moves between executors (no network shuffle).
    Examples: ``filter``, ``select``, ``withColumn``, ``map``.

    A **wide** transformation requires data from *multiple* partitions to be
    combined — triggers a shuffle across the network.
    Examples: ``groupBy``, ``join``, ``distinct``, ``repartition``.

    Transformations applied here
    ----------------------------
    1. **Row-level filtering** – Drops records with non-positive quantity or
       price, and records missing an ``order_id`` (which would break joins).
    2. **Null imputation** – Fills blank/null ``customer_name`` and ``city``
       with sentinel values so aggregations stay correct.
    3. **Derived financial columns**:
       * ``gross_amount``    = unit_price × quantity
       * ``discount_amount`` = gross_amount × discount_percent
       * ``net_revenue``     = gross_amount − discount_amount
         *(only for COMPLETED orders; 0.0 otherwise)*
    4. **Boolean flag** – ``is_successful_sale`` for easy filtering downstream.
    5. **Date-part extraction** – ``order_date``, ``order_month``,
       ``order_hour`` for time-series analytics.

    Parameters
    ----------
    df_raw : DataFrame
        Raw ingested DataFrame (output of ``ingest_raw_data``).

    Returns
    -------
    DataFrame
        Cleaned, enriched DataFrame ready for aggregation and export.
    """
    print("\n[STEP 3] Applying Narrow Transformations (Data Cleaning & Feature Engineering)...")

    # ------------------------------------------------------------------
    # Step 3a: Filter out structurally invalid rows
    # ------------------------------------------------------------------
    # Using bitwise AND (&) instead of Python's logical 'and' because Spark
    # Column objects override __and__ to produce a Column expression, while
    # 'and' would short-circuit on the Python boolean level and fail.
    df_filtered = df_raw.filter(
        (col("quantity") > 0) &
        (col("unit_price") > 0) &
        (col("order_id").isNotNull())
    )

    # ------------------------------------------------------------------
    # Step 3b: Impute missing values and derive calculated columns
    # ------------------------------------------------------------------
    df_transformed = (
        df_filtered
        # Sentinel imputation: replace empty/null strings with a readable label
        .withColumn(
            "customer_name",
            when((col("customer_name") == "") | col("customer_name").isNull(), "Unknown Customer")
            .otherwise(col("customer_name"))
        )
        .withColumn(
            "city",
            when((col("city") == "") | col("city").isNull(), "Unknown City")
            .otherwise(col("city"))
        )
        # Gross amount before discount is applied
        .withColumn("gross_amount",    spark_round(col("unit_price") * col("quantity"), 2))
        # Monetary value of the discount
        .withColumn("discount_amount", spark_round(col("gross_amount") * col("discount_percent"), 2))
        # Net revenue recognised only on completed orders; cancelled / returned
        # orders contribute 0.0 so they don't inflate revenue totals.
        .withColumn(
            "net_revenue",
            when(
                col("order_status") == "COMPLETED",
                spark_round(col("gross_amount") - col("discount_amount"), 2)
            ).otherwise(0.0)
        )
        # Boolean flag – useful for COUNT(is_successful_sale = true) queries
        .withColumn("is_successful_sale", col("order_status") == "COMPLETED")
        # Temporal features for time-series bucketing and trend analysis
        .withColumn("order_date",  to_date(col("order_timestamp")))
        .withColumn("order_month", date_format(col("order_timestamp"), "yyyy-MM"))
        .withColumn("order_hour",  hour(col("order_timestamp")))
    )

    valid_count = df_transformed.count()
    raw_count   = df_raw.count()
    dropped     = raw_count - valid_count

    print(f" Cleaned data contains {valid_count} valid records ({dropped} corrupt/invalid rows dropped).")
    print("\n--- Sample Cleaned Records with Derived Financials ---")
    df_transformed.select(
        "order_id", "customer_name", "order_status", "unit_price",
        "quantity", "gross_amount", "net_revenue", "is_successful_sale"
    ).show(6, truncate=False)

    return df_transformed


# ===========================================================================
# 5.  BUSINESS AGGREGATIONS (WIDE TRANSFORMATIONS)
# ===========================================================================

def compute_business_aggregations(df: DataFrame) -> Tuple[DataFrame, DataFrame]:
    """
    Computes key business KPIs using *wide transformations* (``groupBy`` +
    ``agg``), which trigger a distributed shuffle across Spark partitions.

    Shuffle mechanics
    -----------------
    When Spark executes ``groupBy("category").agg(...)``, it:
    1. **Map phase** – Each partition locally pre-aggregates its own rows
       (partial HashAggregate).
    2. **Exchange (Shuffle)** – Rows with the same key are sent to the same
       downstream partition over the network.
    3. **Reduce phase** – Final aggregation is computed on the received data
       (final HashAggregate).

    This is why ``spark.sql.shuffle.partitions`` matters — it controls how
    many reducer partitions are created after the shuffle.

    Metrics produced
    ----------------
    1. **Category sales summary** — total revenue, orders, units sold, AOV.
    2. **Payment method breakdown** — transaction count, total & average revenue.

    Parameters
    ----------
    df : DataFrame
        Cleaned and enriched DataFrame (output of ``clean_and_transform_data``).

    Returns
    -------
    Tuple[DataFrame, DataFrame]
        ``(category_summary, payment_summary)`` — both are un-cached
        DataFrames; call ``.cache()`` if you need to reuse them downstream.
    """
    print("\n[STEP 4] Executing Wide Transformations (Aggregations & Grouping)...")

    # Only COMPLETED orders contribute to revenue KPIs
    df_completed = df.filter(col("order_status") == "COMPLETED")

    # ------------------------------------------------------------------
    # Metric 1: Category-level sales performance
    # ------------------------------------------------------------------
    print("\n--- Business Metric 1: Category Sales Performance ---")
    category_summary = (
        df_completed
        .groupBy("category")
        .agg(
            spark_round(spark_sum("net_revenue"),  2).alias("total_net_revenue"),
            count("order_id")                       .alias("completed_orders"),
            spark_sum("quantity")                   .alias("total_units_sold"),
            spark_round(spark_avg("net_revenue"),  2).alias("avg_order_value"),
            # Distinct customers per category gives a reach / penetration metric
            countDistinct("customer_id")            .alias("unique_customers"),
        )
        .orderBy(desc("total_net_revenue"))
    )
    category_summary.show(truncate=False)

    # ------------------------------------------------------------------
    # Metric 2: Revenue breakdown by payment method
    # ------------------------------------------------------------------
    print("\n--- Business Metric 2: Revenue by Payment Method ---")
    payment_summary = (
        df_completed
        .groupBy("payment_method")
        .agg(
            count("order_id")                          .alias("transaction_count"),
            spark_round(spark_sum("net_revenue"),   2) .alias("total_revenue"),
            spark_round(spark_avg("net_revenue"),   2) .alias("avg_transaction_value"),
        )
        .orderBy(desc("total_revenue"))
    )
    payment_summary.show(truncate=False)

    return category_summary, payment_summary


# ===========================================================================
# 6.  WINDOW FUNCTIONS
# ===========================================================================

def compute_window_analytics(df: DataFrame) -> DataFrame:
    """
    Demonstrates advanced Spark Window Functions for within-group ranking and
    running aggregates — operations that are impossible with plain ``groupBy``.

    Window function primer
    ----------------------
    A **window** is a logical subset of rows defined by:
    * ``partitionBy`` – Equivalent to GROUP BY; rows with the same key are
      processed together.
    * ``orderBy``     – Defines the row ordering *within* each partition.
    * ``rowsBetween`` / ``rangeBetween`` – Specifies the sliding frame of
      rows included in each calculation (e.g. all preceding rows up to the
      current row for a running total).

    Unlike ``groupBy``, window functions keep the original row count intact —
    every input row emits exactly one output row with the new computed column
    appended.

    Analyses performed
    ------------------
    A. **Top-2 products per category** using ``dense_rank()`` over a window
       partitioned by category, ordered by descending product revenue.
       ``dense_rank`` (vs ``rank``) ensures no gaps in ranking when there are
       ties (e.g. rank 1, 2, 2, 3 rather than 1, 2, 2, 4).

    B. **Customer cumulative spend** using a running ``SUM`` over a window
       ordered by ``order_timestamp`` with an unbounded preceding frame — this
       produces a progressive running total of how much each customer has spent
       from their very first order up to the current row.

    Parameters
    ----------
    df : DataFrame
        Cleaned and enriched DataFrame.

    Returns
    -------
    DataFrame
        ``top_products_per_cat`` — ranked product revenue DataFrame.
    """
    print("\n[STEP 5] Performing Advanced Window Functions...")
    df_completed = df.filter(col("order_status") == "COMPLETED")

    # ------------------------------------------------------------------
    # Analysis A: Top-2 revenue-generating products per category
    # ------------------------------------------------------------------
    print("\n--- Window Metric A: Top 2 Revenue-Generating Products per Category ---")

    # First, aggregate total revenue per (category, product) pair
    product_revenue = (
        df_completed
        .groupBy("category", "product_name")
        .agg(spark_round(spark_sum("net_revenue"), 2).alias("product_revenue"))
    )

    # Window spec: rank products within each category, best first
    category_window = Window.partitionBy("category").orderBy(desc("product_revenue"))

    top_products_per_cat = (
        product_revenue
        .withColumn("rank_in_category", dense_rank().over(category_window))
        .filter(col("rank_in_category") <= 2)          # Keep only the top 2 per category
        .orderBy("category", "rank_in_category")
    )
    top_products_per_cat.show(truncate=False)

    # ------------------------------------------------------------------
    # Analysis B: Customer cumulative spend running total
    # ------------------------------------------------------------------
    print("\n--- Window Metric B: Customer Cumulative Spend (Running Total) ---")

    # Unbounded preceding → current row = "all rows before and including this one"
    customer_window = (
        Window
        .partitionBy("customer_id")
        .orderBy("order_timestamp")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )

    customer_running_spend = (
        df_completed
        .withColumn(
            "running_total_spend",
            spark_round(spark_sum("net_revenue").over(customer_window), 2)
        )
        .select(
            "customer_id", "customer_name", "order_id", "order_timestamp",
            "net_revenue", "running_total_spend"
        )
        # Exclude imputed placeholder names so results show only known customers
        .filter(col("customer_name") != "Unknown Customer")
    )
    customer_running_spend.show(8, truncate=False)

    return top_products_per_cat


# ===========================================================================
# 7.  SPARK SQL INTEGRATION
# ===========================================================================

def demonstrate_spark_sql(spark: SparkSession, df: DataFrame) -> DataFrame:
    """
    Demonstrates Spark SQL by registering a DataFrame as a temporary view and
    running an ANSI-compatible SQL query against Spark's Catalyst engine.

    Temporary View vs. Global Temp View
    ------------------------------------
    * ``createOrReplaceTempView``  – Scoped to the current SparkSession.
      Dropped automatically when the session ends.
    * ``createGlobalTempView``     – Accessible across multiple SparkSessions
      within the same JVM process (prefixed as ``global_temp.<view_name>``).

    Using SQL vs. DataFrame API
    ----------------------------
    Both are semantically equivalent — Spark translates both into the same
    Catalyst logical plan.  SQL is often preferred when:
    * Sharing queries with analysts who know SQL but not Python.
    * Rapidly prototyping multi-step analytical queries.
    * Migrating existing SQL-based ETL logic into Spark.

    Parameters
    ----------
    spark : SparkSession  Active Spark session.
    df    : DataFrame     Cleaned and enriched transaction DataFrame.

    Returns
    -------
    DataFrame
        Result of the top-5 cities by revenue SQL query.
    """
    print("\n[STEP 6] Running Analytical Spark SQL Queries...")

    # Register as a temporary, in-memory view queryable via SQL
    df.createOrReplaceTempView("transactions_view")
    print(" Temporary View 'transactions_view' created in Spark Catalog.")

    sql_query = """
        SELECT
            city,
            COUNT(order_id)           AS total_orders,
            ROUND(SUM(net_revenue),2) AS city_revenue,
            ROUND(AVG(net_revenue),2) AS city_avg_order_value
        FROM   transactions_view
        WHERE  order_status = 'COMPLETED'
          AND  city != 'Unknown City'
        GROUP  BY city
        ORDER  BY city_revenue DESC
        LIMIT  5
    """
    print("\nExecuting SQL Query:\n" + sql_query)
    sql_results = spark.sql(sql_query)
    sql_results.show(truncate=False)

    return sql_results


# ===========================================================================
# 8.  CATALYST QUERY PLAN INSPECTION
# ===========================================================================

def inspect_catalyst_execution_plan(df: DataFrame) -> None:
    """
    Prints the Spark Catalyst physical execution plan for a representative
    aggregation query, illustrating what happens under the hood.

    Reading the plan (bottom → top)
    --------------------------------
    1. ``FileScan csv``     – Reads the source CSV with pushed-down column
       pruning (only required columns are read).
    2. ``Filter``           – Row-level predicate pushed as close to the scan
       as possible (predicate pushdown optimisation).
    3. ``HashAggregate``    – First pass: local pre-aggregation on each
       partition (map-side combine reduces shuffle bytes).
    4. ``Exchange``         – Shuffle: rows are repartitioned by the groupBy
       key so all rows with the same key land in the same partition.
    5. ``HashAggregate``    – Second pass: final aggregation on the shuffled
       data produces the result.

    Parameters
    ----------
    df : DataFrame
        Any cleaned DataFrame (used to construct the sample query).
    """
    print("\n[STEP 7] Inspecting Catalyst Query Execution Plan (.explain())...")

    # Build a simple aggregation query to visualise the plan stages
    sample_query_df = (
        df.filter(col("order_status") == "COMPLETED")
          .groupBy("category")
          .agg(spark_sum("net_revenue").alias("revenue"))
    )

    print("\n=== Spark Physical Execution Plan ===")
    print("Read bottom-to-top: FileScan -> Filter -> HashAggregate -> Exchange -> HashAggregate")
    sample_query_df.explain(mode="simple")


# ===========================================================================
# 9.  PARQUET EXPORT (DATA LAKE SINK)
# ===========================================================================

def export_partitioned_parquet(df: DataFrame, output_dir: str) -> None:
    """
    Writes the cleaned DataFrame to Parquet format, partitioned by ``category``.

    Why Parquet?
    ------------
    * **Columnar storage** – Queries that select a subset of columns read only
      those column files, skipping the rest entirely (columnar pruning).
    * **Compression** – SNAPPY compression reduces storage by ~60–70% vs CSV
      while remaining splittable for parallel reads.
    * **Schema embedding** – Parquet files carry their own schema, so
      downstream readers don't need a separate schema definition.

    Why partition by ``category``?
    ------------------------------
    Partitioning physically separates data into subdirectories
    (``category=Electronics/``, ``category=Clothing/``, etc.).  Future
    queries filtered on ``category`` will skip irrelevant partition
    directories entirely (**partition pruning**), massively reducing I/O on
    large datasets stored in S3, ADLS, or GCS.

    Fallback strategy (Windows)
    ---------------------------
    Spark's native HDFS writer requires ``winutils.exe`` on Windows.  If the
    write fails (e.g. missing winutils), we fall back to PyArrow's dataset
    writer, which achieves the same partitioned Parquet layout without Hadoop.

    Parameters
    ----------
    df         : DataFrame  Cleaned and enriched transaction DataFrame.
    output_dir : str        Target directory path for the Parquet dataset.
    """
    print(f"\n[STEP 8] Exporting Cleaned Data to Partitioned Parquet ({output_dir})...")
    start_time = time.time()

    try:
        # Primary path: use Spark's native Hadoop-based writer
        (
            df
            .write
            .mode("overwrite")          # Overwrite any existing output from previous runs
            .partitionBy("category")    # One subdirectory per product category
            .parquet(output_dir)
        )
        print(" Parquet files written via Spark native Hadoop FileSystem.")

    except Exception as e:
        # Fallback: PyArrow dataset writer (no Hadoop dependency)
        print(f" Spark native write encountered Windows Hadoop limitation ({e}).")
        print(" Using PyArrow to partition and export dataset to Parquet...")
        import pyarrow.parquet as pq
        import pyarrow as pa

        # Convert Spark DataFrame → Pandas → Arrow Table
        pdf   = df.toPandas()
        table = pa.Table.from_pandas(pdf)
        pq.write_to_dataset(
            table,
            root_path=output_dir,
            partition_cols=["category"],
            use_dictionary=True,
            compression="SNAPPY"
        )
        print(" Parquet files written via PyArrow dataset engine.")

    elapsed = round(time.time() - start_time, 2)
    print(f" Data exported to Parquet partitioned by category in {elapsed}s.")

    # Show partition directory structure for confirmation
    if os.path.exists(output_dir):
        partitions = [f for f in os.listdir(output_dir) if f.startswith("category=")]
        print(f" Created {len(partitions)} partition folders:")
        for part in sorted(partitions):
            print(f"   |-- {part}/")


# ===========================================================================
# 10. PIPELINE ORCHESTRATOR
# ===========================================================================

def run_full_pipeline(csv_path: str, parquet_output_dir: str) -> None:
    """
    Top-level orchestrator that executes all pipeline stages in sequence.

    Pipeline stages
    ---------------
    1.  ``create_spark_session``          → Initialise Spark
    2.  ``ingest_raw_data``               → Read CSV with explicit schema
    3.  ``clean_and_transform_data``      → Validate, impute, and enrich rows
    4.  ``compute_business_aggregations`` → Category & payment KPIs (wide)
    5.  ``compute_window_analytics``      → Ranking & running totals (window)
    6.  ``demonstrate_spark_sql``         → SQL query via temp view
    7.  ``inspect_catalyst_execution_plan`` → Print physical plan
    8.  ``export_partitioned_parquet``    → Write Parquet data lake sink

    Error handling
    --------------
    A ``try/finally`` block guarantees ``spark.stop()`` is called even when
    an exception propagates up.  Failing to stop Spark leaks JVM threads and
    ports that can interfere with subsequent pipeline runs in the same process.

    Parameters
    ----------
    csv_path           : str  Path to the raw transactions CSV file.
    parquet_output_dir : str  Target directory for the Parquet export.
    """
    spark = create_spark_session()

    try:
        # Stage 2: Ingest
        df_raw = ingest_raw_data(spark, csv_path)

        # Stage 3: Clean & enrich (narrow transformations)
        df_cleaned = clean_and_transform_data(df_raw)

        # Stage 4: Business KPIs (wide transformations / shuffle)
        compute_business_aggregations(df_cleaned)

        # Stage 5: Window-based analytics (ranking & running totals)
        compute_window_analytics(df_cleaned)

        # Stage 6: Spark SQL integration
        demonstrate_spark_sql(spark, df_cleaned)

        # Stage 7: Catalyst physical plan inspection
        inspect_catalyst_execution_plan(df_cleaned)

        # Stage 8: Persist to partitioned Parquet (data lake sink)
        export_partitioned_parquet(df_cleaned, parquet_output_dir)

        print("\n" + "=" * 70)
        print(" PIPELINE EXECUTION COMPLETED SUCCESSFULLY!")
        print("=" * 70)

    finally:
        # Always release JVM resources regardless of success or failure
        print("\nStopping SparkSession...")
        spark.stop()
        print(" SparkSession terminated cleanly.")


# ===========================================================================
# ENTRY POINT
# ===========================================================================

if __name__ == "__main__":
    base_dir    = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    data_csv    = os.path.join(base_dir, "data",   "raw_transactions.csv")
    parquet_out = os.path.join(base_dir, "output", "analytics_parquet")

    run_full_pipeline(data_csv, parquet_out)
