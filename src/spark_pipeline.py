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
"""

import os
import sys
import time

# Auto-configure HADOOP_HOME for Windows environments
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
hadoop_home = os.path.join(project_root, "hadoop")
if os.path.exists(hadoop_home):
    os.environ["HADOOP_HOME"] = hadoop_home
    hadoop_bin = os.path.join(hadoop_home, "bin")
    if hadoop_bin not in os.environ.get("PATH", ""):
        os.environ["PATH"] = hadoop_bin + os.pathsep + os.environ.get("PATH", "")

from pyspark.sql import SparkSession
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
    desc
)
from pyspark.sql.window import Window


def create_spark_session(app_name: str = "EcommerceSparkDemo") -> SparkSession:
    """
    Creates and configures a local SparkSession.
    - local[*]: Uses all available CPU cores on your machine as workers.
    - spark.sql.shuffle.partitions: Set to 4 (default is 200, which creates too many
      empty partition tasks for small local datasets).
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
    
    # Adjust log level to WARN to minimize verbose INFO logging in the terminal
    spark.sparkContext.setLogLevel("WARN")
    
    print(f" SparkSession created successfully in {round(time.time() - start_time, 2)}s")
    print(f"   -> Spark Version : {spark.version}")
    print(f"   -> Master URL    : {spark.sparkContext.master}")
    print(f"   -> Application ID: {spark.sparkContext.applicationId}")
    return spark


def get_transactions_schema() -> StructType:
    """
    Explicit Schema Definition.
    In production environments, always define explicit schemas instead of using
    inferSchema=True because:
    1. Schema inference scans the entire dataset once before reading (2x I/O).
    2. Explicit schemas enforce strict data contracts and prevent silent type casting bugs.
    """
    return StructType([
        StructField("order_id", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("customer_name", StringType(), True),
        StructField("city", StringType(), True),
        StructField("category", StringType(), False),
        StructField("product_name", StringType(), False),
        StructField("unit_price", DoubleType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("discount_percent", DoubleType(), True),
        StructField("payment_method", StringType(), True),
        StructField("order_status", StringType(), True),
        StructField("order_timestamp", TimestampType(), True)
    ])


def ingest_raw_data(spark: SparkSession, csv_path: str):
    """
    Reads the raw CSV data into a Spark DataFrame using the explicit schema.
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


def clean_and_transform_data(df_raw):
    """
    Demonstrates Narrow Transformations:
    - filter(): Drops bad / dirty data without moving data between partitions.
    - withColumn(): Derives calculated fields, parses dates, and replaces nulls.
    """
    print("\n[STEP 3] Applying Narrow Transformations (Data Cleaning & Feature Engineering)...")
    
    # 1. Filter out invalid quantities or prices (Data Quality Check)
    df_filtered = df_raw.filter(
        (col("quantity") > 0) & 
        (col("unit_price") > 0) &
        (col("order_id").isNotNull())
    )
    
    # 2. Impute null customer_name and city (Handling Missing Values)
    # 3. Derive financial columns:
    #    gross_amount = unit_price * quantity
    #    discount_amount = gross_amount * discount_percent
    #    net_revenue = gross_amount - discount_amount
    # 4. Extract date parts for temporal analytics
    df_transformed = (
        df_filtered
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
        .withColumn("gross_amount", spark_round(col("unit_price") * col("quantity"), 2))
        .withColumn("discount_amount", spark_round(col("gross_amount") * col("discount_percent"), 2))
        .withColumn("net_revenue", spark_round(col("gross_amount") - col("discount_amount"), 2))
        .withColumn("order_date", to_date(col("order_timestamp")))
        .withColumn("order_month", date_format(col("order_timestamp"), "yyyy-MM"))
        .withColumn("order_hour", hour(col("order_timestamp")))
    )
    
    valid_count = df_transformed.count()
    raw_count = df_raw.count()
    dropped = raw_count - valid_count
    
    print(f" Cleaned data contains {valid_count} valid records ({dropped} corrupt/invalid rows dropped).")
    print("\n--- Sample Cleaned Records with Derived Financials ---")
    df_transformed.select(
        "order_id", "customer_name", "category", "product_name", 
        "unit_price", "quantity", "gross_amount", "net_revenue", "order_month"
    ).show(5, truncate=False)
    
    return df_transformed


def compute_business_aggregations(df):
    """
    Demonstrates Wide Transformations:
    - groupBy() and agg(): Involves Shuffling data across partitions.
    """
    print("\n[STEP 4] Executing Wide Transformations (Aggregations & Grouping)...")
    
    # Filter for completed sales
    df_completed = df.filter(col("order_status") == "COMPLETED")
    
    # 1. Performance by Category
    print("\n--- Business Metric 1: Category Sales Performance ---")
    category_summary = (
        df_completed
        .groupBy("category")
        .agg(
            spark_round(spark_sum("net_revenue"), 2).alias("total_net_revenue"),
            count("order_id").alias("completed_orders"),
            spark_sum("quantity").alias("total_units_sold"),
            spark_round(spark_avg("net_revenue"), 2).alias("avg_order_value")
        )
        .orderBy(desc("total_net_revenue"))
    )
    category_summary.show(truncate=False)
    
    # 2. Payment Method Popularity & Revenue
    print("\n--- Business Metric 2: Revenue by Payment Method ---")
    payment_summary = (
        df_completed
        .groupBy("payment_method")
        .agg(
            count("order_id").alias("transaction_count"),
            spark_round(spark_sum("net_revenue"), 2).alias("total_revenue"),
            spark_round(spark_avg("net_revenue"), 2).alias("avg_transaction_value")
        )
        .orderBy(desc("total_revenue"))
    )
    payment_summary.show(truncate=False)
    
    return category_summary, payment_summary


def compute_window_analytics(df):
    """
    Demonstrates Advanced Spark Window Functions:
    - Window.partitionBy(): Groups rows without collapsing them into a single row.
    - dense_rank(): Ranks products within each category by revenue.
    - Running cumulative revenue per customer.
    """
    print("\n[STEP 5] Performing Advanced Window Functions...")
    df_completed = df.filter(col("order_status") == "COMPLETED")
    
    # Analysis A: Top 2 best-selling products in EACH product category
    print("\n--- Window Metric A: Top 2 Revenue-Generating Products per Category ---")
    product_revenue = (
        df_completed
        .groupBy("category", "product_name")
        .agg(spark_round(spark_sum("net_revenue"), 2).alias("product_revenue"))
    )
    
    category_window = Window.partitionBy("category").orderBy(desc("product_revenue"))
    
    top_products_per_cat = (
        product_revenue
        .withColumn("rank_in_category", dense_rank().over(category_window))
        .filter(col("rank_in_category") <= 2)
        .orderBy("category", "rank_in_category")
    )
    top_products_per_cat.show(truncate=False)
    
    # Analysis B: Customer Cumulative Spend Running Total
    print("\n--- Window Metric B: Customer Cumulative Spend (Running Total) ---")
    customer_window = (
        Window
        .partitionBy("customer_id")
        .orderBy("order_timestamp")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
    
    customer_running_spend = (
        df_completed
        .withColumn("running_total_spend", spark_round(spark_sum("net_revenue").over(customer_window), 2))
        .select(
            "customer_id", "customer_name", "order_id", "order_timestamp",
            "net_revenue", "running_total_spend"
        )
        .filter(col("customer_name") != "Unknown Customer")
    )
    customer_running_spend.show(8, truncate=False)
    
    return top_products_per_cat


def demonstrate_spark_sql(spark: SparkSession, df):
    """
    Demonstrates Spark SQL:
    - Registers a DataFrame as a Temporary View.
    - Runs ANSI SQL queries directly against Spark's Catalyst engine.
    """
    print("\n[STEP 6] Running Analytical Spark SQL Queries...")
    
    # Register DataFrame as a Temporary In-Memory Table
    df.createOrReplaceTempView("transactions_view")
    print(" Temporary View 'transactions_view' created in Spark Catalog.")
    
    sql_query = """
        SELECT 
            city,
            COUNT(order_id) AS total_orders,
            ROUND(SUM(net_revenue), 2) AS city_revenue,
            ROUND(AVG(net_revenue), 2) AS city_avg_order_value
        FROM transactions_view
        WHERE order_status = 'COMPLETED' AND city != 'Unknown City'
        GROUP BY city
        ORDER BY city_revenue DESC
        LIMIT 5
    """
    print("\nExecuting SQL Query:\n" + sql_query)
    sql_results = spark.sql(sql_query)
    sql_results.show(truncate=False)
    
    return sql_results


def inspect_catalyst_execution_plan(df):
    """
    Explains the Catalyst Optimizer & Physical Execution Plan:
    Reveals what Spark actually does under the hood (FileScan, Filter, HashAggregate, Exchange/Shuffle).
    """
    print("\n[STEP 7] Inspecting Catalyst Query Execution Plan (.explain())...")
    
    # Create an aggregation query DataFrame
    sample_query_df = (
        df.filter(col("order_status") == "COMPLETED")
        .groupBy("category")
        .agg(spark_sum("net_revenue").alias("revenue"))
    )
    
    print("\n=== Spark Physical Execution Plan ===")
    print("Notice the stages below: FileScan -> Filter -> HashAggregate -> Exchange (Shuffle) -> HashAggregate")
    sample_query_df.explain(mode="simple")


def export_partitioned_parquet(df, output_dir: str):
    """
    Demonstrates Enterprise Storage Practices:
    - Writing to Columnar Parquet format.
    - Partitioning by 'category' to enable partition pruning on future analytical queries.
    """
    print(f"\n[STEP 8] Exporting Cleaned Data to Partitioned Parquet ({output_dir})...")
    start_time = time.time()
    
    try:
        (
            df
            .write
            .mode("overwrite")
            .partitionBy("category")
            .parquet(output_dir)
        )
        print(" Parquet files written via Spark native Hadoop FileSystem.")
    except Exception as e:
        print(f" Spark native write encountered Windows Hadoop limitation ({e}).")
        print(" Using PyArrow to partition and export dataset to Parquet...")
        import pyarrow.parquet as pq
        import pyarrow as pa
        
        # Convert Spark DataFrame to Pandas / Arrow and partition by category
        pdf = df.toPandas()
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
    
    # Inspect output directory structure
    if os.path.exists(output_dir):
        partitions = [f for f in os.listdir(output_dir) if f.startswith("category=")]
        print(f" Created {len(partitions)} partition folders:")
        for part in partitions:
            print(f"   |-- {part}/")


def run_full_pipeline(csv_path: str, parquet_output_dir: str):
    """
    Main orchestrator running all parts in sequence.
    """
    spark = create_spark_session()
    
    try:
        # Ingestion
        df_raw = ingest_raw_data(spark, csv_path)
        
        # Cleaning & Narrow Transformations
        df_cleaned = clean_and_transform_data(df_raw)
        
        # Business Aggregations (Wide Transformations)
        compute_business_aggregations(df_cleaned)
        
        # Window Functions
        compute_window_analytics(df_cleaned)
        
        # Spark SQL
        demonstrate_spark_sql(spark, df_cleaned)
        
        # Query Optimization Plan
        inspect_catalyst_execution_plan(df_cleaned)
        
        # Export to Data Lake
        export_partitioned_parquet(df_cleaned, parquet_output_dir)
        
        print("\n" + "="*70)
        print(" PIPELINE EXECUTION COMPLETED SUCCESSFULLY!")
        print("="*70)
        
    finally:
        # Always terminate the SparkSession to release JVM memory and background threads
        print("\nStopping SparkSession...")
        spark.stop()
        print(" SparkSession terminated cleanly.")


if __name__ == "__main__":
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    data_csv = os.path.join(base_dir, "data", "raw_transactions.csv")
    parquet_out = os.path.join(base_dir, "output", "analytics_parquet")
    
    run_full_pipeline(data_csv, parquet_out)
