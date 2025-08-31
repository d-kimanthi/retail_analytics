from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructType,
    TimestampType,
)

# Initialize Spark session with Iceberg + AWS Glue support
spark = (
    SparkSession.builder.appName("Init_Iceberg_Tables")
    .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
    .config(
        "spark.sql.catalog.glue_catalog.catalog-impl",
        "org.apache.iceberg.aws.glue.GlueCatalog",
    )
    .config(
        "spark.sql.catalog.glue_catalog.warehouse",
        "s3a://dk-retail-analytics-lake/warehouse",
    )
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    )
    .config("spark.sql.iceberg.handle-timestamp-without-timezone", "true")
    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
    )
    .getOrCreate()
)

# Create database if not exists
spark.sql("CREATE DATABASE IF NOT EXISTS glue_catalog.raw")
spark.sql("CREATE DATABASE IF NOT EXISTS glue_catalog.marts")

# Define schema for raw transactions table
transaction_schema = (
    StructType()
    .add("transaction_id", StringType())
    .add("store_id", StringType())
    .add("event_time", TimestampType())
    .add("product_id", StringType())
    .add("product_name", StringType())
    .add("category", StringType())
    .add("quantity", IntegerType())
    .add("unit_price", DoubleType())
    .add("total_amount", DoubleType())
    .add("payment_method", StringType())
    .add("customer_id", StringType())
)

# Create raw transactions table
spark.sql(
    """
    CREATE TABLE IF NOT EXISTS glue_catalog.raw.pos_transactions (
        transaction_id STRING,
        store_id STRING,
        event_time TIMESTAMP,
        product_id STRING,
        product_name STRING,
        category STRING,
        quantity INT,
        unit_price DOUBLE,
        total_amount DOUBLE,
        payment_method STRING,
        customer_id STRING
    )
    USING iceberg
    PARTITIONED BY (days(event_time))
"""
)

# Create daily sales summary table
spark.sql(
    """
    CREATE TABLE IF NOT EXISTS glue_catalog.marts.daily_sales_summary (
        store_id STRING,
        sale_date DATE,
        product_id STRING,
        product_name STRING,
        category STRING,
        total_quantity INT,
        total_sales DOUBLE,
        transaction_count INT
    )
    USING iceberg
    PARTITIONED BY (sale_date)
"""
)

print("Tables created successfully!")
spark.stop()
