from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_date
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructType,
    TimestampType,
)

# Define schema for transaction data
transaction_schema = (
    StructType()
    .add("transaction_id", StringType())
    .add("store_id", StringType())
    .add("event_time", StringType())
    .add("product_id", StringType())
    .add("product_name", StringType())
    .add("category", StringType())
    .add("quantity", IntegerType())
    .add("unit_price", DoubleType())
    .add("total_amount", DoubleType())
    .add("payment_method", StringType())
    .add("customer_id", StringType())
)

# Initialize Spark session with Iceberg + AWS Glue support
spark = (
    SparkSession.builder.appName("POS_Transaction_Streaming")
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

spark.sparkContext.setLogLevel("WARN")

# Read from Kafka topic
df_raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "pos_transactions")
    .option("startingOffsets", "latest")
    .load()
)

# Parse Kafka value (JSON) and prepare for writing
df_parsed = (
    df_raw.selectExpr("CAST(value AS STRING)")
    .select(from_json(col("value"), transaction_schema).alias("data"))
    .select("data.*")
    .withColumn("event_time", col("event_time").cast(TimestampType()))
)

# Write to Iceberg table in the raw layer
query = (
    df_parsed.writeStream.format("iceberg")
    .outputMode("append")
    .option(
        "checkpointLocation",
        "s3a://dk-retail-analytics-lake/checkpoints/pos_transactions/",
    )
    .option("path", "glue_catalog.raw.pos_transactions")
    .partitionBy("event_time")  # Partition by event_time as specified
    .trigger(processingTime="1 minute")  # Process micro-batches every minute
    .start()
)

# Wait for the streaming query to terminate
query.awaitTermination()
