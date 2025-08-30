from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import (
    StructType,
    StringType,
    IntegerType,
    DoubleType,
    TimestampType,
)

# Define schema for transaction data
transaction_schema = (
    StructType()
    .add("store_id", IntegerType())
    .add("product", StringType())
    .add("quantity", IntegerType())
    .add("price", DoubleType())
    .add("total_amount", DoubleType())
    .add("timestamp", StringType())
)

# Initialize Spark session with Iceberg + S3 support
spark = (
    SparkSession.builder.appName("POS_Transaction_Streaming")
    .config("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.my_catalog.type", "hadoop")
    .config(
        "spark.sql.catalog.my_catalog.warehouse",
        "s3a://dk-retail-analytics-lake/warehouse",
    )
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

# Parse Kafka value (JSON)
df_parsed = (
    df_raw.selectExpr("CAST(value AS STRING)")
    .select(from_json(col("value"), transaction_schema).alias("data"))
    .select("data.*")
    .withColumn("event_time", col("timestamp").cast(TimestampType()))
)

# Write to Iceberg (raw layer)
query = (
    df_parsed.writeStream.format("iceberg")
    .outputMode("append")
    .option(
        "checkpointLocation",
        "s3a://dk-retail-analytics-lake/checkpoints/pos_transactions/",
    )
    .option("path", "my_catalog.raw.pos_transactions")
    .start()
)

query.awaitTermination()
