from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, sum as _sum

# Initialize Spark session with Iceberg and AWS S3
spark = (
    SparkSession.builder.appName("BuildDailySalesMart")
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
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# Read from raw Iceberg table
df = spark.read.format("iceberg").load("my_catalog.raw.pos_transactions")

# Extract sale date and group
df_grouped = (
    df.withColumn("sale_date", to_date(col("event_time")))
    .groupBy("store_id", "product", "sale_date")
    .agg(
        _sum("quantity").alias("total_quantity"),
        _sum("total_amount").alias("total_sales"),
    )
)

# Write to Iceberg mart
df_grouped.write.format("iceberg").mode("overwrite").save(
    "my_catalog.marts.daily_sales_summary"
)
