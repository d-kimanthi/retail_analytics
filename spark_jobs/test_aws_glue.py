import os

from pyspark.sql import SparkSession

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "please-provide-access-key-id")
AWS_SECRET_ACCESS_KEY = os.getenv(
    "AWS_SECRET_ACCESS_KEY", "please-provide-secret-access-key"
)
AWS_DEFAULT_REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-2")
WAREHOUSE = os.getenv(
    "ICEBERG_WAREHOUSE_DIR",
    "s3a://your-bucket-name/warehouse",
)

# Initialize Spark session with AWS Glue Catalog
print("Initializing Spark Session with AWS Glue Catalog...")
print(f"AWS_ACCESS_KEY_ID={AWS_ACCESS_KEY_ID}")
print(f"AWS_DEFAULT_REGION={AWS_DEFAULT_REGION}")

spark = (
    SparkSession.builder.appName("Iceberg-Glue-on-S3")
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    )
    .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
    .config(
        "spark.sql.catalog.glue_catalog.catalog-impl",
        "org.apache.iceberg.aws.glue.GlueCatalog",
    )
    .config("spark.sql.catalog.glue_catalog.warehouse", WAREHOUSE)
    .config(
        "spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO"
    )
    .config("spark.sql.defaultCatalog", "glue_catalog")
    # AWS credentials for Glue Catalog access
    .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY_ID)
    .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.endpoint.region", AWS_DEFAULT_REGION)
    .getOrCreate()
)

print("Spark:", spark.version)
print("AWS Region:", AWS_DEFAULT_REGION)
print("Iceberg warehouse:", WAREHOUSE)

# Create database + table in the glue_catalog (stored in S3)
spark.sql("CREATE DATABASE IF NOT EXISTS retail")

spark.sql(
    """
    CREATE TABLE IF NOT EXISTS retail.test_table (
        id INT,
        name STRING,
        ts TIMESTAMP
    )
    USING iceberg
"""
)

spark.sql(
    """
    INSERT INTO retail.test_table
    VALUES (1, 'test1', current_timestamp()),
           (2, 'test2', current_timestamp())
"""
)

print("\nRead back from S3-backed Iceberg table:")
spark.sql("SELECT * FROM retail.test_table ORDER BY id").show(truncate=False)

print("\nVerify table location (should be s3a://...):")
spark.sql("DESCRIBE EXTENDED retail.test_table").filter("col_name = 'Location'").show(
    truncate=False
)

print("\nList tables in Glue catalog:")
spark.sql("SHOW TABLES IN retail").show()

spark.stop()
