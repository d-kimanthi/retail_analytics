import os

from pyspark.sql import SparkSession

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "please-provide-access-key-id")
AWS_SECRET_ACCESS_KEY = os.getenv(
    "AWS_SECRET_ACCESS_KEY", "please-provide-secret-access-key"
)
HMS_URI = os.getenv("HMS_URI", "thrift://localhost:9083")
WAREHOUSE = os.getenv(
    "ICEBERG_WAREHOUSE_DIR",
    # Use a local filesystem warehouse by default for local/dev runs to avoid
    # needing S3 credentials/config in the Hive metastore container.
    "file:///Users/kimanthi/projects/retail-analytics/warehouse",
)

# Initialize basic Spark session with AWS configurations
print("Initializing Spark Session with AWS configurations...")
print(f"AWS_ACCESS_KEY_ID={AWS_ACCESS_KEY_ID}")
print(f"AWS_SECRET_ACCESS_KEY={AWS_SECRET_ACCESS_KEY}")


spark = (
    SparkSession.builder.appName("Iceberg-Hive-on-S3")
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    )
    .config("spark.sql.catalog.hive_catalog", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.hive_catalog.type", "hive")
    .config("spark.sql.catalog.hive_catalog.uri", HMS_URI)
    .config("spark.sql.catalog.hive_catalog.warehouse", WAREHOUSE)
    .config("spark.sql.defaultCatalog", "hive_catalog")
    .getOrCreate()
)

print("Spark:", spark.version)
print("Hive Metastore URI:", HMS_URI)
print("Iceberg warehouse:", WAREHOUSE)

# Create namespace + table in the hive_catalog (stored in S3)
spark.sql("CREATE NAMESPACE IF NOT EXISTS retail")

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

print("\nVerify table location (should be s3://...):")
spark.sql("DESCRIBE EXTENDED retail.test_table").filter("col_name = 'Location'").show(
    truncate=False
)

spark.stop()
