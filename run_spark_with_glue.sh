#!/bin/bash

# Source AWS credentials from file
CREDS_FILE="./config/awscreds.env"

# Export variables from the env file
if [ -f "$CREDS_FILE" ]; then
  # Ignore comments and empty lines
  export $(grep -v '^#' "$CREDS_FILE" | xargs)
fi

# Verify AWS credentials are set
echo "AWS credentials loaded:"
echo "AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID:0:4}****"
echo "AWS_SECRET_ACCESS_KEY=****"
echo "AWS_DEFAULT_REGION=$AWS_DEFAULT_REGION"

# S3 warehouse directory - MUST use s3a:// scheme
ICEBERG_WAREHOUSE_DIR=${ICEBERG_WAREHOUSE_DIR:-s3a://your-bucket-name/warehouse}

echo "AWS Region: $AWS_DEFAULT_REGION"
echo "Iceberg Warehouse: $ICEBERG_WAREHOUSE_DIR"

# Spark Submit with Iceberg and AWS Glue Catalog
spark-submit \
  --packages \
org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.5.2,\
org.apache.iceberg:iceberg-aws-bundle:1.5.2,\
org.apache.hadoop:hadoop-aws:3.3.4,\
com.amazonaws:aws-java-sdk-bundle:1.12.470 \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog \
  --conf spark.sql.catalog.glue_catalog.warehouse=$ICEBERG_WAREHOUSE_DIR \
  --conf spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO \
  --conf spark.sql.defaultCatalog=glue_catalog \
  --conf spark.hadoop.fs.s3a.access.key=$AWS_ACCESS_KEY_ID \
  --conf spark.hadoop.fs.s3a.secret.key=$AWS_SECRET_ACCESS_KEY \
  --conf spark.hadoop.fs.s3a.endpoint.region=$AWS_DEFAULT_REGION \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  --conf spark.hadoop.fs.s3.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  --conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider \
  --conf spark.hadoop.fs.s3a.fast.upload=true \
  --conf spark.hadoop.fs.s3a.multipart.size=67108864 \
  --conf spark.hadoop.fs.s3a.path.style.access=false \
  --conf spark.hadoop.fs.s3a.bucket.probe=0 \
  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
  --conf spark.eventLog.enabled=false \
  "$@"