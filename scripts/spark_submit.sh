#!/bin/bash

# Source the Spark environment
source "$(dirname "$0")/setup_spark_env.sh"

# Submit Spark job with necessary packages
spark-submit \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.8.1,\
  org.apache.iceberg:iceberg-aws-bundle:1.8.1,\
  org.apache.hadoop:hadoop-aws:3.3.4,\
  com.amazonaws:aws-java-sdk-bundle:1.12.470,\
  org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2 \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog \
  --conf spark.sql.catalog.glue_catalog.warehouse=s3a://dk-retail-analytics-lake/warehouse \
  --conf spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO \
  --conf spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.DefaultAWSCredentialsProviderChain \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  "$@"
--packages org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.9.2,org.apache.iceberg:iceberg-aws-bundle:1.9.2




spark-submit \
  --packages \
org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.8.1,\
org.apache.hadoop:hadoop-aws:3.3.2 \
  --conf "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions" \
  --conf "spark.sql.catalog.hive_catalog=org.apache.iceberg.spark.SparkCatalog" \
  --conf "spark.sql.catalog.hive_catalog.type=hive" \
  --conf "spark.sql.catalog.hive_catalog.uri=thrift://localhost:9083" \
  --conf "spark.sql.catalog.hive_catalog.warehouse=/Users/kimanthi/projects/retail-analytics/warehouse" \
  --conf "spark.sql.defaultCatalog=hive_catalog" \
  --conf "spark.hadoop.fs.s3a.access.key=$AWS_ACCESS_KEY_ID" \
  --conf "spark.hadoop.fs.s3a.secret.key=$AWS_SECRET_ACCESS_KEY" \
  --conf "spark.hadoop.fs.s3a.endpoint=s3.amazonaws.com" \
  --conf "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem" \
  "$@"