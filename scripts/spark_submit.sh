#!/bin/bash
# This script starts a Spark shell with the necessary configurations to connect to AWS S3 and use Iceberg.
spark-shell \
  --jars /Users/kimanthi/sources/jars/hadoop-aws-3.3.2.jar,/Users/kimanthi/sources/jars/aws-java-sdk-bundle-1.11.901.jar,/Users/kimanthi/sources/jars/iceberg-spark-runtime-3.3_2.12-1.8.1.jar \
  --conf spark.sql.catalog.my_catalog=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.my_catalog.type=hadoop \
  --conf spark.sql.catalog.my_catalog.warehouse=s3a://dk-retail-analytics-lake/warehouse \
  --conf spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.DefaultAWSCredentialsProviderChain \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem


# This script submits a Spark job to process streaming transactions from Kafka and store them in Iceberg tables on S3.
spark-submit \
  --packages \
org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.8.1,\
org.apache.hadoop:hadoop-aws:3.3.2,\
com.amazonaws:aws-java-sdk-bundle:1.11.901,\
org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2 \
  spark_jobs/streaming/process_transactions.py


# This script submits a Spark job to build a sales mart from the processed transactions stored in Iceberg tables on S3.
spark-submit \
  --packages \
org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.8.1,\
org.apache.hadoop:hadoop-aws:3.3.2,\
com.amazonaws:aws-java-sdk-bundle:1.11.901 \
  spark_jobs/batch/build_sales_mart.py
