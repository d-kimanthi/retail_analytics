#!/bin/bash

# Java Home (requires Java 8 or 11)
# Uncomment and set this if JAVA_HOME is not already set
# export JAVA_HOME=$(/usr/libexec/java_home -v 11)

# Spark Home
echo "Start of script SPARK_HOME is set to: $SPARK_HOME"

# Python environment
export PYSPARK_PYTHON="/Users/kimanthi/projects/retail-analytics/.venv/bin/python3"
export PYSPARK_DRIVER_PYTHON="/Users/kimanthi/projects/retail-analytics/.venv/bin/python3"

# AWS Configurations (if using AWS)
export AWS_DEFAULT_REGION=us-east-2  # Change this to your AWS region

# Download necessary dependencies
mkdir -p "$SPARK_HOME/jars"

# Download AWS Hadoop SDK
if [ ! -f "$SPARK_HOME/jars/hadoop-aws-3.3.2.jar" ]; then
    curl -L "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.2/hadoop-aws-3.3.2.jar" -o "$SPARK_HOME/jars/hadoop-aws-3.3.2.jar"
fi

# Download AWS SDK Bundle
if [ ! -f "$SPARK_HOME/jars/aws-java-sdk-bundle-1.12.262.jar" ]; then
    curl -L "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar" -o "$SPARK_HOME/jars/aws-java-sdk-bundle-1.12.262.jar"
fi

# Download Iceberg Spark Runtime
if [ ! -f "$SPARK_HOME/jars/iceberg-spark-runtime-3.3_2.12-0.14.1.jar" ]; then
    curl -L "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.3_2.12/0.14.1/iceberg-spark-runtime-3.3_2.12-0.14.1.jar" -o "$SPARK_HOME/jars/iceberg-spark-runtime-3.3_2.12-0.14.1.jar"
fi

# Download Spark Kafka Integration
if [ ! -f "$SPARK_HOME/jars/spark-sql-kafka-0-10_2.12-3.3.2.jar" ]; then
    curl -L "https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.3.2/spark-sql-kafka-0-10_2.12-3.3.2.jar" -o "$SPARK_HOME/jars/spark-sql-kafka-0-10_2.12-3.3.2.jar"
fi

# Download Kafka Clients
if [ ! -f "$SPARK_HOME/jars/kafka-clients-3.3.1.jar" ]; then
    curl -L "https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.3.1/kafka-clients-3.3.1.jar" -o "$SPARK_HOME/jars/kafka-clients-3.3.1.jar"
fi

echo "Spark environment setup complete!"
echo "SPARK_HOME is set to: $SPARK_HOME"
echo "Run 'source scripts/setup_spark_env.sh' to set up the Spark environment in new terminals"
