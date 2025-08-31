FROM apache/hive:3.1.3

# Version arguments
ARG HADOOP_AWS_VER=3.3.2
ARG AWS_SDK_VER=1.12.262
ARG ICEBERG_VER=1.4.0

# Download Hadoop AWS JAR
ADD https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VER}/hadoop-aws-${HADOOP_AWS_VER}.jar /opt/hive/lib/

# Download AWS Java SDK Bundle
ADD https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VER}/aws-java-sdk-bundle-${AWS_SDK_VER}.jar /opt/hive/lib/

# Download Iceberg AWS Bundle
ADD https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VER}/iceberg-aws-bundle-${ICEBERG_VER}.jar /opt/hive/lib/

# Download additional required Hadoop dependencies
ADD https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-common/${HADOOP_AWS_VER}/hadoop-common-${HADOOP_AWS_VER}.jar /opt/hive/lib/
ADD https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-client-api/${HADOOP_AWS_VER}/hadoop-client-api-${HADOOP_AWS_VER}.jar /opt/hive/lib/
ADD https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-client-runtime/${HADOOP_AWS_VER}/hadoop-client-runtime-${HADOOP_AWS_VER}.jar /opt/hive/lib/

# Download WildFly OpenSSL (required for S3A)
ADD https://repo1.maven.org/maven2/org/wildfly/openssl/wildfly-openssl/1.0.7.Final/wildfly-openssl-1.0.7.Final.jar /opt/hive/lib/

# Environment variables for S3A configuration
ENV AWS_REGION=us-east-2
ENV JAVA_OPTS="-Dcom.amazonaws.services.s3.enableV4=true"

# Expose metastore port
EXPOSE 9083

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=30s --retries=3 \
    CMD netstat -ln | grep 9083 || exit 1