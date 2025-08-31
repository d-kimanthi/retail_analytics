<!-- start metastore -->

docker compose -f docker-compose.hive.yml --env-file ./config/awscreds.env up -d
docker compose -f docker-compose.hive.yml down -v

hadoop fs -ls s3a://dk-retail-analytics-lake/

ADD https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar /opt/hive/lib/
ADD https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.767/aws-java-sdk-bundle-1.12.767.jar /opt/hive/lib/
