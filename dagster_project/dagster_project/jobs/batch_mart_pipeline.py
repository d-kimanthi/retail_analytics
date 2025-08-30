from dagster import job, op
import subprocess


@op
def build_sales_mart():
    subprocess.run(
        [
            "spark-submit",
            "--packages",
            "org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.8.1,"
            "org.apache.hadoop:hadoop-aws:3.3.2,"
            "com.amazonaws:aws-java-sdk-bundle:1.11.901",
            "spark_jobs/batch/build_sales_mart.py",
        ],
        check=True,
    )


@job
def build_sales_mart_job():
    build_sales_mart()
