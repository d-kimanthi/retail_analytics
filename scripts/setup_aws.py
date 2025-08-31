import logging

import boto3

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_s3_bucket(bucket_name, region="us-east-1"):
    """Create an S3 bucket."""
    s3_client = boto3.client("s3", region_name=region)

    try:
        if region == "us-east-1":
            s3_client.create_bucket(Bucket=bucket_name)
        else:
            s3_client.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration={"LocationConstraint": region},
            )
        logger.info(f"Created bucket: {bucket_name}")
        return True
    except s3_client.exceptions.BucketAlreadyExists:
        logger.info(f"Bucket {bucket_name} already exists")
        return True
    except Exception as e:
        logger.error(f"Error creating bucket {bucket_name}: {str(e)}")
        return False


def create_glue_database(database_name, region="us-east-1"):
    """Create a Glue Database."""
    glue_client = boto3.client("glue", region_name=region)

    try:
        glue_client.create_database(
            DatabaseInput={
                "Name": database_name,
                "Description": f"Database for {database_name}",
            }
        )
        logger.info(f"Created Glue database: {database_name}")
        return True
    except glue_client.exceptions.AlreadyExistsException:
        logger.info(f"Database {database_name} already exists")
        return True
    except Exception as e:
        logger.error(f"Error creating database {database_name}: {str(e)}")
        return False


def main():
    # Configuration
    BUCKET_NAME = "dk-retail-analytics-lake"
    REGION = "us-east-2"

    # Create S3 bucket
    if not create_s3_bucket(BUCKET_NAME, REGION):
        logger.error("Failed to create S3 bucket")
        return

    # Create Glue databases
    databases = ["raw", "marts"]
    for db in databases:
        if not create_glue_database(db, REGION):
            logger.error(f"Failed to create Glue database: {db}")
            return

    logger.info("AWS infrastructure setup completed successfully!")


if __name__ == "__main__":
    main()
