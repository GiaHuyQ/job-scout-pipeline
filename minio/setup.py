from config import settings
from logging_config import logging

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

s3_client =  boto3.client(
    "s3",
    endpoint_url=settings.MINIO_ENDPOINT_URL,
    aws_access_key_id=settings.MINIO_ROOT_USER,
    aws_secret_access_key=settings.MINIO_ROOT_PASSWORD.get_secret_value(),
    config=Config(signature_version="s3v4"),
    region_name="us-east-1"
)

BUCKETS_TO_CREATE = {
    "jobs-bronze": [
        {"Key": "PipeLine", "Value": "JobScout"},
        {"Key": "Stage", "Value": "Bronze"}
    ],
    "jobs-silver": [
        {"Key": "PipeLine", "Value": "JobScout"},
        {"Key": "Stage", "Value": "Silver"}
    ],
    "jobs-gold": [
        {"Key": "PipeLine", "Value": "JobScout"},
        {"Key": "Stage", "Value": "Gold"}
    ]
}

try:
    logger.info("Starting Medalion Bucket infras deploying...")
    for bucket, tags in BUCKETS_TO_CREATE.items():
        try:
            s3_client.head_bucket(Bucket=bucket)
            logger.info("Bucket %s already exist. Skipping creation", bucket)
        except ClientError as err:
            if err.response["Error"]["Code"] == "404":
                logger.info("Creating bucket: %s", bucket)
                s3_client.create_bucket(Bucket=bucket)
            else:
                raise err
    
        logger.info("Enabling Versioning for bucket: %s", bucket)
        s3_client.put_bucket_versioning(
            Bucket=bucket,
            VersioningConfiguration={"Status": "Enabled"}
        )

        logger.info("Committing Tagging for bucket: %s", bucket)
        s3_client.put_bucket_tagging(
            Bucket=bucket,
            Tagging={
                "TagSet": [
                    *tags
                ]
            }
        )

    logger.info("Medallion Bucket infras deployed successfully!")

except Exception as err:
    logger.exception("Global bucket deployment sequence collapsed: %s", err)