import asyncio
from config import settings
from logging_config import logging, setup_logging

import aioboto3
from aiobotocore.config import AioConfig
from botocore.exceptions import ClientError

setup_logging(level=settings.LOG_LEVEL)
logger = logging.getLogger(__name__)

MINIO_CONFIG = {
    "endpoint_url": settings.MINIO_ENDPOINT_URL,
    "aws_access_key_id": settings.MINIO_ROOT_USER,
    "aws_secret_access_key": settings.MINIO_ROOT_PASSWORD.get_secret_value(),
    "config": AioConfig(signature_version="s3v4"),
    "region_name": "us-east-1"
}

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

async def main() -> None:
    """Create buckets"""
    session = aioboto3.Session()

    async with session.client("s3", **MINIO_CONFIG) as async_s3_client:                 # type: ignore
        try:
            logger.info("Starting Medalion Bucket infras deploying...")
            for bucket, tags in BUCKETS_TO_CREATE.items():
                try:
                    await async_s3_client.head_bucket(Bucket=bucket)
                    logger.info("Bucket %s already exist. Skipping creation", bucket)

                except ClientError as err:
                    if err.response["Error"]["Code"] == "404":
                        logger.info("Creating bucket: %s", bucket)
                        await async_s3_client.create_bucket(Bucket=bucket)

                    else:
                        raise err
        
                logger.info("Enabling Versioning for bucket: %s", bucket)

                await async_s3_client.put_bucket_versioning(
                    Bucket=bucket,
                    VersioningConfiguration={"Status": "Enabled"}
                )

                logger.info("Committing Tagging for bucket: %s", bucket)

                await async_s3_client.put_bucket_tagging(
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


if __name__ == "__main__":
    asyncio.run(main())