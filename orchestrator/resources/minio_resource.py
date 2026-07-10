# orchestration/resources/minio_resource.py
from contextlib import asynccontextmanager
import aioboto3
from aiobotocore.config import AioConfig

import dagster as dg

class MinIOS3Resource(dg.ConfigurableResource):

    s3_bucket: str
    s3_prefix: str = ""
    

    endpoint_url: str
    aws_access_key_id: str
    aws_secret_access_key: str
    region_name: str

    @asynccontextmanager
    async def get_client(self, session: aioboto3.Session):

        creds_dict = {
            "endpoint_url": self.endpoint_url,
            "aws_access_key_id": self.aws_access_key_id,
            "aws_secret_access_key": self.aws_secret_access_key,
            "region_name": self.region_name
        }

        boto_config = AioConfig(signature_version="s3v4")
        
        async with session.client("s3", config=boto_config, **creds_dict) as client: # type: ignore
            yield client