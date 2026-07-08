# orchestration/resources/minio_resource.py
import aioboto3
from aiobotocore.config import AioConfig
from dagster import ConfigurableResource

class MinIOS3Resource(ConfigurableResource):
    endpoint_url: str
    aws_access_key_id: str
    aws_secret_access_key: str

    def get_client(self, session: aioboto3.Session):
        return session.client(
            "s3",
            endpoint_url=self.endpoint_url,
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
            config=AioConfig(signature_version="s3v4"),
            region_name="us-east-1"
        )