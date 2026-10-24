from collections.abc import Iterator
from dataclasses import dataclass
from uuid import uuid4

import boto3
import pytest
from moto.server import ThreadedMotoServer

REGION = "ap-south-1"
BUCKET = "lake"


@dataclass(frozen=True)
class S3:
    storage_options: dict[str, str]

    def table_uri(self) -> str:
        return f"s3://{BUCKET}/{uuid4().hex}"


@pytest.fixture(scope="session")
def s3() -> Iterator[S3]:
    """An in-process S3 (moto) — real HTTP, real conditional-write semantics, no container.

    Limitation (2026-10-24): delta-rs *data* reads (Parquet GETs through DataFusion) stall
    against moto's server, while writes, log reads and boto3 range GETs work. Tests here assert
    on the Delta log; read-path tests need a real S3-compatible endpoint.
    """
    server = ThreadedMotoServer(ip_address="127.0.0.1", port=0)
    server.start()
    host, port = server.get_host_and_port()
    endpoint = f"http://{host}:{port}"
    boto3.client(
        "s3", endpoint_url=endpoint, region_name=REGION, aws_access_key_id="test", aws_secret_access_key="test"
    ).create_bucket(Bucket=BUCKET, CreateBucketConfiguration={"LocationConstraint": REGION})
    yield S3(
        {
            "AWS_ENDPOINT_URL": endpoint,
            "AWS_ACCESS_KEY_ID": "test",
            "AWS_SECRET_ACCESS_KEY": "test",
            "AWS_REGION": REGION,
            "AWS_ALLOW_HTTP": "true",
            "AWS_EC2_METADATA_DISABLED": "true",
        }
    )
    server.stop()
