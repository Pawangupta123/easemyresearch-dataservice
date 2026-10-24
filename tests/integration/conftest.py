import os
from collections.abc import Iterator
from dataclasses import dataclass
from urllib.parse import urlparse
from uuid import uuid4

import boto3
import pytest
from moto.server import ThreadedMotoServer
from pymongo import MongoClient
from pymongo.database import Database
from pymongo.errors import PyMongoError

REGION = "ap-south-1"
BUCKET = "lake"
# Local replica set from `docker compose up -d`. Never a shared or production cluster: the
# guard below refuses any host but localhost, and every test gets its own throwaway database.
TEST_MONGODB_URL = os.environ.get("TEST_MONGODB_URL", "mongodb://localhost:27018/?replicaSet=rs0&directConnection=true")

Document = dict[str, object]


@pytest.fixture(scope="session")
def mongo_client() -> Iterator[MongoClient[Document]]:
    host = urlparse(TEST_MONGODB_URL).hostname
    if host not in {"localhost", "127.0.0.1"}:
        raise RuntimeError(f"integration tests only run against a local Mongo, not {host}")
    client: MongoClient[Document] = MongoClient(TEST_MONGODB_URL, serverSelectionTimeoutMS=2000)
    try:
        client.admin.command("ping")
    except PyMongoError:
        if os.environ.get("REQUIRE_MONGO") == "1":  # CI: a missing service is a failure, not a skip
            raise
        pytest.skip("local Mongo replica set not running — `docker compose up -d`")
    yield client
    client.close()


@pytest.fixture
def mongo_db(mongo_client: MongoClient[Document]) -> Iterator[Database[Document]]:
    name = f"test_dataservice_{uuid4().hex[:12]}"
    yield mongo_client[name]
    mongo_client.drop_database(name)


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
