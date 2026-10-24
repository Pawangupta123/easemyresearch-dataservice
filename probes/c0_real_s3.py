"""C0 on REAL AWS S3: concurrent appends to one Delta table never overwrite each other.

Run on demand (not in CI — it needs AWS credentials and writes to a bucket):

    AWS_ACCESS_KEY_ID=… AWS_SECRET_ACCESS_KEY=… AWS_REGION=ap-south-1 C0_BUCKET=<test bucket> \\
        uv run python probes/c0_real_s3.py [writers]

Writes under a unique `dataservice-probe/c0-<id>/` prefix and deletes it afterwards. Never
prints credentials. Expected (verified 2026-10-24, 20 writers): every successful commit kept,
the others failed loudly with CommitFailedError, nothing overwritten — "PASS".
"""

import os
import sys
import threading
import uuid

import boto3
import pyarrow as pa
from deltalake import DeltaTable, write_deltalake


def main(writers: int) -> int:
    bucket, region = os.environ["C0_BUCKET"], os.environ["AWS_REGION"]
    options = {
        "AWS_ACCESS_KEY_ID": os.environ["AWS_ACCESS_KEY_ID"],
        "AWS_SECRET_ACCESS_KEY": os.environ["AWS_SECRET_ACCESS_KEY"],
        "AWS_REGION": region,
        "AWS_EC2_METADATA_DISABLED": "true",
    }
    prefix = f"dataservice-probe/c0-{uuid.uuid4().hex[:8]}"
    uri = f"s3://{bucket}/{prefix}"
    succeeded: list[int] = []
    failed: list[str] = []
    try:
        write_deltalake(uri, pa.table({"writer": [-1]}), storage_options=options)

        def append(writer: int) -> None:
            try:
                write_deltalake(uri, pa.table({"writer": [writer]}), mode="append", storage_options=options)
                succeeded.append(writer)
            except Exception as error:
                failed.append(type(error).__name__)

        threads = [threading.Thread(target=append, args=(writer,)) for writer in range(writers)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        actions = pa.table(DeltaTable(uri, storage_options=options).get_add_actions(flatten=True))
        in_log = sorted(int(value) for value in actions.column("min.writer").to_pylist())
        ok = in_log == sorted([-1, *succeeded])
        print(f"writers={writers} succeeded={len(succeeded)} failed={len(failed)} {sorted(set(failed))}")
        print("PASS: every successful commit kept, failures loud, nothing overwritten" if ok else "FAIL: silent loss")
        return 0 if ok else 1
    finally:
        s3 = boto3.client("s3", region_name=region)
        pages = s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=f"{prefix}/")
        keys = [item["Key"] for page in pages for item in page.get("Contents", [])]
        for start in range(0, len(keys), 1000):
            s3.delete_objects(Bucket=bucket, Delete={"Objects": [{"Key": key} for key in keys[start : start + 1000]]})
        print(f"cleaned up {len(keys)} probe objects")


if __name__ == "__main__":
    sys.exit(main(int(sys.argv[1]) if len(sys.argv) > 1 else 20))
