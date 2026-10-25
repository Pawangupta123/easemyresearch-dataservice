"""The reader on S3 (moto): DuckDB reads the pinned version's Parquet files with the same AWS
settings delta-rs uses — from the snapshot's storage options, or from the environment alone."""

import pyarrow as pa
import pytest
from deltalake import write_deltalake

from dataservice.storage.reader import Query, Sort, count, query
from dataservice.storage.snapshot import PATCH_SCHEMA, FieldSpec, Snapshot
from tests.integration.conftest import S3

UTC_TS = pa.timestamp("us", tz="UTC")
FIELDS = (FieldSpec("age", "num"),)
ROWS = pa.table(
    {
        "_rid": [f"r{n:03d}" for n in range(100)],
        "_deleted_at": pa.array([None] * 100, UTC_TS),
        "f_age": [float(n) for n in range(100)],
    }
)


class _NoPatches:
    def patches(self, dataset_id: str, from_seq: int, to_seq: int) -> pa.Table:
        return pa.Table.from_pylist([], schema=PATCH_SCHEMA)


def test_a_base_on_s3_is_read_with_the_snapshot_options(s3: S3) -> None:
    uri = s3.table_uri()
    write_deltalake(uri, ROWS, storage_options=s3.storage_options)
    snapshot = Snapshot("ds", uri, 0, 0, 0, FIELDS, s3.storage_options)
    page = query(snapshot, _NoPatches(), Query(sort=(Sort("age", descending=True),), limit=3))
    assert page.column("f_age").to_pylist() == [99.0, 98.0, 97.0]
    assert count(snapshot, _NoPatches()) == 100


def test_a_base_on_s3_is_read_with_settings_from_the_environment_only(s3: S3, monkeypatch: pytest.MonkeyPatch) -> None:
    uri = s3.table_uri()
    write_deltalake(uri, ROWS, storage_options=s3.storage_options)
    for name, value in s3.storage_options.items():
        monkeypatch.setenv(name, value)
    snapshot = Snapshot("ds", uri, 0, 0, 0, FIELDS, {})  # how a container with a task role runs
    assert count(snapshot, _NoPatches()) == 100
