"""The reader's DuckDB engine: one shared, budgeted database; a cursor per read; S3 credentials
resolved like delta-rs does; extensions never downloaded during a read."""

import threading
from collections.abc import Iterator
from pathlib import Path

import duckdb
import pyarrow as pa
import pytest
from deltalake import write_deltalake

from dataservice.storage.reader import ReaderLimits, configure, query, scan
from dataservice.storage.reader.engine import connection, s3_options, s3_secret_sql
from dataservice.storage.snapshot import PATCH_SCHEMA, FieldSpec, Snapshot

NO_PATCHES = pa.Table.from_pylist([], schema=PATCH_SCHEMA)
UTC_TS = pa.timestamp("us", tz="UTC")


class _Source:
    def patches(self, dataset_id: str, from_seq: int, to_seq: int) -> pa.Table:
        return NO_PATCHES


@pytest.fixture
def limits() -> Iterator[None]:
    yield
    configure(ReaderLimits())  # back to DuckDB's defaults for the other tests


def _setting(name: str) -> object:
    with connection() as con:
        return con.execute(f"SELECT current_setting('{name}')").fetchall()[0][0]


def test_every_read_shares_the_configured_budget(limits: None) -> None:
    configure(ReaderLimits(threads=2, memory_limit="512MB"))
    assert _setting("threads") == 2
    assert str(_setting("memory_limit")).startswith("48")  # 512 MB, shown in MiB
    assert _setting("autoinstall_known_extensions") is False  # a read never downloads anything


def test_a_session_setting_of_one_read_does_not_reach_another() -> None:
    with connection() as one:
        one.execute("SET TimeZone = 'Asia/Kolkata'")
        assert _setting("TimeZone") == "UTC"  # another read's cursor keeps its own


def test_a_full_stream_keeps_each_base_file_in_one_unbroken_run(tmp_path: Path) -> None:
    """Without patches. With patches the edits join (a parallel hash join) breaks the runs into
    ~5k-row pieces — correct rows, weaker row-id clustering after compaction (STATUS §2: compact
    file by file)."""
    uri = str(tmp_path / "base")
    ids = [f"r{n:06d}" for n in range(400_000)]
    for start in range(0, len(ids), 50_000):  # 8 files: unordered streaming would interleave them
        part = ids[start : start + 50_000]
        rows = pa.table({"_rid": part, "_deleted_at": pa.array([None] * len(part), UTC_TS), "f_age": [1.0] * len(part)})
        write_deltalake(uri, rows, mode="append")
    with scan(Snapshot("ds", uri, 7, 0, 0, (FieldSpec("age", "num"),)), _Source(), batch_rows=5000) as reader:
        streamed = [str(value) for value in reader.read_all().column("_rid").to_pylist()]
    assert sorted(streamed) == ids
    # The Delta log lists files in its own order; each file must still come through whole.
    numbers = [int(row_id[1:]) for row_id in streamed]
    seams = sum(1 for before, after in zip(numbers, numbers[1:], strict=False) if after != before + 1)
    assert seams <= 7


def test_reads_running_at_once_each_see_their_own_base(tmp_path: Path) -> None:
    snapshots = []
    for n in range(4):
        uri = str(tmp_path / f"base{n}")
        rows = pa.table({"_rid": [f"t{n}"] * 1, "_deleted_at": pa.array([None], UTC_TS), "f_age": [float(n)]})
        write_deltalake(uri, rows)
        snapshots.append(Snapshot("ds", uri, 0, 0, 0, (FieldSpec("age", "num"),)))
    seen: dict[int, list[object]] = {}

    def read(n: int) -> None:
        seen[n] = [query(snapshots[n], _Source()).to_pylist()[0]["f_age"] for _ in range(20)]

    threads = [threading.Thread(target=read, args=(n,)) for n in range(4)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert seen == {n: [float(n)] * 20 for n in range(4)}


def test_aws_options_fold_every_spelling_and_the_snapshot_wins_over_the_environment() -> None:
    environ = {"AWS_REGION": "us-east-1", "AWS_ACCESS_KEY_ID": "env-key", "HOME": "/root"}
    options = {"region": "ap-south-1", "aws_secret_access_key": "s", "endpoint": "http://127.0.0.1:9000"}
    assert s3_options(options, environ) == {
        "AWS_REGION": "ap-south-1",
        "AWS_ACCESS_KEY_ID": "env-key",
        "AWS_SECRET_ACCESS_KEY": "s",
        "AWS_ENDPOINT_URL": "http://127.0.0.1:9000",
    }


def test_static_keys_go_into_the_secret_quoted_and_scoped_to_the_bucket(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN", "AWS_REGION", "AWS_ENDPOINT_URL"):
        monkeypatch.delenv(name, raising=False)
    sql = s3_secret_sql(
        "lake", {"AWS_ACCESS_KEY_ID": "k'1", "AWS_SECRET_ACCESS_KEY": "s", "AWS_ENDPOINT_URL": "http://h:9000"}
    )
    assert "KEY_ID 'k''1'" in sql and "SCOPE 's3://lake'" in sql
    assert "ENDPOINT 'h:9000'" in sql and "URL_STYLE 'path'" in sql and "USE_SSL false" in sql
    assert "credential_chain" not in sql


def test_without_static_keys_the_secret_uses_the_aws_credential_chain(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"):
        monkeypatch.delenv(name, raising=False)
    sql = s3_secret_sql("lake", {"AWS_REGION": "ap-south-1"})
    assert "PROVIDER credential_chain" in sql and "REGION 'ap-south-1'" in sql and "KEY_ID" not in sql


def test_duckdb_accepts_both_kinds_of_secret(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"):
        monkeypatch.delenv(name, raising=False)
    con = duckdb.connect()
    try:
        con.load_extension("httpfs")
        con.load_extension("aws")
        con.execute(s3_secret_sql("a", {"AWS_ACCESS_KEY_ID": "k", "AWS_SECRET_ACCESS_KEY": "s", "AWS_REGION": "x"}))
        con.execute(s3_secret_sql("b", {"AWS_REGION": "ap-south-1"}))
        scopes = sorted(
            str(row[0])
            for row in con.execute("SELECT unnest(scope) FROM duckdb_secrets() WHERE name LIKE 's3_%'").fetchall()
        )
        assert scopes == ["s3://a", "s3://b"]
    finally:
        con.close()
