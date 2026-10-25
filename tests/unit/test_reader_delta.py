"""The reader reads a pinned Delta version exactly as Delta defines it — whatever its files hold."""

from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
import pytest
from deltalake import DeltaTable, Field, write_deltalake
from deltalake.schema import PrimitiveType

from dataservice.storage.reader import count, query, scan
from dataservice.storage.snapshot import PATCH_SCHEMA, FieldSpec, Snapshot

UTC_TS = pa.timestamp("us", tz="UTC")
NO_PATCHES = pa.Table.from_pylist([], schema=PATCH_SCHEMA)


class _Source:
    def __init__(self, table: pa.Table = NO_PATCHES) -> None:
        self.table = table

    def patches(self, dataset_id: str, from_seq: int, to_seq: int) -> pa.Table:
        return self.table


def _base(*ids: str, **columns: list[object]) -> pa.Table:
    return pa.table({"_rid": list(ids), "_deleted_at": pa.array([None] * len(ids), UTC_TS)} | columns)


def _snapshot(uri: str, version: int, *fields: FieldSpec, patch_to: int = 0) -> Snapshot:
    return Snapshot("ds", uri, version, 0, patch_to, fields or (FieldSpec("age", "num"),))


AGE, NAME = FieldSpec("age", "num"), FieldSpec("name", "str")


def test_files_written_before_a_column_was_added_read_it_as_empty(tmp_path: Path) -> None:
    uri = str(tmp_path / "base")
    write_deltalake(uri, _base("r1", "r2", f_age=[1.0, 2.0]))
    write_deltalake(uri, _base("r3", f_age=[3.0], f_name=["c"]), mode="append", schema_mode="merge")
    snapshot = _snapshot(uri, 1, AGE, NAME)
    assert query(snapshot, _Source()).to_pylist() == [
        {"_rid": "r1", "f_age": 1.0, "f_name": None},
        {"_rid": "r2", "f_age": 2.0, "f_name": None},
        {"_rid": "r3", "f_age": 3.0, "f_name": "c"},
    ]
    with scan(snapshot, _Source()) as reader:
        assert reader.read_all().num_rows == 3


def test_a_column_added_in_metadata_only_reads_as_empty_and_takes_patches(tmp_path: Path) -> None:
    uri = str(tmp_path / "base")
    write_deltalake(uri, _base("r1", "r2", f_age=[1.0, 2.0]))
    DeltaTable(uri).alter.add_columns([Field("f_name", PrimitiveType("string"), nullable=True)])
    patch: dict[str, object] = {
        "seq": 1,
        "op": "set",
        "row": "r2",
        "follow_up": 0,
        "field": "name",
        "t": "str",
        "v_str": "Ravi",
    }
    patch |= {"v_num": None, "v_date": None, "v_opts": None, "at": datetime(2026, 10, 24, tzinfo=UTC)}
    rows = query(_snapshot(uri, 1, AGE, NAME, patch_to=1), _Source(pa.Table.from_pylist([patch], schema=PATCH_SCHEMA)))
    assert [row["f_name"] for row in rows.to_pylist()] == [None, "Ravi"]


def test_a_snapshot_field_the_base_lacks_reads_as_empty(tmp_path: Path) -> None:
    uri = str(tmp_path / "base")
    write_deltalake(uri, _base("r1", f_age=[1.0]))
    assert query(_snapshot(uri, 0, AGE, NAME), _Source()).to_pylist() == [{"_rid": "r1", "f_age": 1.0, "f_name": None}]


def test_a_base_under_a_path_with_spaces_and_non_ascii_is_read(tmp_path: Path) -> None:
    uri = str(tmp_path / "with space é" / "base")
    write_deltalake(uri, _base("r1", f_age=[1.0]))
    assert "%20" in DeltaTable(uri).file_uris()[0]  # delta-rs hands out encoded paths
    assert query(_snapshot(uri, 0), _Source()).num_rows == 1


def test_a_pinned_version_reads_every_file_it_is_made_of(tmp_path: Path) -> None:
    uri = str(tmp_path / "base")
    write_deltalake(uri, _base("r1", "r2", f_age=[1.0, 2.0]))
    write_deltalake(uri, _base("r3", f_age=[3.0]), mode="append")
    write_deltalake(uri, _base("r9", f_age=[9.0]), mode="overwrite")
    assert [row["_rid"] for row in query(_snapshot(uri, 1), _Source()).to_pylist()] == ["r1", "r2", "r3"]
    assert count(_snapshot(uri, 0), _Source()) == 2 and count(_snapshot(uri, 2), _Source()) == 1


UNREADABLE: dict[str, Callable[[str, pa.Table], None]] = {
    # reader v2 column mapping lists no reader features — the guard reads the table property
    "column-mapping": lambda uri, rows: write_deltalake(uri, rows, configuration={"delta.columnMapping.mode": "name"}),
    "partitioned": lambda uri, rows: write_deltalake(uri, rows, partition_by=["f_name"]),
}


@pytest.mark.parametrize("kind", sorted(UNREADABLE))
def test_a_table_the_files_alone_do_not_describe_is_refused(tmp_path: Path, kind: str) -> None:
    uri = str(tmp_path / "base")
    UNREADABLE[kind](uri, _base("r1", f_age=[1.0], f_name=["a"]))
    with pytest.raises(ValueError, match="can't be read file by file"):
        query(_snapshot(uri, 0, AGE, NAME), _Source())


def test_c21_each_revision_reads_with_its_own_schema_across_a_type_change(tmp_path: Path) -> None:
    """A type change rewrites the column (a Delta job); a revision pinned before it still reads
    its base version with its own `schemaRef`, the new one with the new type."""
    uri = str(tmp_path / "base")
    write_deltalake(uri, _base("r1", "r2", f_age=[30.0, 41.0]))  # v0: age is a number
    write_deltalake(uri, _base("r1", "r2", f_age=["30", "41 yrs"]), mode="overwrite", schema_mode="overwrite")
    before = query(_snapshot(uri, 0, AGE), _Source()).column("f_age").to_pylist()
    after = query(_snapshot(uri, 1, FieldSpec("age", "str")), _Source()).column("f_age").to_pylist()
    assert before == [30.0, 41.0] and after == ["30", "41 yrs"]


def test_reading_a_base_with_another_revisions_schema_is_refused(tmp_path: Path) -> None:
    uri = str(tmp_path / "base")
    write_deltalake(uri, _base("r1", f_age=["30"]))
    with pytest.raises(ValueError, match="age is num but its column is VARCHAR"):
        query(_snapshot(uri, 0, AGE), _Source())
