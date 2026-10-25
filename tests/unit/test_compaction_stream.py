"""Streaming compaction on a local Delta base: every column type and patch op fold in, the
revision reads back identically (C39), and anything that would lose a value is refused."""

from datetime import UTC, date, datetime
from pathlib import Path

import pyarrow as pa
import pytest
from deltalake import DeltaTable, write_deltalake

from dataservice.storage.compaction import compact
from dataservice.storage.fenced_commit import CommitContext, OpRef, max_token_seen
from dataservice.storage.reader import Query, count, query
from dataservice.storage.snapshot import PATCH_SCHEMA, FieldSpec, Snapshot

AT = datetime(2026, 10, 24, 9, 30, tzinfo=UTC)
FIELDS = (FieldSpec("age", "num"), FieldSpec("name", "str"), FieldSpec("visit", "date"), FieldSpec("sym", "opts"))
BASE = pa.table(
    {
        "_rid": ["r1", "r2", "r3", "r4"],
        "_deleted_at": pa.array([None, None, datetime(2026, 10, 1, tzinfo=UTC), None], pa.timestamp("us", tz="UTC")),
        "f_age": [30.0, 41.0, 55.0, None],
        "f_name": ["Asha", "Ravi", "Meena", None],
        "f_visit": pa.array([date(2026, 1, 5), None, date(2026, 2, 1), None], pa.date32()),
        "f_sym": pa.array([["o_fever"], [], None, None], pa.list_(pa.string())),
    }
)


def _patch(
    seq: int, op: str, row: str, field: str | None = None, t: str | None = None, **values: object
) -> dict[str, object]:
    return {"seq": seq, "op": op, "row": row, "field": field, "t": t, "at": AT} | {
        column: values.get(column) for column in ("v_str", "v_num", "v_date", "v_opts")
    }


EVERY_OP = pa.Table.from_pylist(
    [
        _patch(1, "set", "r1", "age", "num", v_num=31.0),
        _patch(2, "set", "r2", "name", "null"),  # cleared
        _patch(3, "set", "r1", "visit", "date", v_date=date(2026, 3, 9)),
        _patch(4, "set", "r2", "sym", "opts", v_opts=["o_cough"]),
        _patch(5, "insert_row", "r9"),
        _patch(6, "set", "r9", "name", "str", v_str="New"),
        _patch(7, "delete_row", "r4"),
        _patch(8, "restore_row", "r3"),
        _patch(9, "insert_row", "r8"),
        _patch(10, "delete_row", "r8"),  # inserted, then trashed
    ],
    schema=PATCH_SCHEMA,
)


class _Source:
    def __init__(self, table: pa.Table) -> None:
        self.table = table

    def patches(self, dataset_id: str, from_seq: int, to_seq: int) -> pa.Table:
        return self.table


def _context(uri: str) -> CommitContext:
    return CommitContext(max_token_seen([DeltaTable(uri)]) + 1, (OpRef("compact-1", 1),), ("base",))


def test_every_type_and_op_folds_in_and_the_revision_reads_the_same(tmp_path: Path) -> None:
    uri = str(tmp_path / "base")
    write_deltalake(uri, BASE)
    before = Snapshot("ds", uri, 0, 0, 10, FIELDS)
    expected = query(before, _Source(EVERY_OP), Query(include_deleted=True)).to_pylist()
    result = compact(before, _Source(EVERY_OP), _context(uri))
    after = Snapshot("ds", uri, result.base_version, 10, 10, FIELDS)  # nothing left to lay over
    assert (result.folded_to, result.rows) == (10, len(expected)) == (10, 6)
    assert query(after, _Source(EVERY_OP), Query(include_deleted=True)).to_pylist() == expected
    assert count(after, _Source(EVERY_OP)) == count(before, _Source(EVERY_OP)) == 4  # r1 r2 r3 r9


def test_a_field_with_no_base_column_is_refused_and_nothing_is_written(tmp_path: Path) -> None:
    uri = str(tmp_path / "base")
    write_deltalake(uri, BASE)
    snapshot = Snapshot("ds", uri, 0, 0, 0, (*FIELDS, FieldSpec("weight", "num")))
    with pytest.raises(ValueError, match="no column in the base table"):
        compact(snapshot, _Source(EVERY_OP.slice(0, 0)), _context(uri))
    assert DeltaTable(uri).version() == 0
