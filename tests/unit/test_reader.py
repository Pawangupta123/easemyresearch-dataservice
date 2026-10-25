"""The one reader: base (Delta at its version) + patches in the revision's range (hybrid edits)."""

from datetime import UTC, date, datetime
from pathlib import Path

import pyarrow as pa
import pytest
from deltalake import write_deltalake

from dataservice.storage.reader import Query, merge, read_rows
from dataservice.storage.snapshot import PATCH_SCHEMA, FieldSpec, Snapshot

FIELDS = (FieldSpec("age", "num"), FieldSpec("name", "str"), FieldSpec("visit", "date"), FieldSpec("symptoms", "opts"))

BASE = pa.table(
    {
        "_rid": ["r1", "r2", "r3"],
        "_deleted_at": pa.array([None, None, datetime(2026, 10, 1, tzinfo=UTC)], pa.timestamp("us", tz="UTC")),
        "f_age": [30.0, 41.0, 55.0],
        "f_name": ["Asha", "Ravi", "Meena"],
        "f_visit": pa.array([date(2026, 1, 5), None, date(2026, 2, 1)], pa.date32()),
        "f_symptoms": pa.array([["o_fever"], [], None], pa.list_(pa.string())),
    }
)


WRITTEN_AT = datetime(2026, 10, 24, 12, 0, tzinfo=UTC)  # Node stamps every patch


def _patch(
    seq: int, op: str, row: str, field: str | None = None, t: str | None = None, **values: object
) -> dict[str, object]:
    return {"seq": seq, "op": op, "row": row, "follow_up": 0, "field": field, "t": t, "at": WRITTEN_AT} | {
        column: values.get(column) for column in ("v_str", "v_num", "v_date", "v_opts")
    }


def _patches(*rows: dict[str, object]) -> pa.Table:
    return pa.Table.from_pylist(list(rows), schema=PATCH_SCHEMA)


def _snapshot(patch_from: int = 0, patch_to: int = 100, base_uri: str = "", version: int = 0) -> Snapshot:
    return Snapshot("ds", base_uri, version, patch_from, patch_to, FIELDS)


def _rows(table: pa.Table) -> dict[str, dict[str, object]]:
    return {row.pop("_rid"): row for row in table.to_pylist()}


def test_without_patches_the_base_is_the_revision() -> None:
    rows = _rows(merge(BASE, _patches(), _snapshot()))
    assert set(rows) == {"r1", "r2"}  # r3 is deleted in base
    assert rows["r1"] == {"f_age": 30.0, "f_name": "Asha", "f_visit": date(2026, 1, 5), "f_symptoms": ["o_fever"]}


def test_the_latest_set_patch_wins_and_a_clear_empties_the_cell() -> None:
    patches = _patches(
        _patch(1, "set", "r1", "age", "num", v_num=31.0),
        _patch(2, "set", "r1", "age", "num", v_num=32.0),  # later server order wins
        _patch(3, "set", "r2", "name", "null"),  # cleared
        _patch(4, "set", "r2", "symptoms", "opts", v_opts=["o_cough", "o_fever"]),
        _patch(5, "set", "r1", "visit", "date", v_date=date(2026, 3, 9)),
    )
    rows = _rows(merge(BASE, patches, _snapshot()))
    assert rows["r1"]["f_age"] == 32.0 and rows["r1"]["f_visit"] == date(2026, 3, 9)
    assert rows["r2"]["f_name"] is None and rows["r2"]["f_symptoms"] == ["o_cough", "o_fever"]
    assert rows["r2"]["f_age"] == 41.0  # untouched cells keep the base value


def test_only_patches_inside_the_revision_range_count() -> None:
    patches = _patches(
        _patch(3, "set", "r1", "age", "num", v_num=99.0),  # ≤ patch_from: already folded into base
        _patch(5, "set", "r1", "name", "str", v_str="Asha K"),  # in range
        _patch(9, "set", "r1", "age", "num", v_num=77.0),  # > patch_to: a later revision
    )
    rows = _rows(merge(BASE, patches, _snapshot(patch_from=3, patch_to=8)))
    assert (rows["r1"]["f_age"], rows["r1"]["f_name"]) == (30.0, "Asha K")


def test_inserted_rows_exist_before_any_compaction() -> None:
    patches = _patches(_patch(1, "insert_row", "r9"), _patch(2, "set", "r9", "age", "num", v_num=12.0))
    rows = _rows(merge(BASE, patches, _snapshot()))
    assert rows["r9"] == {"f_age": 12.0, "f_name": None, "f_visit": None, "f_symptoms": None}


def test_delete_and_restore_follow_the_latest_row_patch() -> None:
    patches = _patches(
        _patch(1, "delete_row", "r1"),
        _patch(2, "restore_row", "r3"),  # deleted in base, restored by a patch
        _patch(3, "delete_row", "r2"),
        _patch(4, "restore_row", "r2"),
    )
    assert set(_rows(merge(BASE, patches, _snapshot()))) == {"r2", "r3"}
    with_deleted = merge(BASE, patches, _snapshot(), Query(include_deleted=True))
    assert set(_rows(with_deleted)) == {"r1", "r2", "r3"}


def test_rows_come_back_in_row_id_order() -> None:
    patches = _patches(_patch(1, "insert_row", "r0"))
    assert merge(BASE, patches, _snapshot()).column("_rid").to_pylist() == ["r0", "r1", "r2"]


def test_a_field_id_that_is_not_a_plain_identifier_is_refused() -> None:
    for bad in ("age'; DROP TABLE base; --", "age\n", ""):  # "$" alone would let the newline through
        with pytest.raises(ValueError, match="invalid field id"):
            FieldSpec(bad, "num")


class _Patches:
    def __init__(self, table: pa.Table) -> None:
        self.table = table
        self.asked: tuple[int, int] | None = None

    def patches(self, dataset_id: str, from_seq: int, to_seq: int) -> pa.Table:
        self.asked = (from_seq, to_seq)
        return self.table


def test_read_rows_reads_the_pinned_delta_version(tmp_path: Path) -> None:
    uri = str(tmp_path / "base")
    write_deltalake(uri, BASE)
    write_deltalake(uri, BASE.slice(0, 1), mode="overwrite")  # version 1 — not the pinned one
    source = _Patches(_patches(_patch(1, "set", "r2", "age", "num", v_num=42.0)))
    rows = _rows(read_rows(_snapshot(base_uri=uri, version=0, patch_to=1), source))
    assert set(rows) == {"r1", "r2"} and rows["r2"]["f_age"] == 42.0
    assert source.asked == (0, 1)


def test_with_deleted_rows_each_row_carries_its_deleted_time() -> None:
    deleted_at = datetime(2026, 10, 20, 9, 30, tzinfo=UTC)
    patches = _patches(
        _patch(1, "delete_row", "r1") | {"at": deleted_at},
        _patch(2, "restore_row", "r3") | {"at": datetime(2026, 10, 21, tzinfo=UTC)},
        _patch(3, "insert_row", "r9"),
    )
    rows = _rows(merge(BASE, patches, _snapshot(), Query(include_deleted=True)))
    assert rows["r1"]["_deleted_at"] == deleted_at  # the delete patch's time
    assert rows["r3"]["_deleted_at"] is None  # restored from the base's trash
    assert rows["r2"]["_deleted_at"] is None and rows["r9"]["_deleted_at"] is None
    assert "_deleted_at" not in merge(BASE, patches, _snapshot()).schema.names  # live view: no trash column
