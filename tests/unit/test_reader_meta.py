"""The grid's row shape through the one reader: who created a row and when, when it last changed
(Node's userId / createdAt / updatedAt), newest-first order, and a read of given row ids (an
edit's before-values) — the same answers on every path (one pass, two passes, the pure merge)."""

from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
import pytest
from bson import ObjectId
from deltalake import DeltaTable, write_deltalake

from dataservice.common.errors import ValidationError
from dataservice.storage.compaction import compact
from dataservice.storage.fenced_commit import CommitContext, OpRef, max_token_seen
from dataservice.storage.mongo_patches import to_patch_row
from dataservice.storage.reader import Query, Sort, count, merge, query
from dataservice.storage.reader.read import PAGE_ROWS_MAX
from dataservice.storage.snapshot import PATCH_SCHEMA, FieldSpec, Snapshot

FIELDS = (FieldSpec("age", "num"), FieldSpec("name", "str"))
UTC_US = pa.timestamp("us", tz="UTC")
MADE = datetime(2026, 9, 1, 8, 0, tzinfo=UTC)  # when the base rows were created
CHANGED = datetime(2026, 9, 2, 8, 0, tzinfo=UTC)  # their last change before any patch
EARLY = datetime(2026, 8, 1, tzinfo=UTC)  # a patch time before CHANGED (clock skew, a backfill)
LATE = datetime(2026, 10, 24, 12, 0, tzinfo=UTC)

BASE = pa.table(
    {
        "_rid": ["r1", "r2", "r3", "r4"],
        "_deleted_at": pa.array([None, None, datetime(2026, 10, 1, tzinfo=UTC), None], UTC_US),
        "_created_by": ["u1", "u2", "u1", "u2"],
        "_created_at": pa.array([MADE] * 4, UTC_US),
        "_updated_at": pa.array([CHANGED] * 4, UTC_US),
        "f_age": [30.0, 41.0, 55.0, 60.0],
        "f_name": ["Asha", "Ravi", "Meena", "Kiran"],
    }
)
NO_META = BASE.drop_columns(["_created_by", "_created_at", "_updated_at"])


def _patch(
    seq: int,
    op: str,
    row: str,
    field: str | None = None,
    t: str | None = None,
    *,
    at: datetime = LATE,
    actor: str | None = "u9",
    visit: int = 0,
    **values: object,
) -> dict[str, object]:
    cells = {column: values.get(column) for column in ("v_str", "v_num", "v_date", "v_opts")}
    return {"seq": seq, "op": op, "row": row, "follow_up": visit, "field": field, "t": t, "at": at, "actor": actor} | (
        cells
    )


def _patches(*rows: dict[str, object]) -> pa.Table:
    return pa.Table.from_pylist(list(rows), schema=PATCH_SCHEMA)


NONE = _patches()
EDITS = _patches(
    _patch(1, "set", "r1", "age", "num", v_num=31.0),  # a later edit moves r1's `_updated_at`
    _patch(2, "set", "r2", "name", "str", at=EARLY, v_str="Ravi K"),  # an earlier one does not
    _patch(3, "insert_row", "r9", at=datetime(2026, 10, 20, tzinfo=UTC), actor="u7"),
    _patch(4, "set", "r9", "name", "str", v_str="New", actor="u8"),  # another user fills it later
)


class _Source:
    def __init__(self, table: pa.Table) -> None:
        self.table = table

    def patches(self, dataset_id: str, from_seq: int, to_seq: int) -> pa.Table:
        return self.table


def _snapshot(uri: str = "", version: int = 0, patch_from: int = 0) -> Snapshot:
    return Snapshot("ds", uri, version, patch_from, 100, FIELDS)


def _rows(table: pa.Table) -> dict[str, dict[str, object]]:
    return {str(row.pop("_rid")): row for row in table.to_pylist()}


def _meta(table: pa.Table) -> dict[str, tuple[object, object, object]]:
    return {rid: (row["_created_by"], row["_created_at"], row["_updated_at"]) for rid, row in _rows(table).items()}


def test_meta_columns_come_back_only_when_asked_after_the_fields() -> None:
    assert merge(BASE, NONE, _snapshot()).schema.names == ["_rid", "f_age", "f_name"]
    table = merge(BASE, NONE, _snapshot(), Query(meta=True, include_deleted=True))
    assert table.schema.names == ["_rid", "f_age", "f_name", "_created_by", "_created_at", "_updated_at", "_deleted_at"]
    assert _meta(table)["r2"] == ("u2", MADE, CHANGED)


def test_a_patch_moves_updated_at_forward_only_and_an_inserted_row_is_its_inserters() -> None:
    meta = _meta(merge(BASE, EDITS, _snapshot(), Query(meta=True)))
    assert meta["r1"] == ("u1", MADE, LATE)  # the creator stays the base's, never the editor
    assert meta["r2"] == ("u2", MADE, CHANGED)  # an earlier patch time never moves it back
    assert meta["r4"] == ("u2", MADE, CHANGED)  # untouched
    assert meta["r9"] == ("u7", datetime(2026, 10, 20, tzinfo=UTC), LATE)  # inserter, not the later editor


def test_a_row_inserted_twice_keeps_its_first_insert() -> None:
    twice = _patches(
        _patch(1, "insert_row", "r9", at=datetime(2026, 10, 20, tzinfo=UTC), actor="u7"),
        _patch(2, "insert_row", "r9", at=datetime(2026, 10, 22, tzinfo=UTC), actor="u8"),
    )
    assert _meta(merge(BASE, twice, _snapshot(), Query(meta=True)))["r9"][:2] == (
        "u7",
        datetime(2026, 10, 20, tzinfo=UTC),
    )


def test_a_follow_up_save_moves_the_main_rows_updated_at_and_never_its_cells() -> None:
    visit = _patches(_patch(1, "set", "r1", "age", "num", visit=1, v_num=99.0))
    row = _rows(merge(BASE, visit, _snapshot(), Query(meta=True)))["r1"]
    assert (row["f_age"], row["_updated_at"]) == (30.0, LATE)  # visit 1's age is another cell (C12)
    both = _patches(
        _patch(1, "set", "r1", "age", "num", at=EARLY, v_num=32.0),  # the main visit's age
        _patch(2, "set", "r1", "age", "num", visit=1, v_num=99.0),  # later, the same field in visit 1
    )
    assert _rows(merge(BASE, both, _snapshot(), Query(meta=True)))["r1"]["f_age"] == 32.0


def test_a_trash_and_a_restore_move_updated_at_too() -> None:
    trashed = _patches(_patch(1, "delete_row", "r2"))
    assert _meta(merge(BASE, trashed, _snapshot(), Query(meta=True, include_deleted=True)))["r2"][2] == LATE


def test_a_base_without_meta_columns_reads_them_empty_and_patches_still_stamp() -> None:
    meta = _meta(merge(NO_META, EDITS, _snapshot(), Query(meta=True)))
    assert meta["r4"] == (None, None, None)
    assert meta["r1"] == (None, None, LATE)
    assert meta["r9"][0] == "u7"


def test_a_meta_column_of_another_type_is_refused_never_cast() -> None:
    wrong = BASE.set_column(BASE.schema.get_field_index("_created_at"), "_created_at", pa.array(["2026-09-01"] * 4))
    with pytest.raises(ValueError, match="_created_at is VARCHAR"):
        merge(wrong, NONE, _snapshot(), Query(meta=True))
    both = wrong.set_column(wrong.schema.get_field_index("f_age"), "f_age", pa.array(["30"] * 4))
    with pytest.raises(ValueError, match="age is num but its column is VARCHAR; _created_at is VARCHAR"):
        merge(both, NONE, _snapshot(), Query(meta=True))


def test_rows_come_newest_first_by_row_id_and_ties_of_a_sort_too() -> None:
    newest = Query(row_order="desc")
    assert list(_rows(merge(BASE, EDITS, _snapshot(), newest))) == ["r9", "r4", "r2", "r1"]
    tied = _patches(_patch(1, "set", "r4", "age", "num", v_num=41.0))  # r2 and r4 both 41
    by_age = Query(sort=(Sort("age"),), row_order="desc")
    assert list(_rows(merge(BASE, tied, _snapshot(), by_age))) == ["r1", "r4", "r2"]
    with pytest.raises(ValidationError, match="rowOrder"):
        merge(BASE, NONE, _snapshot(), Query(row_order="newest"))  # type: ignore[arg-type]


def test_given_row_ids_read_only_those_rows_inserted_and_trashed_ones_included() -> None:
    picked = Query(rids=("r9", "r3", "r1", "r1", "missing"), include_deleted=True, meta=True)
    rows = _rows(merge(BASE, EDITS, _snapshot(), picked))
    assert list(rows) == ["r1", "r3", "r9"]  # by row id; a duplicate once; an unknown id is no row
    assert rows["r3"]["_deleted_at"] is not None and rows["r9"]["f_name"] == "New"
    assert list(_rows(merge(BASE, EDITS, _snapshot(), Query(rids=("r3",))))) == []  # trashed, not asked for
    assert list(_rows(merge(BASE, EDITS, _snapshot(), Query(rids=())))) == []


def test_row_ids_are_checked_before_any_read() -> None:
    with pytest.raises(ValidationError, match=f"At most {PAGE_ROWS_MAX}"):
        merge(BASE, NONE, _snapshot(), Query(rids=tuple(f"r{n}" for n in range(PAGE_ROWS_MAX + 1))))
    with pytest.raises(ValidationError, match="non-empty strings"):
        merge(BASE, NONE, _snapshot(), Query(rids=("r1", "")))
    with pytest.raises(ValidationError, match="does not take row ids"):
        count(_snapshot(), _Source(NONE), Query(rids=("r1",)))


@pytest.mark.parametrize(
    "request_",
    [
        Query(meta=True, row_order="desc", limit=2),  # a page: patches touch it → two passes
        Query(meta=True, row_order="desc", limit=2, offset=1),
        Query(meta=True, fields=("age",), limit=3),  # only meta is patched on some rows
        Query(meta=True, sort=(Sort("name", descending=True),), limit=2),
        Query(meta=True, rids=("r2", "r9"), include_deleted=True),
        Query(meta=True),
    ],
)
def test_every_path_over_delta_gives_the_pure_merges_answer(tmp_path: Path, request_: Query) -> None:
    uri = str(tmp_path / "base")
    write_deltalake(uri, BASE)
    assert query(_snapshot(uri), _Source(EDITS), request_) == merge(BASE, EDITS, _snapshot(), request_)


def test_compaction_keeps_the_meta_columns_with_the_moved_updated_at(tmp_path: Path) -> None:
    uri = str(tmp_path / "base")
    write_deltalake(uri, BASE)
    before = query(_snapshot(uri), _Source(EDITS), Query(meta=True, include_deleted=True))
    context = CommitContext(max_token_seen([DeltaTable(uri)]) + 1, (OpRef("compact-1", 1),), ("base",))
    result = compact(_snapshot(uri, patch_from=0), _Source(EDITS), context)
    after = query(_snapshot(uri, result.base_version, 100), _Source(EDITS), Query(meta=True, include_deleted=True))
    assert after == before
    assert DeltaTable(uri).schema().to_arrow().names == BASE.schema.names  # nothing added, nothing lost


def test_compaction_of_a_base_without_meta_columns_adds_none(tmp_path: Path) -> None:
    uri = str(tmp_path / "base")
    write_deltalake(uri, NO_META)
    context = CommitContext(max_token_seen([DeltaTable(uri)]) + 1, (OpRef("compact-1", 1),), ("base",))
    compact(_snapshot(uri), _Source(EDITS), context)
    assert DeltaTable(uri).schema().to_arrow().names == NO_META.schema.names


def test_the_mongo_patch_actor_becomes_a_hex_string() -> None:
    document = {
        "seq": 1,
        "op": "insert_row",
        "target": {"row": "r9", "scope": {"kind": "main", "followUp": 0}},
        "value": None,
        "at": datetime(2026, 10, 20),
    }
    user = ObjectId()
    assert to_patch_row(document | {"actor": user})["actor"] == str(user)
    assert to_patch_row(document | {"actor": str(user)})["actor"] == str(user)
    assert to_patch_row(document)["actor"] is None
