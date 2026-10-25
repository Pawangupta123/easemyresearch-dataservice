"""Follow-up visits through the one reader — the behaviour of Node's records today: a visit row
exists once it is submitted, a visit is shown while its main row is live, and the same field id
in the main visit, visit 1 and visit 2 are three different cells (C12)."""

from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
import pytest
from deltalake import write_deltalake

from dataservice.common.errors import ValidationError
from dataservice.storage.compaction import compact
from dataservice.storage.fenced_commit import CommitContext, OpRef
from dataservice.storage.reader import merge, merge_visits, query_visits
from dataservice.storage.snapshot import PATCH_SCHEMA, FieldSpec, Snapshot, TableVersion

AT = datetime(2026, 10, 24, 9, 30, tzinfo=UTC)
FIELDS = (FieldSpec("age", "num"), FieldSpec("weight", "num"))
VISIT_FIELDS = (FieldSpec("weight", "num"), FieldSpec("note", "str"))  # follow-ups reuse field ids
BASE = pa.table(
    {
        "_rid": ["r1", "r2", "r3"],
        "_deleted_at": pa.array([None, None, datetime(2026, 10, 1, tzinfo=UTC)], pa.timestamp("us", tz="UTC")),
        "f_age": [30.0, 41.0, 55.0],
        "f_weight": [70.0, 80.0, 90.0],
    }
)
VISITS = pa.table(
    {
        "_rid": ["r1", "r1", "r2", "r3"],
        "_follow_up": pa.array([1, 2, 1, 1], pa.int32()),
        "f_weight": [71.0, 72.0, 81.0, 91.0],
        "f_note": ["v1", "v2", "v1", "trashed row"],
    }
)


def _patch(
    seq: int, op: str, row: str, visit: int, field: str | None = None, t: str | None = None, **values: object
) -> dict[str, object]:
    return {"seq": seq, "op": op, "row": row, "follow_up": visit, "field": field, "t": t, "at": AT} | {
        column: values.get(column) for column in ("v_str", "v_num", "v_date", "v_opts")
    }


def _patches(*rows: dict[str, object]) -> pa.Table:
    return pa.Table.from_pylist(list(rows), schema=PATCH_SCHEMA)


def _snapshot(followups: TableVersion | None = None, uri: str = "") -> Snapshot:
    return Snapshot("ds", uri, 0, 0, 100, FIELDS, followups=followups, visit_fields=VISIT_FIELDS)


def _visits(patches: pa.Table, follow_up: int | None = None) -> dict[tuple[str, int], dict[str, object]]:
    table = merge_visits(BASE, VISITS, patches, _snapshot(), follow_up)
    return {(str(row.pop("_rid")), int(str(row.pop("_follow_up")))): row for row in table.to_pylist()}


def test_stored_visits_of_live_rows_come_back_in_row_then_visit_order() -> None:
    table = merge_visits(BASE, VISITS, _patches(), _snapshot())
    assert [(row["_rid"], row["_follow_up"]) for row in table.to_pylist()] == [("r1", 1), ("r1", 2), ("r2", 1)]
    assert table.schema.names == ["_rid", "_follow_up", "f_weight", "f_note"]


def test_c12_the_same_field_in_two_visits_and_the_main_visit_are_three_cells() -> None:
    patches = _patches(
        _patch(1, "set", "r1", 1, "weight", "num", v_num=75.0),  # visit 1
        _patch(2, "set", "r1", 2, "weight", "num", v_num=76.0),  # visit 2, same field
        _patch(3, "set", "r1", 1, "weight", "num", v_num=74.0),  # visit 1 again: visit 2 untouched
    )
    visits = _visits(patches)
    assert (visits[("r1", 1)]["f_weight"], visits[("r1", 2)]["f_weight"]) == (74.0, 76.0)
    main = {row["_rid"]: row for row in merge(BASE, patches, _snapshot()).to_pylist()}
    assert main["r1"]["f_weight"] == 70.0  # a visit's patch never reaches the main visit's cell


def test_a_main_visit_patch_never_reaches_a_follow_up_visit() -> None:
    patches = _patches(_patch(1, "set", "r1", 0, "weight", "num", v_num=99.0))
    assert merge(BASE, patches, _snapshot()).to_pylist()[0]["f_weight"] == 99.0
    assert _visits(patches)[("r1", 1)]["f_weight"] == 71.0


def test_a_submitted_visit_exists_at_once_and_a_cell_without_a_visit_shows_nowhere() -> None:
    patches = _patches(
        _patch(1, "insert_row", "r2", 2),  # visit 2 of r2 submitted
        _patch(2, "set", "r2", 2, "note", "str", v_str="second"),
        _patch(3, "set", "r2", 3, "note", "str", v_str="orphan"),  # visit 3 never submitted
        _patch(4, "insert_row", "r1", 1),  # visit 1 of r1 exists already: adds nothing
    )
    visits = _visits(patches)
    assert visits[("r2", 2)] == {"f_weight": None, "f_note": "second"} and ("r2", 3) not in visits
    keys = [(row["_rid"], row["_follow_up"]) for row in merge_visits(BASE, VISITS, patches, _snapshot()).to_pylist()]
    assert keys == [("r1", 1), ("r1", 2), ("r2", 1), ("r2", 2)]  # every visit once


def test_one_visit_number_alone() -> None:
    assert sorted(_visits(_patches(), follow_up=2)) == [("r1", 2)]
    assert sorted(_visits(_patches(), follow_up=1)) == [("r1", 1), ("r2", 1)]
    other_visit = _patches(_patch(1, "insert_row", "r2", 2), _patch(2, "set", "r2", 2, "note", "str", v_str="x"))
    assert sorted(_visits(other_visit, follow_up=1)) == [("r1", 1), ("r2", 1)]  # visit 2's patches stay out


def test_trashing_a_row_hides_its_visits_and_restoring_brings_them_back() -> None:
    trashed = _patches(_patch(1, "delete_row", "r1", 0))
    assert sorted(_visits(trashed)) == [("r2", 1)]
    restored = _patches(
        _patch(1, "delete_row", "r1", 0), _patch(2, "restore_row", "r1", 0), _patch(3, "restore_row", "r3", 0)
    )
    assert sorted(_visits(restored)) == [("r1", 1), ("r1", 2), ("r2", 1), ("r3", 1)]


def test_a_visit_of_a_row_inserted_by_a_patch_is_shown() -> None:
    patches = _patches(
        _patch(1, "insert_row", "r9", 0),
        _patch(2, "insert_row", "r9", 1),
        _patch(3, "set", "r9", 1, "weight", "num", v_num=60.0),
    )
    assert _visits(patches)[("r9", 1)] == {"f_weight": 60.0, "f_note": None}


def test_a_delete_or_restore_of_a_single_visit_is_refused() -> None:
    for op in ("delete_row", "restore_row"):
        with pytest.raises(ValueError, match="on a follow-up visit"):
            _visits(_patches(_patch(1, op, "r1", 1)))


def test_a_patch_without_a_visit_is_refused() -> None:
    unscoped = _patch(1, "set", "r1", 0, "age", "num", v_num=1.0) | {"follow_up": None}
    with pytest.raises(ValueError, match="without a visit"):
        merge(BASE, _patches(unscoped), _snapshot())


def test_bad_visit_requests_are_refused() -> None:
    for follow_up, fields in ((0, None), (-1, None), (1, ("nope",))):
        with pytest.raises(ValidationError):
            merge_visits(BASE, VISITS, _patches(), _snapshot(), follow_up, fields)


def test_visits_on_delta_at_the_pinned_version_and_none_before_the_first(tmp_path: Path) -> None:
    base, followups = str(tmp_path / "base"), str(tmp_path / "followups")
    write_deltalake(base, BASE)
    write_deltalake(followups, VISITS)
    write_deltalake(followups, VISITS.slice(0, 1), mode="overwrite")  # version 1: not the pinned one

    class _Source:
        def patches(self, dataset_id: str, from_seq: int, to_seq: int) -> pa.Table:
            return _patches(_patch(1, "set", "r2", 1, "note", "str", v_str="edited"))

    pinned = query_visits(_snapshot(TableVersion(followups, 0), base), _Source())
    assert [(row["_rid"], row["_follow_up"], row["f_note"]) for row in pinned.to_pylist()] == [
        ("r1", 1, "v1"),
        ("r1", 2, "v2"),
        ("r2", 1, "edited"),
    ]
    assert query_visits(_snapshot(None, base), _Source()).num_rows == 0  # no followups table yet


def test_compaction_refuses_while_visit_patches_would_be_dropped(tmp_path: Path) -> None:
    uri = str(tmp_path / "base")
    write_deltalake(uri, BASE)

    class _Source:
        def patches(self, dataset_id: str, from_seq: int, to_seq: int) -> pa.Table:
            return _patches(_patch(1, "set", "r1", 1, "weight", "num", v_num=75.0))

    with pytest.raises(ValueError, match="follow-up visit patches"):
        compact(_snapshot(uri=uri), _Source(), CommitContext(1, (OpRef("compact-1", 1),), ("base",)))
