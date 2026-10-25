"""Row operations at their edges, which pages take two passes, and requests refused before I/O."""

from datetime import UTC, date, datetime
from pathlib import Path

import pyarrow as pa
import pytest
from deltalake import write_deltalake

from dataservice.common.errors import ValidationError
from dataservice.storage.reader import (
    DEEP_OFFSET,
    PAGE_ROWS_MAX,
    Filter,
    Query,
    Sort,
    count,
    explain,
    merge,
    query,
    scan,
)
from dataservice.storage.snapshot import PATCH_SCHEMA, FieldSpec, Snapshot

AT = datetime(2026, 10, 24, 9, 30, tzinfo=UTC)
FIELDS = (FieldSpec("age", "num"), FieldSpec("name", "str"), FieldSpec("visit", "date"))
BASE = pa.table(
    {
        "_rid": ["r1", "r2", "r3"],
        "_deleted_at": pa.array([None, None, datetime(2026, 10, 1, tzinfo=UTC)], pa.timestamp("us", tz="UTC")),
        "f_age": [30.0, 41.0, 55.0],
        "f_name": ["Asha", "Ravi", "Meena"],
        "f_visit": pa.array([date(2026, 1, 5), None, date(2026, 2, 1)], pa.date32()),
    }
)


def _patch(
    seq: int, op: str, row: str, field: str | None = None, t: str | None = None, **values: object
) -> dict[str, object]:
    return {"seq": seq, "op": op, "row": row, "field": field, "t": t, "at": AT} | {
        column: values.get(column) for column in ("v_str", "v_num", "v_date", "v_opts")
    }


def _patches(*rows: dict[str, object]) -> pa.Table:
    return pa.Table.from_pylist(list(rows), schema=PATCH_SCHEMA)


class _Source:
    def __init__(self, table: pa.Table) -> None:
        self.table, self.calls = table, 0

    def patches(self, dataset_id: str, from_seq: int, to_seq: int) -> pa.Table:
        self.calls += 1
        return self.table


def _snapshot(uri: str = "") -> Snapshot:
    return Snapshot("ds", uri, 0, 0, 100, FIELDS)


def _ids(table: pa.Table) -> list[str]:
    return [str(value) for value in table.column("_rid").to_pylist()]


@pytest.fixture
def delta_base(tmp_path: Path) -> str:
    uri = str(tmp_path / "base")
    write_deltalake(uri, BASE)
    return uri


# ── row operations ───────────────────────────────────────────────────────────────────────────

ADDED_THEN_REMOVED = _patches(
    _patch(1, "insert_row", "r9"),
    _patch(2, "set", "r9", "age", "num", v_num=12.0),
    _patch(3, "delete_row", "r9"),
)


def test_a_row_inserted_then_deleted_is_in_the_trash_not_the_grid() -> None:
    assert _ids(merge(BASE, ADDED_THEN_REMOVED, _snapshot())) == ["r1", "r2"]
    trash = {
        row["_rid"]: row
        for row in merge(BASE, ADDED_THEN_REMOVED, _snapshot(), Query(include_deleted=True)).to_pylist()
    }
    assert trash["r9"]["_deleted_at"] == AT and trash["r9"]["f_age"] == 12.0
    page = merge(BASE, ADDED_THEN_REMOVED, _snapshot(), Query(sort=(Sort("age"),), limit=5))  # two passes
    assert _ids(page) == ["r1", "r2"]


def test_inserting_a_row_that_exists_adds_nothing_and_a_doubled_insert_adds_one() -> None:
    patches = _patches(
        _patch(1, "insert_row", "r1"),  # live in base
        _patch(2, "insert_row", "r3"),  # trashed in base: an insert does not restore it
        _patch(3, "insert_row", "r9"),
        _patch(4, "insert_row", "r9"),  # a retried insert
    )
    assert _ids(merge(BASE, patches, _snapshot())) == ["r1", "r2", "r9"]
    assert _ids(merge(BASE, patches, _snapshot(), Query(include_deleted=True))) == ["r1", "r2", "r3", "r9"]
    assert _ids(merge(BASE, patches, _snapshot(), Query(sort=(Sort("name"),), limit=10))) == ["r1", "r2", "r9"]


def test_count_with_trash_and_filters_counts_what_the_same_query_returns(delta_base: str) -> None:
    patches = _patches(_patch(1, "delete_row", "r2"), _patch(2, "set", "r1", "age", "num", v_num=90.0))
    source = _Source(patches)
    for filters in ((Filter("age", "gt", 40),), (Filter("visit", "not_null"),)):  # patched / untouched field
        request = Query(filters=filters, include_deleted=True)
        assert count(_snapshot(delta_base), source, request) == query(_snapshot(delta_base), source, request).num_rows
    assert count(_snapshot(delta_base), source, Query(filters=(Filter("age", "gt", 40),), include_deleted=True)) == 3


def test_a_delete_without_its_time_is_refused() -> None:
    untimed = _patch(1, "delete_row", "r1") | {"at": None}
    with pytest.raises(ValueError, match="without `at`"):
        merge(BASE, _patches(untimed), _snapshot())


def test_a_patch_without_a_row_id_is_refused() -> None:
    nowhere = _patch(1, "insert_row", "r9") | {"row": None}  # would add a row with no id, for good
    with pytest.raises(ValueError, match="without a row id"):
        merge(BASE, _patches(nowhere), _snapshot())


# ── one pass or two ──────────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("patches", "request_", "passes"),
    [
        (_patches(), Query(fields=("name",), sort=(Sort("age"),), limit=2), 1),  # nothing touched
        (
            _patches(_patch(1, "set", "r1", "age", "num", v_num=1.0)),
            Query(fields=("name",), sort=(Sort("age"),), limit=2),
            1,
        ),
        (_patches(_patch(1, "set", "r1", "name", "str", v_str="A")), Query(fields=("name",), limit=2), 2),
        (_patches(_patch(1, "delete_row", "r1")), Query(fields=("name",), limit=2), 2),  # row states
        (_patches(_patch(1, "set", "r1", "name", "str", v_str="A")), Query(fields=("name",)), 1),  # no limit
        (_patches(_patch(1, "set", "r1", "name", "str", v_str="A")), Query(limit=PAGE_ROWS_MAX + 1), 1),
        (_patches(), Query(limit=2, offset=DEEP_OFFSET), 2),  # a deep page, even untouched
    ],
)
def test_which_pages_take_two_passes(delta_base: str, patches: pa.Table, request_: Query, passes: int) -> None:
    assert len(explain(_snapshot(delta_base), _Source(patches), request_)) == passes


def test_a_full_stream_comes_in_batches_no_bigger_than_asked(tmp_path: Path) -> None:
    uri = str(tmp_path / "base")
    rows = 5000
    write_deltalake(
        uri,
        pa.table(
            {
                "_rid": [f"r{n:05d}" for n in range(rows)],
                "_deleted_at": pa.array([None] * rows, pa.timestamp("us", tz="UTC")),
                "f_age": [float(n) for n in range(rows)],
            }
        ),
    )
    snapshot = Snapshot("ds", uri, 0, 0, 0, (FieldSpec("age", "num"),))
    with scan(snapshot, _Source(_patches()), batch_rows=1000) as reader:
        sizes = [batch.num_rows for batch in reader]
    assert sum(sizes) == rows and max(sizes) <= 1000


# ── requests refused before any I/O ──────────────────────────────────────────────────────────

BAD = (
    Query(limit=2**63),
    Query(offset=2**63),
    Query(limit=True),  # a bool is not a count
    Query(filters=(Filter("age", "gt", float("nan")),)),
    Query(filters=(Filter("age", "gt", float("inf")),)),
    Query(filters=(Filter("age", "gt", 10**400),)),
    Query(filters=(Filter("visit", "eq", datetime(2026, 1, 5, 12, tzinfo=UTC)),)),  # a datetime is not a date
    Query(fields=("nope",)),
)


@pytest.mark.parametrize("request_", BAD)
def test_a_bad_request_fails_before_the_base_or_the_patches_are_read(request_: Query) -> None:
    source = _Source(_patches())
    with pytest.raises(ValidationError):
        query(Snapshot("ds", "/no/such/table", 0, 0, 0, FIELDS), source, request_)
    if request_.filters:
        with pytest.raises(ValidationError):
            count(Snapshot("ds", "/no/such/table", 0, 0, 0, FIELDS), source, request_)
    assert source.calls == 0


def test_a_huge_whole_number_filter_still_compares() -> None:
    assert _ids(merge(BASE, _patches(), _snapshot(), Query(filters=(Filter("age", "lt", 2**200),)))) == ["r1", "r2"]
