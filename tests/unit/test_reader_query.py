"""The reader at scale: only the needed columns, filter / sort / page / count inside DuckDB,
patches laid over only what they touch, a full read as a stream — and the same answers as the
plain merge whatever the path."""

from datetime import UTC, date, datetime
from pathlib import Path

import pyarrow as pa
import pytest
from deltalake import write_deltalake

from dataservice.common.errors import ValidationError
from dataservice.storage import reader
from dataservice.storage.reader import Filter, Query, Sort, count, explain, merge, query, scan
from dataservice.storage.snapshot import PATCH_SCHEMA, FieldSpec, Snapshot

FIELDS = (FieldSpec("age", "num"), FieldSpec("name", "str"), FieldSpec("visit", "date"), FieldSpec("symptoms", "opts"))

BASE = pa.table(
    {
        "_rid": ["r1", "r2", "r3", "r4", "r5"],
        "_deleted_at": pa.array(
            [None, None, datetime(2026, 10, 1, tzinfo=UTC), None, None], pa.timestamp("us", tz="UTC")
        ),
        "f_age": [30.0, 41.0, 55.0, None, 41.0],
        "f_name": ["Asha", "Ravi", "Meena", "ravindra", None],
        "f_visit": pa.array([date(2026, 1, 5), None, date(2026, 2, 1), date(2026, 3, 1), None], pa.date32()),
        "f_symptoms": pa.array([["o_fever"], [], None, ["o_cough", "o_fever"], None], pa.list_(pa.string())),
    }
)


def _patch(
    seq: int, op: str, row: str, field: str | None = None, t: str | None = None, **values: object
) -> dict[str, object]:
    return {"seq": seq, "op": op, "row": row, "field": field, "t": t} | {
        column: values.get(column) for column in ("v_str", "v_num", "v_date", "v_opts")
    }


def _patches(*rows: dict[str, object]) -> pa.Table:
    return pa.Table.from_pylist(list(rows), schema=PATCH_SCHEMA)


class _Source:
    def __init__(self, table: pa.Table) -> None:
        self.table = table

    def patches(self, dataset_id: str, from_seq: int, to_seq: int) -> pa.Table:
        return self.table


def _snapshot(uri: str = "") -> Snapshot:
    return Snapshot("ds", uri, 0, 0, 100, FIELDS)


def _ids(table: pa.Table) -> list[str]:
    return [str(value) for value in table.column("_rid").to_pylist()]


EDITS = _patches(
    _patch(1, "set", "r1", "age", "num", v_num=99.0),  # patched past a filter's bound
    _patch(2, "insert_row", "r9"),
    _patch(3, "set", "r9", "name", "str", v_str="Asha new"),
    _patch(4, "delete_row", "r2"),
)


def _filtered(*filters: Filter, patches: pa.Table = EDITS) -> list[str]:
    return _ids(merge(BASE, patches, _snapshot(), Query(filters=filters)))


def test_only_the_asked_fields_come_back() -> None:
    table = merge(BASE, EDITS, _snapshot(), Query(fields=("name",)))
    assert table.schema.names == ["_rid", "f_name"]
    assert _ids(table) == ["r1", "r4", "r5", "r9"]  # r2 deleted by a patch, r3 in base


def test_filters_see_the_patched_value_not_the_base_one() -> None:
    assert _filtered(Filter("age", "gt", 90)) == ["r1"]
    assert _filtered(Filter("age", "lt", 40)) == []  # r1's base 30 no longer counts
    assert _filtered(Filter("name", "contains", "asha")) == ["r1", "r9"]  # inserted row, patched cell


def test_each_filter_operator() -> None:
    assert _filtered(Filter("age", "eq", 41)) == ["r5"]
    assert _filtered(Filter("age", "ne", 41)) == ["r1", "r4", "r9"]  # blanks are "not 41"
    assert _filtered(Filter("age", "le", 41), Filter("age", "ge", 41)) == ["r5"]
    assert _filtered(Filter("age", "in", [41, 99])) == ["r1", "r5"]
    assert _filtered(Filter("age", "is_null")) == ["r4", "r9"]
    assert _filtered(Filter("visit", "not_null")) == ["r1", "r4"]
    assert _filtered(Filter("visit", "ge", date(2026, 2, 1))) == ["r4"]
    assert _filtered(Filter("name", "contains", "RAVI")) == ["r4"]
    assert _filtered(Filter("symptoms", "contains", "o_fever")) == ["r1", "r4"]


def test_a_filter_value_is_data_never_sql() -> None:
    assert _filtered(Filter("name", "eq", "x' OR '1'='1")) == []


def test_sort_puts_empty_cells_last_and_the_row_id_breaks_ties() -> None:
    by_age = Query(fields=("age",), sort=(Sort("age", descending=True),))
    assert _ids(merge(BASE, EDITS, _snapshot(), by_age)) == ["r1", "r5", "r4", "r9"]
    by_age_up = Query(fields=("age",), sort=(Sort("age"),))
    assert _ids(merge(BASE, EDITS, _snapshot(), by_age_up)) == ["r5", "r1", "r4", "r9"]


def test_pages_cover_every_row_once_in_a_stable_order() -> None:
    sort = (Sort("name"),)
    whole = _ids(merge(BASE, EDITS, _snapshot(), Query(sort=sort)))
    pages = [_ids(merge(BASE, EDITS, _snapshot(), Query(sort=sort, limit=3, offset=at))) for at in (0, 3)]
    assert pages[0] + pages[1] == whole and len(pages[0]) == 3


def test_a_sort_field_need_not_be_returned() -> None:
    table = merge(BASE, EDITS, _snapshot(), Query(fields=("name",), sort=(Sort("age"),)))
    assert table.schema.names == ["_rid", "f_name"] and _ids(table) == ["r5", "r1", "r4", "r9"]


def test_count_on_a_delta_base(tmp_path: Path) -> None:
    uri = str(tmp_path / "base")
    write_deltalake(uri, BASE)
    source = _Source(EDITS)
    assert count(_snapshot(uri), source) == 4  # r1 r4 r5 + inserted r9
    assert count(_snapshot(uri), source, Query(include_deleted=True)) == 6
    assert count(_snapshot(uri), source, Query(filters=(Filter("age", "gt", 90),))) == 1
    assert count(_snapshot(uri), _Source(_patches())) == 4  # no patches: base only, r3 trashed


def test_query_on_a_delta_base_matches_the_in_memory_merge(tmp_path: Path) -> None:
    uri = str(tmp_path / "base")
    write_deltalake(uri, BASE)
    for request in (
        Query(),
        Query(include_deleted=True),
        Query(fields=("age", "symptoms"), filters=(Filter("age", "ne", 41),), sort=(Sort("age"),), limit=2),
    ):
        expected = merge(BASE, EDITS, _snapshot(), request)
        assert query(_snapshot(uri), _Source(EDITS), request).to_pylist() == expected.to_pylist()


def test_an_empty_delta_base_reads_as_no_rows(tmp_path: Path) -> None:
    uri = str(tmp_path / "base")
    write_deltalake(uri, BASE.slice(0, 0))
    assert query(_snapshot(uri), _Source(_patches())).num_rows == 0
    rows = query(_snapshot(uri), _Source(_patches(_patch(1, "insert_row", "r1"))))
    assert _ids(rows) == ["r1"]


def test_scan_streams_every_row_in_batches(tmp_path: Path) -> None:
    uri = str(tmp_path / "base")
    write_deltalake(uri, BASE)
    with scan(_snapshot(uri), _Source(EDITS), include_deleted=True, batch_rows=2) as reader:
        batches = list(reader)
    assert sorted(_ids(pa.Table.from_batches(batches))) == ["r1", "r2", "r3", "r4", "r5", "r9"]
    assert "_deleted_at" in batches[0].schema.names
    with scan(_snapshot(uri), _Source(EDITS)) as live:
        assert sorted(_ids(live.read_all())) == ["r1", "r4", "r5", "r9"]


def test_a_filter_on_an_untouched_field_reaches_the_parquet_scan(tmp_path: Path) -> None:
    uri = str(tmp_path / "base")
    write_deltalake(uri, BASE)
    plan = explain(
        _snapshot(uri), _Source(EDITS), Query(fields=("name",), filters=(Filter("visit", "ge", date(2026, 2, 1)),))
    )
    assert "Filters: f_visit >= '2026-02-01'::DATE" in plan  # pushed into the Parquet scan itself
    assert "f_symptoms" not in plan  # a column nobody asked for is never read


def test_bad_requests_are_refused_as_validation_errors() -> None:
    for request in (
        Query(fields=("nope",)),
        Query(filters=(Filter("age", "gt", "41"),)),  # text for a number
        Query(filters=(Filter("age", "gt", True),)),
        Query(filters=(Filter("symptoms", "eq", "o_fever"),)),
        Query(filters=(Filter("age", "contains", "4"),)),
        Query(filters=(Filter("age", "in", []),)),
        Query(sort=(Sort("symptoms"),)),
        Query(limit=-1),
        Query(offset=-1),
    ):
        with pytest.raises(ValidationError):
            merge(BASE, EDITS, _snapshot(), request)


def test_a_clear_empties_the_cell_even_if_the_patch_carries_a_value() -> None:
    patches = _patches(_patch(1, "set", "r1", "name", "null", v_str="leftover"))
    assert merge(BASE, patches, _snapshot(), Query(fields=("name",))).to_pylist()[0] == {"_rid": "r1", "f_name": None}


PAGE_REQUESTS = (
    Query(sort=(Sort("age", descending=True),), limit=2),  # sorted by a patched field
    Query(fields=("name",), sort=(Sort("name"),), limit=2, offset=2),  # the inserted row on the page
    Query(filters=(Filter("age", "ne", 41),), limit=10),
    Query(include_deleted=True, sort=(Sort("visit"),), limit=4),
    Query(limit=3, offset=50),  # past the end: no rows
    Query(limit=0),
)


@pytest.mark.parametrize("request_", PAGE_REQUESTS)
def test_a_page_read_in_two_passes_equals_the_one_pass_read(request_: Query, monkeypatch: pytest.MonkeyPatch) -> None:
    two_passes = merge(BASE, EDITS, _snapshot(), request_)
    monkeypatch.setattr(reader, "PAGE_ROWS_MAX", -1)  # every request in one pass
    one_pass = merge(BASE, EDITS, _snapshot(), request_)
    assert two_passes.schema == one_pass.schema and two_passes.to_pylist() == one_pass.to_pylist()


def test_a_page_reads_only_its_own_rows_in_full(tmp_path: Path) -> None:
    uri = str(tmp_path / "base")
    write_deltalake(uri, BASE)
    plan = explain(_snapshot(uri), _Source(EDITS), Query(fields=("symptoms",), sort=(Sort("visit"),), limit=2))
    find, read = plan.split("f_symptoms", 1)
    assert "f_visit" in find and "_rid IN" not in find  # pass 1: row id and sort column only
    assert "(_rid IN ('r1', 'r4'))" in read  # pass 2: the Parquet scan asks for the page's rows only


def test_a_row_id_with_a_quote_is_read_back_as_itself() -> None:
    base = BASE.set_column(0, "_rid", pa.array(["it's", "r2", "r3", "r4", "r5"]))
    page = merge(base, _patches(), _snapshot(), Query(fields=("age",), sort=(Sort("age"),), limit=1))
    assert page.to_pylist() == [{"_rid": "it's", "f_age": 30.0}]
