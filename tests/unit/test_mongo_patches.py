from datetime import date, datetime

import pyarrow as pa

from dataservice.storage.mongo_patches import to_patch_row
from dataservice.storage.snapshot import PATCH_SCHEMA


def _doc(op: str, value: dict[str, object] | None, field: str | None = "age") -> dict[str, object]:
    return {"seq": 4, "op": op, "target": {"row": "r1", "field": field}, "value": value}


def test_a_number_lands_in_v_num_as_a_float() -> None:
    row = to_patch_row(_doc("set", {"t": "num", "v": 31}))
    assert (row["t"], row["v_num"], row["v_str"]) == ("num", 31.0, None)
    assert isinstance(row["v_num"], float)


def test_a_bson_datetime_becomes_a_date_only_value() -> None:
    row = to_patch_row(_doc("set", {"t": "date", "v": datetime(2026, 3, 9)}))
    assert row["v_date"] == date(2026, 3, 9)


def test_a_cleared_cell_and_a_row_op_carry_no_value() -> None:
    cleared, inserted = (
        to_patch_row(_doc("set", {"t": "null", "v": None})),
        to_patch_row(_doc("insert_row", None, None)),
    )
    for row in (cleared, inserted):
        assert all(row[column] is None for column in ("v_str", "v_num", "v_date", "v_opts"))
    assert (cleared["t"], inserted["t"], inserted["field"]) == ("null", None, None)


def test_rows_fit_the_patch_schema() -> None:
    rows = [to_patch_row(_doc("set", {"t": "opts", "v": ["o_a", "o_b"]})), to_patch_row(_doc("delete_row", None, None))]
    table = pa.Table.from_pylist(rows, schema=PATCH_SCHEMA)
    assert table.column("v_opts").to_pylist() == [["o_a", "o_b"], None]
