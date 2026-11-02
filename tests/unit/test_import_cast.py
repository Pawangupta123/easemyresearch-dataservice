"""The final import's rows: the Data Service must cast every fixture workbook exactly as Node's own
walk does (the `ingest` part of tests/fixtures/import_golden/*.golden.json — Node's spec, and the
rows importWorkbookRecords + toTableColumns made from it)."""

import json
from datetime import datetime
from pathlib import Path

import pytest

from dataservice.modules.ingest.detect.cast import FieldSpec, cast_rows, row_id
from dataservice.modules.ingest.detect.cells import read_first_sheet

FIXTURES = Path(__file__).resolve().parent.parent / "fixtures" / "import_golden"
WORKBOOKS = sorted(FIXTURES.glob("*.xlsx"))


def spec_of(field: dict[str, object]) -> FieldSpec:
    return FieldSpec(
        field_id=str(field["fieldId"]),
        column_index=int(str(field["columnIndex"])),
        type=str(field["type"]),
        value_type=field["valueType"],  # type: ignore[arg-type]
        date_format=field["dateFormat"],  # type: ignore[arg-type]
        corrections=dict(field["corrections"]),  # type: ignore[call-overload]
        options=dict(field["options"]),  # type: ignore[call-overload]
    )


@pytest.mark.parametrize("workbook", WORKBOOKS, ids=lambda path: path.stem)
def test_rows_match_nodes_final_import(workbook: Path) -> None:
    golden = json.loads(workbook.with_suffix(".golden.json").read_text(encoding="utf-8"))["ingest"]
    spec = golden["spec"]
    sheet = read_first_sheet(str(workbook))
    assert sheet is not None
    rows = list(
        cast_rows(
            sheet,
            [spec_of(field) for field in spec["fields"]],
            spec["importId"],
            datetime.fromisoformat(spec["startedAt"]),
        )
    )
    expected = [{key: value for key, value in row.items() if key != "_created_by"} for row in golden["rows"]]
    assert len(rows) == len(expected)
    for index, (mine, node) in enumerate(zip(json.loads(json.dumps(rows)), expected, strict=True)):
        assert mine == node, f"{workbook.stem} row {index}"


def test_row_ids_are_nodes() -> None:
    started = datetime.fromisoformat("2026-11-02T10:00:00+00:00")
    assert row_id("00000000000000000000beef", started, 0)[:8] == f"{int(started.timestamp()):08x}"
    assert row_id("00000000000000000000beef", started, 255)[-6:] == "0000ff"
    with pytest.raises(ValueError):
        row_id("x", started, 0x1000000)
