"""The import's golden vectors: the Data Service's detection must decide every fixture workbook
exactly as Node's import rules do (tests/fixtures/import_golden/*.golden.json, written by the
backend's scripts/import-golden/export-golden.ts from these same workbooks)."""

import json
from pathlib import Path

import pytest

from dataservice.modules.ingest.detect.cells import read_first_sheet, validate_headers
from dataservice.modules.ingest.detect.preview import ai_samples, preview_fields

FIXTURES = Path(__file__).resolve().parent.parent / "fixtures" / "import_golden"
WORKBOOKS = sorted(FIXTURES.glob("*.xlsx"))


def _detect(path: Path) -> dict[str, object]:
    sheet = read_first_sheet(str(path))
    assert sheet is not None
    counter = 0

    def new_id() -> str:
        nonlocal counter
        counter += 1
        return f"{counter:024x}"

    fields = preview_fields(sheet, new_id)
    return {
        "source": path.name,
        "headers": validate_headers(sheet.row(0)),
        "fields": fields,
        "samples": ai_samples(sheet, fields),
    }


def test_there_are_golden_vectors() -> None:
    assert len(WORKBOOKS) >= 5
    for workbook in WORKBOOKS:
        assert workbook.with_suffix(".golden.json").exists(), f"run the backend's export-golden for {workbook.name}"


@pytest.mark.parametrize("workbook", WORKBOOKS, ids=lambda path: path.stem)
def test_detection_matches_node(workbook: Path) -> None:
    expected = json.loads(workbook.with_suffix(".golden.json").read_text(encoding="utf-8"))
    actual = json.loads(json.dumps(_detect(workbook)))
    assert actual["headers"] == expected["headers"]
    assert actual["samples"] == expected["samples"]
    assert len(actual["fields"]) == len(expected["fields"])
    for mine, node in zip(actual["fields"], expected["fields"], strict=True):
        for key in sorted(set(mine) | set(node)):
            assert mine.get(key) == node.get(key), f"{workbook.stem} / {node['fieldName']} / {key}"
