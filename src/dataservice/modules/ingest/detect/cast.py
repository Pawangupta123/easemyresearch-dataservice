"""An upload's rows as the dataset's table — Node's final import (import-records.helper.ts:
importWorkbookRecords, import-finalize.helper.ts: transformCellToRecordEntry, and the Data Service
table shape of import-dataservice.helper.ts: toTableColumns), rule for rule.

Rows are walked bottom-to-top and every row gets Node's deterministic id (importRowRecordId), so
the grid's newest-first order is the sheet's own and a re-run writes the same ids.
"""

import hashlib
import re
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import datetime
from typing import Literal

from dataservice.modules.ingest.detect import dates, js, numeric
from dataservice.modules.ingest.detect.cells import Sheet, cell_text, last_meaningful_row, resolve

ValueType = Literal["str", "num", "date", "opts"]
CellValue = float | str | list[str] | None

_SINGLE_CHOICE = {"radio", "select"}  # Node's RECORD_SINGLE_CHOICE_FIELD_TYPES
_DAY = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}", re.ASCII)
ROW_OFFSET_LIMIT = 0xFFFFFF


@dataclass(frozen=True)
class FieldSpec:
    """One column as Node's rules read it (Node's IngestFieldSpec)."""

    field_id: str
    column_index: int  # 1-based
    type: str
    value_type: ValueType
    date_format: dates.DateFormat | None
    corrections: dict[str, str]  # lower-cased value → accepted correction
    options: dict[str, str]  # lower-cased option text → option id


def row_id(import_id: str, started_at: datetime, row_offset: int) -> str:
    """importRowRecordId: seconds + md5(import id)[:5] + the row's offset from the last row."""
    if not 0 <= row_offset <= ROW_OFFSET_LIMIT:
        raise ValueError(f"row offset {row_offset} is outside 0..{ROW_OFFSET_LIMIT}")
    seconds = int(started_at.timestamp())
    raw = seconds.to_bytes(4, "big") + hashlib.md5(import_id.encode(), usedforsecurity=False).digest()[:5]
    return (raw + row_offset.to_bytes(3, "big")).hex()


def _is_choice(field_type: str) -> bool:
    return field_type in _SINGLE_CHOICE


def _entry(value: str | float | bool | None, spec: FieldSpec) -> tuple[object, list[str]]:
    """transformCellToRecordEntry → (value, option ids)."""
    if value is not None and spec.corrections:
        key = js.trim(js.to_string(value)).lower()
        if key and key in spec.corrections:
            value = spec.corrections[key]
    if _is_choice(spec.type):
        if value is None:
            return None, []
        option = spec.options.get(js.trim(js.to_string(value)).lower())
        return None, [option] if option else []
    if spec.type == "number":
        classified = numeric.classify(value)
        return (classified.parsed_value if classified.kind == "convertible" else None), []
    if spec.type == "date":
        text = "" if value is None else js.trim(js.to_string(value))
        return dates.parse(text, spec.date_format), []
    return value, []


def _cell(value_type: ValueType, value: object, option_ids: list[str]) -> CellValue:
    """toCell: the stored value of one entry."""
    if value_type == "opts":
        return option_ids or None
    if value is None or value == "":
        return None
    if value_type == "num":
        return value if isinstance(value, float) and js.is_finite(value) else None
    if value_type == "date":
        text = js.to_string(value)
        return text[:10] if _DAY.match(text) else None
    return js.to_string(value)


def _meaningful(value: object, option_ids: list[str]) -> bool:
    if option_ids:
        return True
    return value is not None and js.trim(js.to_string(value)) != ""


def cast_rows(
    sheet: Sheet, specs: list[FieldSpec], import_id: str, started_at: datetime
) -> Iterator[dict[str, CellValue]]:
    """Each kept row (bottom-to-top) as `{_rid, f_<fieldId>: value}` — Node's toStagedRows without
    the meta columns the writer adds."""
    last = last_meaningful_row(sheet, sheet.column_count)
    for row_number in range(last, 1, -1):
        row = sheet.row(row_number - 1)
        cells = [row[spec.column_index - 1] if spec.column_index - 1 < len(row) else None for spec in specs]
        if not any(cell_text(cell) != "" for cell in cells):
            continue
        entries = [_entry(resolve(cell).value, spec) for cell, spec in zip(cells, specs, strict=True)]
        if not any(_meaningful(value, option_ids) for value, option_ids in entries):
            continue
        out: dict[str, CellValue] = {"_rid": row_id(import_id, started_at, last - row_number)}
        for spec, (value, option_ids) in zip(specs, entries, strict=True):
            out[f"f_{spec.field_id}"] = _cell(spec.value_type, value, option_ids)
        yield out
