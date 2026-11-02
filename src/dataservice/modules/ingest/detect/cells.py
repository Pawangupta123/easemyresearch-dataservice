"""An xlsx upload's first sheet, cell for cell as Node's import parser hands it on (backend
import-workbook.helper.ts + import-parse.helper.ts), read with calamine.

The contract every rule downstream relies on:
  empty / an error cell        → None
  text, rich text, hyperlink   → the plain string, CRLF as LF
  boolean                      → bool
  formula                      → its cached result
  a date-formatted number      → an `IsoDate`: "YYYY-MM-DD", or "YYYY-MM-DD HH:MM:SS" with a time
  a number formatted as a text label ([=1]"Yes";[=0]"No") → the label Excel shows (labels.py)
  any other number             → float (JavaScript has one number type)
Coordinates are absolute (row 0 = sheet row 1, the header), as Node's sheet ranges are.
"""

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta

from python_calamine import CalamineWorkbook

from dataservice.modules.ingest.detect import js
from dataservice.modules.ingest.detect.labels import text_label_cells

IMPORT_EMPTY_ROW_STOP_THRESHOLD = 7
# Excel's day 0 ("1900-01-00"): a time-only cell is a time on it, which JavaScript's
# Date.UTC(1900, 0, 0) places on 31 Dec 1899.
_DAY_ZERO = date(1899, 12, 31)


@dataclass(frozen=True)
class IsoDate:
    """A date cell, already in the import's ISO text (Node's formatImportDateValue)."""

    text: str


Cell = str | float | bool | IsoDate | None


@dataclass(frozen=True)
class Sheet:
    rows: list[list[Cell]]  # row 0 = the header row
    column_count: int

    @property
    def row_count(self) -> int:
        return len(self.rows)

    def row(self, index: int) -> list[Cell]:
        """Row `index` (0-based), as wide as the sheet; past the end an all-empty row."""
        if 0 <= index < len(self.rows):
            return self.rows[index]
        return [None] * self.column_count


def _iso(moment: datetime) -> IsoDate:
    whole = (moment + timedelta(microseconds=500_000)).replace(microsecond=0)  # SheetJS keeps whole seconds
    if whole.time() == time(0, 0, 0):
        return IsoDate(whole.strftime("%Y-%m-%d"))
    return IsoDate(whole.strftime("%Y-%m-%d %H:%M:%S"))


def to_cell(value: object) -> Cell:
    """One calamine value under the import's cell contract."""
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, int | float):
        return float(value)
    if isinstance(value, str):
        return value.replace("\r\n", "\n")
    if isinstance(value, datetime):
        return _iso(value)
    if isinstance(value, date):
        return IsoDate(value.strftime("%Y-%m-%d"))
    if isinstance(value, time):
        return _iso(datetime.combine(_DAY_ZERO, value))
    if isinstance(value, timedelta):
        return _iso(datetime.combine(_DAY_ZERO, time()) + value)
    return None


def read_first_sheet(path: str) -> Sheet | None:
    """The workbook's first sheet, or None when it has none."""
    workbook = CalamineWorkbook.from_path(path)
    if not workbook.sheet_names:
        return None
    raw: Sequence[Sequence[object]] = workbook.get_sheet_by_index(0).to_python(skip_empty_area=False)
    width = max((len(row) for row in raw), default=0)
    rows = [[to_cell(value) for value in row] + [None] * (width - len(row)) for row in raw]
    for (r, c), label in text_label_cells(path).items():
        if r < len(rows) and c < width:
            rows[r][c] = label
    return Sheet(rows=rows, column_count=width)


# ── Node's import-parse.helper.ts ────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class Resolved:
    value: str | float | bool | None
    source: str  # 'empty' | 'date' | 'primitive'


def resolve(cell: Cell) -> Resolved:
    """resolveImportCellValue: a date becomes its ISO text."""
    if cell is None:
        return Resolved(None, "empty")
    if isinstance(cell, IsoDate):
        return Resolved(cell.text, "date")
    return Resolved(cell, "primitive")


def cell_text(cell: Cell) -> str:
    """normalizeImportCellText: the resolved value as trimmed text, '' for empty."""
    value = resolve(cell).value
    return "" if value is None else js.trim(js.to_string(value))


def header_of(cell: Cell) -> str:
    """resolveImportHeaderCell: a header as trimmed text ('' for an empty cell)."""
    if cell is None:
        return ""
    if isinstance(cell, IsoDate):
        return cell.text
    return js.trim(js.to_string(cell))


def validate_headers(cells: list[Cell]) -> dict[str, object]:
    """validateImportHeaders: every column needs a header."""
    headers = [header_of(cell) for cell in cells]
    messages = [f"Column {index + 1} header cannot be empty" for index, header in enumerate(headers) if not header]
    if messages:
        return {"ok": False, "headers": headers, "message": "\n".join(messages)}
    return {"ok": True, "headers": headers}


def last_meaningful_row(sheet: Sheet, column_count: int) -> int:
    """findLastMeaningfulImportRow as a 1-based sheet row: data ends after the import's run of
    empty rows (IMPORT_EMPTY_ROW_STOP_THRESHOLD) once some data was seen."""
    last, empty_run, seen = 1, 0, False
    for row_number in range(2, sheet.row_count + 1):
        row = sheet.row(row_number - 1)
        if any(cell_text(cell) != "" for cell in row[:column_count]):
            seen, last, empty_run = True, row_number, 0
            continue
        if not seen:
            continue
        empty_run += 1
        if empty_run >= IMPORT_EMPTY_ROW_STOP_THRESHOLD:
            break
    return last
