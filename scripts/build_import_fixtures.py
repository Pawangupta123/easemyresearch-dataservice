"""Build the import golden-vector workbooks (plan 2.3 / skeleton step 7b).

    uv run python -m scripts.build_import_fixtures

Each workbook is one theme; each column one case the import's type rules must decide exactly as
Node's do (backend src/v2/modules/import/helpers). The backend's
`scripts/import-golden/export-golden.ts` runs Node's real rules over these files and writes the
expected preview next to each (`<name>.golden.json`); the Data Service's tests then require its
own detection to produce the same. Deterministic: no randomness, fixed dates.
"""

import re
import zipfile
from collections.abc import Callable, Sequence
from datetime import date, datetime
from pathlib import Path

from openpyxl import Workbook
from openpyxl.cell.rich_text import CellRichText, TextBlock
from openpyxl.cell.text import InlineFont

OUT = Path(__file__).resolve().parent.parent / "tests" / "fixtures" / "import_golden"
ROWS = 60

Cell = object
Column = tuple[str, Callable[[int], Cell]]


def _cycle(values: Sequence[Cell]) -> Callable[[int], Cell]:
    return lambda index: values[index % len(values)]


def _sparse(every: int, value: Callable[[int], Cell]) -> Callable[[int], Cell]:
    """Empty except every `every`-th row."""
    return lambda index: value(index) if index % every == 0 else None


NUMBERS: list[Column] = [
    ("Age", lambda i: 18 + i % 50),
    ("Weight kg", lambda i: f"{40 + i % 30} kg"),
    ("BMI", lambda i: round(18.5 + (i * 0.37) % 12, 2)),
    ("Fee", _cycle(["₹1,200", "₹950", "1,20,000", "Rs 800", "$45", "2 000"])),
    ("Duration", _cycle(["10-20", "5/7", "12", "15", "18", "20", "22", "25", "30", "35"])),
    ("Clock", _cycle(["10:30", "11:45 am", "09:00-10:00", "14:15:30"])),
    ("Codes", _cycle(["36620, 71045, 74018", "12; 15", "100", "200", "300", "400", "500", "600"])),
    ("Mostly numbers", lambda i: "unknown" if i % 9 == 0 else 100 + i),
    ("Score 1-3", _cycle([1, 2, 3])),
    ("Decimal codes", _cycle([1.5, 2.5, 3.5])),
    ("Negative", lambda i: -5 + i % 11),
    ("Scientific", _cycle(["1e3", "2.5E2", "0x1F", "  42  ", "+7", "-0", ".5", "5."])),
    ("Years", lambda i: f"{20 + i % 40} yrs"),
    # A number column with a few list cells: the multi-value notice and its sample limit (3).
    ("Lists in numbers", lambda i: f"{i}, {i + 1}, {i + 2}" if i % 12 == 0 else 1000 + i),
]

DATES: list[Column] = [
    ("ISO text", lambda i: f"2026-{1 + i % 12:02d}-{1 + i % 28:02d}"),
    ("Day first", lambda i: f"{1 + i % 28:02d}/{1 + i % 12:02d}/2025"),
    ("Month first", lambda i: f"{1 + i % 12:02d}/{1 + i % 28:02d}/2025"),
    ("Ambiguous only", lambda i: f"{1 + i % 12:02d}/{1 + (i + 1) % 12:02d}/2024"),
    ("Conflict", _cycle(["25/05/2024", "05/25/2024", "13/01/2024", "01/13/2024"])),
    ("Month names", _cycle(["12 May 2024", "May 12, 2024", "1st Jan 2025", "Sept 3 2023", "3-Mar-2022"])),
    ("Excel dates", lambda i: date(2025, 1 + i % 12, 1 + i % 28)),
    ("Excel datetimes", lambda i: datetime(2025, 1 + i % 12, 1 + i % 28, i % 24, (i * 7) % 60)),
    ("Mostly dates", lambda i: "not recorded" if i % 4 == 0 else f"2024-03-{1 + i % 28:02d}"),
    ("Invalid dates", _cycle(["31/02/2024", "2024-13-01", "00/01/2024", "12/12/24"])),
    ("ISO with time", lambda i: f"2024-06-{1 + i % 28:02d} 1{i % 10}:30"),
    # 51 of 60 dates (85%): above the date share a column needs (80%).
    ("Dates 85%", lambda i: "pending" if i % 7 == 0 else f"2024-04-{1 + i % 28:02d}"),
]

CATEGORIES: list[Column] = [
    ("Smoker", _cycle(["Yes", "No", "yes", "No", "Yes", "No", "Yse", "no ", "YES", "Nope"])),
    ("Sex", _cycle(["Male", "Female", "male", "Female", "Femal", "Male"])),
    ("Pain", _cycle(["Severe", "Mild", "Moderate", "None", "mild"])),
    ("Agreement", _cycle(["Agree", "Neutral", "Strongly agree", "Disagree", "Strongly disagree"])),
    ("Blood group", _cycle(["AB", "O", "B", "A"])),
    ("Ward", lambda i: f"Ward {i % 25}"),
    ("Patient name", lambda i: f"Patient {i}"),
    ("Stage", _cycle(["Stage IV", "Stage I", "Stage III", "Stage II"])),
    ("Codes as options", _cycle(["10", "2", "33", "4"])),
    ("Flags", _cycle([True, False, True])),
    ("Tick all", _cycle(["Fever, cough, sore throat, headache, body ache " * 5, "None"])),
    ("Rare", _sparse(5, _cycle(["Left", "Right"]))),
    # Four long answers in a choice column: the long-options notice and its sample limit (3).
    ("Long answers", lambda i: f"Answer {i % 4}: " + "several symptoms reported together " * 7),
    # 33 answers, 18 distinct (ratio 0.55): just above the choice limit (0.5), so text.
    ("Half unique", lambda i: f"Site {i % 18}" if i < 33 else None),
    # 20 answers, 10 distinct (ratio exactly 0.5): still a choice.
    ("At the limit", lambda i: f"Clinic {i % 10}" if i < 20 else None),
    # Two main answers and a typo only the loose threshold (0.6, ≤ 3 main answers) catches.
    ("Consent typo", lambda i: "ye" if i == 7 else ("Yes" if i % 2 else "No")),
    # A misspelling seen twice in 60: twice is already a main answer (no suggestion).
    ("Gender", lambda i: "Femail" if i in (3, 9) else ("Female" if i % 2 else "Male")),
    # Cell text named like JavaScript Object keys: a value / an option, never Object's function.
    ("Occupation", lambda i: ("Constructor", "toString", "valueOf")[i % 3] if i % 7 == 0 else f"Job {i}"),
    ("Role", _cycle(["Constructor", "Teacher", "Nurse", "toString"])),
]

STRUCTURE: list[Column] = [
    ("ID", lambda i: f"P-{i:04d}"),
    ("Spaces", _cycle(["  padded  ", " nbsp ", "﻿bom", "tab\t", "plain"])),
    ("Lines", _cycle(["line one\nline two", "single"])),
    ("Empty column", lambda _: None),
    ("Whitespace only", _cycle(["   ", "", None])),
    ("Unicode", _cycle(["हाँ", "नहीं", "हाँ", "नहीं", "हा"])),
    ("Emoji", _cycle(["😀 happy", "😢 sad", "😀 happy", "😀 hapy"])),
    ("Rich", lambda i: CellRichText([TextBlock(InlineFont(b=True), "Bold"), f" {i % 3}"])),
    ("CRLF", _cycle(["first\r\nsecond", "one line"])),
]


def _labels(sheet: object) -> None:
    """Numbers whose format shows a label: Excel displays 'Yes'/'No', the import reads the label."""
    ws = sheet
    ws.append(["Consent", "Plain number", "Percent", "Sections"])  # type: ignore[attr-defined]
    for index in range(ROWS):
        ws.append([index % 2, index % 2, (index % 10) / 10, index % 2])  # type: ignore[attr-defined]
        ws.cell(row=index + 2, column=1).number_format = '[=1]"Yes";[=0]"No";General'  # type: ignore[attr-defined]
        ws.cell(row=index + 2, column=3).number_format = "0%"  # type: ignore[attr-defined]
        ws.cell(row=index + 2, column=4).number_format = '"Yes";;"No"'  # type: ignore[attr-defined]


def _write(name: str, columns: list[Column], *, gap_after: int | None = None) -> None:
    workbook = Workbook()
    sheet = workbook.active
    if sheet is None:
        raise RuntimeError("no sheet")
    sheet.append([header for header, _ in columns])
    for index in range(ROWS):
        if gap_after is not None and index == gap_after:
            for _ in range(8):  # more empty rows than the import's stop threshold (7)
                sheet.append([])
        sheet.append([value(index) for _, value in columns])
    workbook.save(OUT / f"{name}.xlsx")
    _as_excel_writes(OUT / f"{name}.xlsx")


# Text that starts or ends with whitespace, without the attribute Excel always gives it.
_UNPRESERVED = re.compile(rb"<t>(\s[^<]*|[^<]*\s)</t>")


def _as_excel_writes(path: Path) -> None:
    """Store text as Excel does, not as openpyxl: a cell's CR as `_x000D_` (XML parsers turn a raw
    CR into a line feed), and `xml:space="preserve"` on text with leading or trailing spaces
    (without it, XML readers may drop the spaces — Excel never leaves it out)."""
    with zipfile.ZipFile(path) as source:
        parts = {name: source.read(name) for name in source.namelist()}
    sheet = "xl/worksheets/sheet1.xml"
    parts[sheet] = parts[sheet].replace(b"\r\r\n", b"_x000D_\n")
    parts[sheet] = _UNPRESERVED.sub(rb'<t xml:space="preserve">\1</t>', parts[sheet])
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as target:
        for name, data in parts.items():
            target.writestr(name, data)


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    _write("numbers", NUMBERS)
    _write("dates", DATES)
    _write("categories", CATEGORIES)
    _write("structure", STRUCTURE, gap_after=40)
    workbook = Workbook()
    sheet = workbook.active
    if sheet is None:
        raise RuntimeError("no sheet")
    _labels(sheet)
    workbook.save(OUT / "labels.xlsx")
    print(f"wrote {len(list(OUT.glob('*.xlsx')))} workbooks to {OUT}")


if __name__ == "__main__":
    main()
