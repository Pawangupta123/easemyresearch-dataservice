"""Numbers formatted as a text label (Node's isTextLabelNumFmt): Excel shows "Yes" for a cell that
holds 1 under `[=1]"Yes";[=0]"No";General`, and the import takes what the researcher sees.

calamine gives values without formats, so the few cells styled this way are found here, straight
in the workbook's XML: the styles whose number format is a label, then the first sheet's numeric
cells that use them. Workbooks without such a style — almost all — cost one small styles.xml read.
"""

import posixpath
import re
import zipfile
from collections.abc import Iterator
from xml.etree import ElementTree

from dataservice.modules.ingest.detect import js

_NS = {
    "m": "http://schemas.openxmlformats.org/spreadsheetml/2006/main",
    "r": "http://schemas.openxmlformats.org/officeDocument/2006/relationships",
    "rel": "http://schemas.openxmlformats.org/package/2006/relationships",
}
_M = "{" + _NS["m"] + "}"
_CELL_REF = re.compile(r"([A-Z]+)([0-9]+)")
_CONDITION = re.compile(r"\[(=|<>|<=|>=|<|>)(-?[0-9.]+)\]")
_LITERAL = re.compile(r'"([^"]*)"')


def is_text_label_format(code: str | None) -> bool:
    """Node's isTextLabelNumFmt — a label format, never currency or a date."""
    if not code:
        return False
    text = js.trim(code)
    if re.search(r"\[=[0-9]", text) and re.search(r'"[A-Za-z]', text):
        return True
    return bool(re.match(r'"[A-Za-z]', text))


def _sections(code: str) -> list[str]:
    """The format's ';'-separated sections (a ';' inside quotes is literal)."""
    sections, current, quoted = [], "", False
    for char in code:
        if char == '"':
            quoted = not quoted
        if char == ";" and not quoted:
            sections.append(current)
            current = ""
        else:
            current += char
    return [*sections, current]


def _matches(op: str, bound: float, value: float) -> bool:
    return {
        "=": value == bound,
        "<>": value != bound,
        "<": value < bound,
        ">": value > bound,
        "<=": value <= bound,
        ">=": value >= bound,
    }[op]


def _render(section: str, value: float) -> str | None:
    """A section as display text: quoted literals, or General; anything richer is not a label."""
    body = _CONDITION.sub("", section).strip()
    if body == "":
        return ""
    if body.lower() == "general":
        return js.number_to_string(value)
    rest = _LITERAL.sub("", body)
    if rest.strip():
        return None  # digit placeholders etc. — not a pure label
    return "".join(_LITERAL.findall(body))


def format_label(code: str, value: float) -> str | None:
    """What Excel displays for `value` under the label format `code` (None: not a label)."""
    sections = _sections(code)
    conditional = [(section, _CONDITION.search(section)) for section in sections]
    if any(match for _, match in conditional):
        for section, match in conditional:
            if match and _matches(match.group(1), float(match.group(2)), value):
                return _render(section, value)
        fallback = next((section for section, match in conditional if not match), None)
        return None if fallback is None else _render(fallback, value)
    if value > 0 or len(sections) == 1:
        return _render(sections[0], value)
    if value < 0:
        return _render(sections[1], abs(value)) if len(sections) > 1 else None
    return _render(sections[2], value) if len(sections) > 2 else _render(sections[0], value)


def _label_styles(archive: zipfile.ZipFile) -> dict[int, str]:
    """Style index → label format code, for the styles whose number format is a label."""
    try:
        root = ElementTree.fromstring(archive.read("xl/styles.xml"))
    except KeyError:
        return {}
    codes = {int(fmt.get("numFmtId", "-1")): fmt.get("formatCode", "") for fmt in root.iter(_M + "numFmt")}
    xfs = root.find("m:cellXfs", _NS)
    styles: dict[int, str] = {}
    for index, xf in enumerate([] if xfs is None else xfs.findall("m:xf", _NS)):
        code = codes.get(int(xf.get("numFmtId", "0")))
        if is_text_label_format(code) and code is not None:
            styles[index] = code
    return styles


def _first_sheet_path(archive: zipfile.ZipFile) -> str | None:
    workbook = ElementTree.fromstring(archive.read("xl/workbook.xml"))
    sheet = workbook.find("m:sheets/m:sheet", _NS)
    rel_id = None if sheet is None else sheet.get("{" + _NS["r"] + "}id")
    rels = ElementTree.fromstring(archive.read("xl/_rels/workbook.xml.rels"))
    for rel in rels.iter("{" + _NS["rel"] + "}Relationship"):
        if rel.get("Id") == rel_id:
            target = rel.get("Target", "")
            return target.lstrip("/") if target.startswith("/") else posixpath.normpath(posixpath.join("xl", target))
    return None


def _column(letters: str) -> int:
    index = 0
    for letter in letters:
        index = index * 26 + (ord(letter) - 64)
    return index - 1


def _numeric_cells(archive: zipfile.ZipFile, path: str, styles: dict[int, str]) -> Iterator[tuple[int, int, str]]:
    with archive.open(path) as stream:
        for _, element in ElementTree.iterparse(stream):
            if element.tag != _M + "c":
                continue
            style = int(element.get("s", "0"))
            kind = element.get("t", "n")
            value = element.find("m:v", _NS)
            if style in styles and kind == "n" and value is not None and value.text:
                ref = _CELL_REF.fullmatch(element.get("r", ""))
                if ref:
                    label = format_label(styles[style], float(value.text))
                    if label is not None and js.trim(label):
                        yield int(ref.group(2)) - 1, _column(ref.group(1)), label
            element.clear()


def text_label_cells(path: str) -> dict[tuple[int, int], str]:
    """(row, column) → the label shown, for the first sheet's label-formatted numbers (0-based)."""
    with zipfile.ZipFile(path) as archive:
        styles = _label_styles(archive)
        if not styles:
            return {}
        sheet = _first_sheet_path(archive)
        if sheet is None:
            return {}
        return {(row, column): label for row, column, label in _numeric_cells(archive, sheet, styles)}
