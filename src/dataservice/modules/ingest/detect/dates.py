"""Is a column dates, and in which order? Node's import-type-date.helper.ts, rule for rule.

"12/05/2024" alone could be 12 May or 5 Dec; a column usually proves the order (a first part above
12 proves day-first, a second above 12 month-first). Values with both parts ≤ 12 count only once
the column proves its order; a column proving both orders is inconsistent and never a date.
"""

import calendar
import re
from collections.abc import Callable
from dataclasses import dataclass
from typing import Literal

from dataservice.modules.ingest.detect import js

DateFormat = Literal["DMY", "MDY"]
Kind = Literal["iso", "month-name", "day-first", "month-first", "ambiguous-numeric", "not-date"]

_MONTH_PREFIX = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
}  # fmt: skip
_MONTH_NAMES = (
    "january", "february", "march", "april", "may", "june",
    "july", "august", "september", "october", "november", "december",
)  # fmt: skip


@dataclass(frozen=True)
class Parts:
    year: int
    month: int
    day: int


@dataclass(frozen=True)
class DateClass:
    kind: Kind
    parts: Parts | None = None
    first: int = 0
    second: int = 0
    year: int = 0


NOT_DATE = DateClass("not-date")


def _month(token: str) -> int | None:
    normalized = token.lower()
    month = _MONTH_PREFIX.get(normalized[:3])
    if not month:
        return None
    return month if _MONTH_NAMES[month - 1].startswith(normalized) else None


def _parts(year: int, month: int, day: int) -> Parts | None:
    if year < 1000 or year > 9999 or month < 1 or month > 12:
        return None
    if day < 1 or day > calendar.monthrange(year, month)[1]:
        return None
    return Parts(year, month, day)


def _dated(kind: Kind, parts: Parts | None) -> DateClass:
    return DateClass(kind, parts) if parts else NOT_DATE


def _iso(match: re.Match[str]) -> DateClass:
    return _dated("iso", _parts(int(match[1]), int(match[2]), int(match[3])))


def _numeric(match: re.Match[str]) -> DateClass:
    first, second, year = int(match[1]), int(match[2]), int(match[3])
    if first > 12 and second <= 12:
        return _dated("day-first", _parts(year, second, first))
    if second > 12 and first <= 12:
        return _dated("month-first", _parts(year, first, second))
    if first <= 12 and second <= 12:
        if first == second:  # 05/05/2024 is the same date either way
            return _dated("day-first", _parts(year, second, first))
        return DateClass("ambiguous-numeric", None, first, second, year) if 1000 <= year <= 9999 else NOT_DATE
    return NOT_DATE


def _day_month(match: re.Match[str]) -> DateClass:
    month = _month(match[2])
    return _dated("month-name", _parts(int(match[3]), month, int(match[1]))) if month else NOT_DATE


def _month_day(match: re.Match[str]) -> DateClass:
    month = _month(match[1])
    return _dated("month-name", _parts(int(match[3]), month, int(match[2]))) if month else NOT_DATE


_A = re.ASCII
_S = js.S
_ISO_DATE = r"([0-9]{4})[-/.]([0-9]{1,2})[-/.]([0-9]{1,2})"
_ISO_TIME = r"(?:[ T][0-9]{2}:[0-9]{2}(?::[0-9]{2}(?:\.[0-9]+)?)?)?"
CATALOG: tuple[tuple[re.Pattern[str], Callable[[re.Match[str]], DateClass]], ...] = (
    (re.compile(_ISO_DATE + _ISO_TIME, _A), _iso),
    (re.compile(r"([0-9]{1,2})[-/.]([0-9]{1,2})[-/.]([0-9]{4})", _A), _numeric),
    (re.compile(rf"([0-9]{{1,2}})(?:st|nd|rd|th)?[-{_S}.]*([A-Za-z]{{3,9}})[-{_S}.,]*([0-9]{{4}})", _A), _day_month),
    (re.compile(rf"([A-Za-z]{{3,9}})[-{_S}.]*([0-9]{{1,2}})(?:st|nd|rd|th)?[,{_S}.]*([0-9]{{4}})", _A), _month_day),
)  # fmt: skip


def classify(raw: str) -> DateClass:
    value = js.trim(raw)
    if not value:
        return NOT_DATE
    for pattern, decide in CATALOG:
        match = pattern.fullmatch(value)
        if match:
            found = decide(match)
            if found.kind != "not-date":
                return found
    return NOT_DATE


@dataclass
class Evidence:
    iso: int = 0
    month_name: int = 0
    day_first: int = 0
    month_first: int = 0
    ambiguous: int = 0

    def register(self, raw: str) -> None:
        kind = classify(raw).kind
        if kind == "iso":
            self.iso += 1
        elif kind == "month-name":
            self.month_name += 1
        elif kind == "day-first":
            self.day_first += 1
        elif kind == "month-first":
            self.month_first += 1
        elif kind == "ambiguous-numeric":
            self.ambiguous += 1


@dataclass(frozen=True)
class Summary:
    format: DateFormat | None
    conflict: bool
    parseable_count: int
    ambiguous_count: int


def summarize(evidence: Evidence) -> Summary:
    conflict = evidence.day_first > 0 and evidence.month_first > 0
    fmt: DateFormat | None = None
    if not conflict:
        fmt = "DMY" if evidence.day_first > 0 else "MDY" if evidence.month_first > 0 else None
    order_safe = evidence.iso + evidence.month_name
    parseable = (
        order_safe
        if conflict
        else order_safe + evidence.day_first + evidence.month_first + (evidence.ambiguous if fmt else 0)
    )
    return Summary(fmt, conflict, parseable, evidence.ambiguous)


def parse(raw: str, fmt: DateFormat | None) -> str | None:
    """parseImportDateValue: one value as "YYYY-MM-DD" under the column's proven order, or None."""
    found = classify(raw)
    if found.kind == "not-date":
        return None
    parts = found.parts
    if found.kind == "ambiguous-numeric":
        if not fmt:
            return None
        parts = (
            _parts(found.year, found.second, found.first)
            if fmt == "DMY"
            else _parts(found.year, found.first, found.second)
        )
    return None if parts is None else f"{parts.year}-{parts.month:02d}-{parts.day:02d}"
