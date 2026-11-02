"""A column's evidence and its type — Node's import-column-stats.helper.ts, rule for rule."""

import math
from dataclasses import dataclass, field
from typing import Literal

from dataservice.modules.ingest.detect import choices, dates, js, numeric

IMPORT_NOTICE_SAMPLE_LIMIT = 3
UNIQUE_LIMIT = 1000

Confidence = Literal["HIGH", "MEDIUM", "LOW"]
DetectedType = Literal["text", "number", "date", "radio"]


@dataclass(frozen=True)
class Occurrence:
    row: int
    column_index: int
    cell: str


@dataclass
class NumericCorrection:
    value: str
    suggested: tuple[float, ...]
    confidence: Confidence
    occurrences: list[Occurrence] = field(default_factory=list)


@dataclass
class ColumnStats:
    total: int = 0
    non_empty: int = 0
    convertible_num: int = 0
    decimal_num: int = 0
    invalid_num: int = 0
    ambiguous_num: int = 0
    multi_value_num: int = 0
    multi_value_samples: list[str] = field(default_factory=list)
    date_count: int = 0
    date_evidence: dates.Evidence = field(default_factory=dates.Evidence)
    unique: dict[str, int] = field(default_factory=dict)
    raw_counts: dict[str, int] = field(default_factory=dict)
    capped: bool = False
    numeric_corrections: dict[str, NumericCorrection] = field(default_factory=dict)
    select_occurrences: dict[str, list[Occurrence]] = field(default_factory=dict)


def _cell_name(column: int, row: int) -> str:
    """`String.fromCharCode(65 + colIndex)` + row — Node's (single-letter) cell name."""
    return f"{chr(65 + column)}{row}"


def _register_numeric(
    stats: ColumnStats,
    normalized: str,
    raw: str,
    occurrence: Occurrence,
    confidence: Confidence,
    suggested: tuple[float, ...],
) -> None:
    entry = stats.numeric_corrections.get(normalized)
    if entry is None:
        entry = NumericCorrection(raw, suggested, confidence)
        stats.numeric_corrections[normalized] = entry
    entry.occurrences.append(occurrence)


def register_cell(stats: ColumnStats, value: str | float | bool, column: int, row: int) -> None:
    """registerCellStats for one non-empty resolved cell (`column` 0-based, `row` the sheet row)."""
    raw = js.trim(js.to_string(value))
    classified = numeric.classify(value)
    normalized = classified.normalized_raw or raw.lower()
    stats.date_evidence.register(raw)

    if not stats.capped:
        stats.unique[normalized] = stats.unique.get(normalized, 0) + 1
        if len(stats.unique) > UNIQUE_LIMIT:
            stats.unique.clear()
            stats.capped = True
    stats.raw_counts[raw] = stats.raw_counts.get(raw, 0) + 1

    occurrence = Occurrence(row, column + 1, _cell_name(column, row))
    stats.select_occurrences.setdefault(normalized, []).append(occurrence)

    if classified.kind == "ambiguous":
        stats.ambiguous_num += 1
        _register_numeric(stats, normalized, raw, occurrence, "LOW", classified.suggested_values)
        return
    if classified.kind == "convertible":
        stats.convertible_num += 1
        if classified.is_decimal_like:
            stats.decimal_num += 1
        if classified.suggested_values:
            _register_numeric(stats, normalized, raw, occurrence, "HIGH", classified.suggested_values)
        return
    stats.invalid_num += 1
    if classified.kind == "multi_value":
        stats.multi_value_num += 1
        if len(stats.multi_value_samples) < IMPORT_NOTICE_SAMPLE_LIMIT:
            stats.multi_value_samples.append(raw)
    if classified.has_digits:
        _register_numeric(stats, normalized, raw, occurrence, "LOW", ())


def detect_type(stats: ColumnStats) -> DetectedType:
    """detectFieldType: dates first, then decimals, choices, whole numbers, else text."""
    non_empty = stats.non_empty or 1
    number_ratio = stats.convertible_num / non_empty
    ambiguous_ratio = stats.ambiguous_num / non_empty
    invalid_ratio = stats.invalid_num / non_empty
    unique_count = math.inf if stats.capped else len(stats.unique)

    is_number = number_ratio >= 0.85 and ambiguous_ratio <= 0.15 and invalid_ratio <= 0.15
    has_decimal = is_number and stats.decimal_num > 0
    is_select = choices.is_select_candidate(
        unique_count,
        stats.non_empty,
        choices.NUMBER_TO_RADIO_MAX_UNIQUE if is_number else choices.SELECT_MAX_UNIQUE,
    )

    summary = dates.summarize(stats.date_evidence)
    date_ratio = summary.parseable_count / stats.non_empty if stats.non_empty else 0
    if not summary.conflict and summary.parseable_count > 0 and date_ratio >= 0.8:
        return "date"
    if has_decimal:
        return "number"
    if is_select:
        return choices.AUTO_CREATED_MCQ_TYPE
    if is_number:
        return "number"
    return "text"
