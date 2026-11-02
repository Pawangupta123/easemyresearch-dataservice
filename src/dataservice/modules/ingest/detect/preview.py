"""An upload's preview fields — Node's buildImportPreviewFields (import-preview.helper.ts): one
field per header with its detected type, options, suggested corrections, notices and the
evidence behind them (typeMetrics), in the exact JSON shape Node stores (camelCase)."""

from collections.abc import Callable

from dataservice.modules.ingest.detect import choices, dates, js
from dataservice.modules.ingest.detect.cells import Sheet, header_of, last_meaningful_row, resolve
from dataservice.modules.ingest.detect.stats import (
    IMPORT_NOTICE_SAMPLE_LIMIT,
    ColumnStats,
    Occurrence,
    detect_type,
    register_cell,
)

IMPORT_OPTION_VALUE_ADVISORY_LENGTH = 200
Json = dict[str, object]


def _occurrence(occurrence: Occurrence) -> Json:
    return {"row": occurrence.row, "columnIndex": occurrence.column_index, "cell": occurrence.cell}


def _shorten(value: str, limit: int = 60) -> str:
    return value if js.length(value) <= limit else js.trim_end(js.slice_units(value, limit)) + "…"


def field_notices(field_name: str, options: list[str]) -> list[Json]:
    """buildFieldNotices: unusually long options mean several answers per cell."""
    long_ones = [option for option in options if js.length(option) > IMPORT_OPTION_VALUE_ADVISORY_LENGTH]
    if not long_ones:
        return []
    count = len(long_ones)
    return [
        {
            "kind": "long_options",
            "message": f'"{field_name}" has {count} unusually long {"option" if count == 1 else "options"}. '
            'Each cell may hold several answers at once (a "tick all that apply" column). '
            "Import it as text if that is the case.",
            "samples": [_shorten(option) for option in long_ones[:IMPORT_NOTICE_SAMPLE_LIMIT]],
        }
    ]


def multi_value_notices(field_name: str, detected: str, count: int, samples: list[str]) -> list[Json]:
    """buildMultiValueNotices: a number column with cells holding several numbers."""
    if detected != "number" or count == 0:
        return []
    return [
        {
            "kind": "multi_value_numbers",
            "message": f'"{field_name}" has {count} {"cell" if count == 1 else "cells"} holding several numbers. '
            "A number field keeps one number per cell, so these would be left empty. "
            "Import it as text to keep them as written.",
            "samples": [_shorten(sample) for sample in samples[:IMPORT_NOTICE_SAMPLE_LIMIT]],
        }
    ]


def _field(temp_field_id: str, field_name: str, column_index: int, stats: ColumnStats) -> Json:
    detected = detect_type(stats)
    summary = dates.summarize(stats.date_evidence)
    mcq = None if stats.capped else choices.mcq_suggestions(stats.raw_counts)

    options: list[str] = []
    corrections: list[Json] = []
    if detected == choices.AUTO_CREATED_MCQ_TYPE and mcq is not None:
        options = mcq.options
        corrections = [
            {
                "value": correction.value,
                "suggested": correction.suggested,
                "confidence": correction.confidence,
                "occurrences": [_occurrence(o) for o in stats.select_occurrences.get(correction.value.lower(), [])],
            }
            for correction in mcq.corrections
        ]
    if detected == "number":
        corrections = [
            {
                "value": correction.value,
                "suggested": [js.number_to_string(value) for value in correction.suggested],
                "confidence": correction.confidence,
                "occurrences": [_occurrence(o) for o in correction.occurrences],
            }
            for correction in stats.numeric_corrections.values()
        ]

    notices = field_notices(field_name, options) + multi_value_notices(
        field_name, detected, stats.multi_value_num, stats.multi_value_samples
    )
    field: Json = {
        "tempFieldId": temp_field_id,
        "fieldName": field_name,
        "columnIndex": column_index,
        "detectedType": detected,
        "options": options,
        "corrections": corrections,
        "typeMetrics": {
            "totalCount": stats.total,
            "nonEmptyCount": stats.non_empty,
            "uniqueCount": None if stats.capped else len(stats.unique),
            "uniqueCapped": stats.capped,
            "convertibleCount": stats.convertible_num,
            "ambiguousCount": stats.ambiguous_num,
            "invalidCount": stats.invalid_num,
            "decimalCount": stats.decimal_num,
            "dateCount": stats.date_count,
            "dateParseableCount": summary.parseable_count,
            "dateAmbiguousCount": summary.ambiguous_count,
            "dateFormat": summary.format,
            "dateFormatConflict": summary.conflict,
            "candidateOptions": mcq.options if mcq is not None else [],
        },
    }
    if notices:
        field["notices"] = notices
    return field


def preview_fields(sheet: Sheet, new_id: Callable[[], str]) -> list[Json]:
    """buildImportPreviewFields over the first sheet: row 1 is the header row."""
    headers = [header_of(cell) for cell in sheet.row(0)]
    stats = [ColumnStats() for _ in headers]
    ids = [new_id() for _ in headers]
    last_row = last_meaningful_row(sheet, len(headers))
    for row_number in range(2, last_row + 1):
        row = sheet.row(row_number - 1)
        for column, column_stats in enumerate(stats):
            column_stats.total += 1
            resolved = resolve(row[column] if column < len(row) else None)
            value = resolved.value
            if value is None or js.trim(js.to_string(value)) == "":
                continue
            column_stats.non_empty += 1
            if resolved.source == "date":
                column_stats.date_count += 1
            register_cell(column_stats, value, column, row_number)
    return [_field(ids[i], headers[i], i + 1, stats[i]) for i in range(len(headers))]
