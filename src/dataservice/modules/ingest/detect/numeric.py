"""Is a cell a number? Node's import-type-numeric.helper.ts, rule for rule.

Every regex is JavaScript's: ASCII digits and letters (re.ASCII), JavaScript's `\\s` (js.S), and
anchors that never let a trailing newline through (fullmatch).
"""

import re
from dataclasses import dataclass, field
from typing import Literal

from dataservice.modules.ingest.detect import js

IMPORT_NUMERIC_ALLOWED_WORDS = ("rs", "inr", "yrs", "years", "year", "kg", "cm")

_A = re.ASCII
_WORDS = re.compile(rf"\b({'|'.join(IMPORT_NUMERIC_ALLOWED_WORDS)})\b", _A | re.IGNORECASE)
_STRICT = re.compile(r"-?[0-9]+(\.[0-9]+)?", _A)
_TOKEN = re.compile(r"-?[0-9]+(?:\.[0-9]+)?", _A)
_TIME = re.compile(
    rf"(?:[01]?[0-9]|2[0-3]):[0-5][0-9](?::[0-5][0-9](?:\.[0-9]{{1,3}})?)?(?:[{js.S}]?[ap]m)?", _A | re.I
)
_TIME_RANGE = re.compile(
    rf"(?:[01]?[0-9]|2[0-3]):[0-5][0-9](?::[0-5][0-9](?:\.[0-9]{{1,3}})?)?[{js.S}]*-[{js.S}]*"
    rf"(?:[01]?[0-9]|2[0-3]):[0-5][0-9](?::[0-5][0-9](?:\.[0-9]{{1,3}})?)?(?:[{js.S}]?[ap]m)?",
    _A | re.I,
)
AMBIGUOUS = re.compile(rf"([0-9]+[{js.S}]*[-/][{js.S}]*[0-9]+)", _A)
_GROUPED = re.compile(
    r"-?(?:[0-9]{1,3}(?:,[0-9]{3})+|[0-9]{1,2}(?:,[0-9]{2})*,[0-9]{3}|[0-9]{1,3}(?: [0-9]{3})+)(?:\.[0-9]+)?", _A
)
_LIST_SEPARATOR = re.compile(rf"[,;|{js.S}]+")
_CURRENCY = re.compile(r"[₹$%]")
_CURRENCY_AND_COMMA = re.compile(r"[,₹$%]", re.I)
_LETTER = re.compile(r"[a-z]", _A | re.I)
_DIGIT = re.compile(r"[0-9]")
_SPACES = re.compile(rf"[{js.S}]+")

Kind = Literal["empty", "convertible", "ambiguous", "invalid_text", "invalid_numeric_format", "multi_value"]


@dataclass(frozen=True)
class Classification:
    kind: Kind
    raw: str = ""
    normalized_raw: str = ""
    has_digits: bool = False
    parsed_value: float | None = None
    suggested_values: list[float] = field(default_factory=list)
    is_decimal_like: bool = False


EMPTY = Classification("empty")


def is_strict(value: str) -> bool:
    return _STRICT.fullmatch(value) is not None


def strip_allowed_words(raw: str) -> str:
    return js.trim(_WORDS.sub("", raw))


def split_numeric_list(raw: str) -> list[str] | None:
    """The numbers of a cell that holds several ("36620, 71045"), or None for one."""
    stripped = js.trim(_CURRENCY.sub("", strip_allowed_words(raw)))
    if _GROUPED.fullmatch(stripped):
        return None
    parts = [part for part in _LIST_SEPARATOR.split(stripped) if part]
    return parts if len(parts) > 1 and all(is_strict(part) for part in parts) else None


def clean_candidate(raw: str, allow_ambiguous_chars: bool = False) -> str:
    cleaned = _WORDS.sub("", _CURRENCY_AND_COMMA.sub("", raw))
    keep = r"[^0-9./-]" if allow_ambiguous_chars else r"[^0-9.-]"
    return js.trim(re.sub(keep, "", cleaned))


def extract_suggestions(raw: str) -> list[float]:
    cleaned = _WORDS.sub(" ", _CURRENCY_AND_COMMA.sub("", raw))
    suggestions: list[float] = []
    seen: set[str] = set()
    for token in _TOKEN.findall(cleaned):
        if not is_strict(token):
            continue
        parsed = float(token)
        if not js.is_finite(parsed):
            continue
        key = js.number_to_string(parsed)
        if key in seen:
            continue
        seen.add(key)
        suggestions.append(parsed)
    return suggestions


def is_time_like(raw: object) -> bool:
    if not isinstance(raw, str):
        return False
    normalized = js.trim(raw).lower()
    if ":" not in normalized:
        return False
    return _TIME.fullmatch(normalized) is not None or _TIME_RANGE.fullmatch(normalized) is not None


def classify(value: str | float | bool | None) -> Classification:
    """classifyNumericImportValue — `value` is the resolved cell (dates already ISO text)."""
    if value is None:
        return EMPTY
    if isinstance(value, float) and not isinstance(value, bool):
        if not js.is_finite(value):
            return EMPTY
        text = js.number_to_string(value)
        return Classification(
            "convertible", text, text.lower(), True, value, [], is_decimal_like=not value.is_integer()
        )
    raw = js.trim(js.to_string(value))
    if raw == "":
        return EMPTY
    normalized = raw.lower()
    has_digits = _DIGIT.search(raw) is not None
    if is_time_like(raw):
        return Classification("invalid_text", raw, normalized, has_digits)
    if split_numeric_list(raw):
        return Classification("multi_value", raw, normalized, has_digits)

    has_letters = _LETTER.search(strip_allowed_words(raw)) is not None
    if AMBIGUOUS.search(raw):
        candidate = clean_candidate(raw, True)
        suggested = extract_suggestions(raw)
        if not suggested and is_strict(candidate):
            parsed = float(candidate)
            if js.is_finite(parsed):
                suggested.append(parsed)
        return Classification("ambiguous", raw, normalized, has_digits, None, suggested)
    if has_letters:
        return Classification("invalid_text", raw, normalized, has_digits, None, extract_suggestions(raw))
    if not has_digits:
        return Classification("invalid_text", raw, normalized, False)

    cleaned = clean_candidate(raw)
    if not is_strict(cleaned):
        return Classification("invalid_numeric_format", raw, normalized, has_digits, None, extract_suggestions(raw))
    parsed = float(cleaned)
    if not js.is_finite(parsed):
        return Classification("invalid_numeric_format", raw, normalized, has_digits, None, extract_suggestions(raw))
    changed = _SPACES.sub("", raw) != js.number_to_string(parsed)
    return Classification(
        "convertible", raw, normalized, has_digits, parsed, [parsed] if changed else [], is_decimal_like="." in cleaned
    )
