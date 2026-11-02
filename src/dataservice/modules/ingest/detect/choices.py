"""Is a column a choice (radio), with which options and typo fixes? Node's import-type-mcq.helper.ts,
import-options-canonical.helper.ts and common/utils/field-conversion.util.ts, rule for rule."""

from dataclasses import dataclass, field
from typing import Literal

from dataservice.modules.ingest.detect import js

AUTO_CREATED_MCQ_TYPE: Literal["radio"] = "radio"
SELECT_MIN_UNIQUE, SELECT_MAX_UNIQUE, SELECT_MAX_UNIQUE_RATIO = 1, 20, 0.5
NUMBER_TO_RADIO_MAX_UNIQUE = 5

# Canonical order of common medical / survey answers; the first definition covering every
# option wins (longer definitions before their subsets).
ORDINAL_CANONICAL: tuple[tuple[str, ...], ...] = (
    ("yes", "no"),
    ("positive", "negative"),
    ("reactive", "non-reactive"),
    ("present", "absent"),
    ("normal", "abnormal"),
    ("pass", "fail"),
    ("male", "female"),
    ("true", "false"),
    ("positive", "negative", "indeterminate"),
    ("positive", "negative", "equivocal"),
    ("positive", "negative", "borderline"),
    ("improved", "stable", "worsened"),
    ("improved", "stable", "worse"),
    ("below normal", "normal", "above normal"),
    ("low", "normal", "high"),
    ("low", "medium", "high"),
    ("none", "mild", "moderate", "severe", "extreme"),
    ("minimal", "mild", "moderate", "severe", "extreme"),
    ("none", "mild", "moderate", "severe"),
    ("minimal", "mild", "moderate", "severe"),
    ("never", "rarely", "sometimes", "often", "always"),
    ("never", "occasionally", "sometimes", "often", "always"),
    ("daily", "weekly", "monthly", "yearly"),
    ("strongly disagree", "disagree", "neutral", "agree", "strongly agree"),
    ("very dissatisfied", "dissatisfied", "neutral", "satisfied", "very satisfied"),
    ("strongly disagree", "disagree", "agree", "strongly agree"),
    ("grade i", "grade ii", "grade iii", "grade iv"),
    ("grade 1", "grade 2", "grade 3", "grade 4"),
    ("stage i", "stage ii", "stage iii", "stage iv"),
    ("stage 1", "stage 2", "stage 3", "stage 4"),
    ("nyha i", "nyha ii", "nyha iii", "nyha iv"),
    ("class i", "class ii", "class iii", "class iv"),
    ("who grade 0", "who grade 1", "who grade 2", "who grade 3", "who grade 4"),
    ("morning", "afternoon", "evening", "night"),
    ("o", "a", "b", "ab"),
    ("never", "former", "current"),
    ("never smoker", "former smoker", "current smoker"),
    ("never", "occasional", "regular", "daily"),
    ("non-drinker", "occasional", "regular", "heavy"),
    ("single", "married", "divorced", "widowed"),
    ("unmarried", "married", "divorced", "widowed"),
    ("type 1", "type 2", "gestational"),
    ("type 1 diabetes", "type 2 diabetes", "gestational diabetes"),
)


def is_select_candidate(unique_count: float, non_empty_count: int, max_unique: int = SELECT_MAX_UNIQUE) -> bool:
    """evaluateSelectCandidate: few distinct values, each repeated (unique_count may be ∞: capped)."""
    non_empty = non_empty_count if non_empty_count > 0 else 0
    unique = unique_count if unique_count > 0 else 0
    if not non_empty or unique < SELECT_MIN_UNIQUE or unique > max_unique:
        return False
    return unique / non_empty <= SELECT_MAX_UNIQUE_RATIO


def normalize_option_order(options: list[str]) -> list[str]:
    if len(options) < 2:
        return options
    present = {js.trim(option).lower() for option in options}
    for canonical in ORDINAL_CANONICAL:
        if present <= set(canonical):
            position = {label: index for index, label in enumerate(canonical)}
            return sorted(options, key=lambda option: position.get(js.trim(option).lower(), 999))
    numbers = [js.to_number(js.trim(option)) for option in options]
    if all(js.is_finite(number) for number in numbers):
        return sorted(options, key=lambda option: js.to_number(js.trim(option)))
    return options


def _edit_distance(a: list[int], b: list[int]) -> int:
    previous = list(range(len(b) + 1))
    for i in range(1, len(a) + 1):
        current = [i] + [0] * len(b)
        for j in range(1, len(b) + 1):
            cost = 0 if a[i - 1] == b[j - 1] else 1
            current[j] = min(previous[j] + 1, current[j - 1] + 1, previous[j - 1] + cost)
        previous = current
    return previous[len(b)]


def _similarity(a: str, b: str) -> float:
    ua, ub = js.units(a), js.units(b)
    longest = max(len(ua), len(ub))
    return 1.0 if longest == 0 else 1 - _edit_distance(ua, ub) / longest


@dataclass(frozen=True)
class Suggestion:
    value: str
    suggested: list[str]
    confidence: Literal["HIGH", "MEDIUM"]


@dataclass
class _Group:
    total: int = 0
    variants: dict[str, int] = field(default_factory=dict)


@dataclass(frozen=True)
class McqSuggestions:
    options: list[str]
    corrections: list[Suggestion]


def mcq_suggestions(raw_counts: dict[str, int]) -> McqSuggestions:
    """generateMcqSuggestions: options by case-insensitive value (the most frequent spelling), and
    rare values close to a main one suggested as that one."""
    grouped: dict[str, _Group] = {}
    for raw, count in raw_counts.items():
        display = js.trim(raw)
        if not display:
            continue
        group = grouped.setdefault(display.lower(), _Group())
        group.total += count
        group.variants[display] = group.variants.get(display, 0) + count

    entries: list[tuple[str, str, int]] = []  # (display, norm, count)
    for norm, group in grouped.items():
        canonical, best = "", -1
        for variant, count in group.variants.items():
            if count > best:
                canonical, best = variant, count
        entries.append((canonical, norm, group.total))

    total = sum(count for _, _, count in entries) or 1
    display_by_norm = {norm: display for display, norm, _ in entries}
    main_threshold = max(0.02 * total, 2)
    mains = [norm for _, norm, count in entries if count >= main_threshold]
    threshold = 0.6 if len(mains) <= 3 else 0.75

    corrections: list[Suggestion] = []
    for display, norm, _ in entries:
        if norm in mains:
            continue
        best_match, best_score = None, 0.0
        for category in mains:
            score = _similarity(norm, category)
            if score > best_score:
                best_score, best_match = score, category
        if best_match is not None and best_score >= threshold:
            suggested = display_by_norm.get(best_match)
            if suggested and suggested.lower() == display.lower():
                continue
            corrections.append(
                Suggestion(display, [suggested] if suggested else [], "HIGH" if best_score >= 0.8 else "MEDIUM")
            )
    return McqSuggestions(normalize_option_order([display for display, _, _ in entries]), corrections)
