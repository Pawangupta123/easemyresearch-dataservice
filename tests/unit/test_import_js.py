"""JavaScript's value semantics the import rules rely on (what Node's own engine gives)."""

import math

import pytest

from dataservice.modules.ingest.detect import js


@pytest.mark.parametrize(
    ("value", "text"),
    [
        (18.0, "18"),
        (-0.0, "0"),
        (0.1 + 0.2, "0.30000000000000004"),
        (123.456, "123.456"),
        (1e20, "100000000000000000000"),
        (1e21, "1e+21"),
        (1.5e-7, "1.5e-7"),
        (0.000001, "0.000001"),
        (-42.5, "-42.5"),
        (math.inf, "Infinity"),
        (math.nan, "NaN"),
    ],
)
def test_number_to_string_is_javascripts(value: float, text: str) -> None:
    assert js.number_to_string(value) == text


@pytest.mark.parametrize(
    ("text", "value"),
    [
        ("", 0.0),
        ("  42  ", 42.0),
        ("0x1F", 31.0),
        ("0b11", 3.0),
        ("1e3", 1000.0),
        (".5", 0.5),
        ("5.", 5.0),
        ("+7", 7.0),
        ("Infinity", math.inf),
    ],
)
def test_to_number_is_javascripts(text: str, value: float) -> None:
    assert js.to_number(text) == value


@pytest.mark.parametrize("text", ["1_000", "12abc", "0x", "--1", "1e", "-0x1F"])
def test_to_number_refuses_what_javascript_refuses(text: str) -> None:
    assert math.isnan(js.to_number(text))


def test_trim_removes_what_javascript_removes() -> None:
    assert js.trim("﻿  text　 ") == "text"
    assert js.trim("\x1ftext\x85") == "\x1ftext\x85"  # Python's strip would remove these


def test_lengths_and_slices_are_in_utf16_code_units() -> None:
    assert js.length("😀a") == 3
    assert js.slice_units("ab😀", 3).encode("utf-16-le", "surrogatepass") == "ab".encode("utf-16-le") + b"=\xd8"
    assert js.to_string(True) == "true"
    assert js.to_string(2.0) == "2"
