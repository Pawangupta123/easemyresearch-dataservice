"""JavaScript's value semantics that Node's import rules depend on, so the port decides alike.

`String(number)`, `Number(text)`, `trim`, `\\s` and string lengths (UTF-16 code units) differ
between JavaScript and Python in ways the rules see: `String(18)` is "18" (Python "18.0"), `trim`
removes a BOM (Python's strip does not), `"Number(" 0x1F ")"` is 31, an emoji is 2 long. Pure.
"""

import math
import re
from decimal import Decimal

# JavaScript's WhiteSpace + LineTerminator — what `trim` removes and `\s` matches.
JS_SPACE = (  # written as escapes on purpose: most of these characters are invisible
    "\t\n\v\f\r \u00a0\u1680\u2000\u2001\u2002\u2003\u2004\u2005\u2006\u2007\u2008\u2009\u200a"
    "\u2028\u2029\u202f\u205f\u3000\ufeff"
)
# For regexes: JavaScript's `\s` as a character-class body.
S = re.escape(JS_SPACE)

_DECIMAL = re.compile(r"[+-]?(?:[0-9]+\.?[0-9]*|\.[0-9]+)(?:[eE][+-]?[0-9]+)?", re.ASCII)
_RADIX = {"0x": 16, "0X": 16, "0o": 8, "0O": 8, "0b": 2, "0B": 2}
_RADIX_DIGITS = {16: "0-9a-fA-F", 8: "0-7", 2: "01"}


def trim(value: str) -> str:
    return value.strip(JS_SPACE)


def trim_end(value: str) -> str:
    return value.rstrip(JS_SPACE)


def to_number(text: str) -> float:
    """JavaScript's `Number(text)` for a string."""
    value = trim(text)
    if not value:
        return 0.0
    if value in ("Infinity", "+Infinity"):
        return math.inf
    if value == "-Infinity":
        return -math.inf
    radix = _RADIX.get(value[:2])
    if radix is not None:
        digits = value[2:]
        if digits and re.fullmatch(f"[{_RADIX_DIGITS[radix]}]+", digits):
            return float(int(digits, radix))
        return math.nan
    if _DECIMAL.fullmatch(value):
        return float(value)
    return math.nan


def is_finite(value: float) -> bool:
    return math.isfinite(value)


def number_to_string(value: float) -> str:
    """JavaScript's `String(number)` (Number::toString, radix 10)."""
    if math.isnan(value):
        return "NaN"
    if math.isinf(value):
        return "Infinity" if value > 0 else "-Infinity"
    if value == 0:
        return "0"
    if value < 0:
        return "-" + number_to_string(-value)
    # The shortest round-trip digits (what both languages use), placed by JavaScript's rules.
    sign, digit_tuple, exponent = Decimal(repr(value)).normalize().as_tuple()
    del sign
    if not isinstance(exponent, int):
        raise ValueError(f"not a finite number: {value!r}")
    digits = "".join(str(d) for d in digit_tuple)
    k = len(digits)
    n = exponent + k  # the decimal point sits after n digits
    if k <= n <= 21:
        return digits + "0" * (n - k)
    if 0 < n <= 21:
        return f"{digits[:n]}.{digits[n:]}"
    if -6 < n <= 0:
        return "0." + "0" * (-n) + digits
    mantissa = digits if k == 1 else f"{digits[0]}.{digits[1:]}"
    exp = n - 1
    return f"{mantissa}e{'+' if exp >= 0 else '-'}{abs(exp)}"


def to_string(value: object) -> str:
    """JavaScript's `String(value)` for the values a cell can hold."""
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int | float):
        return number_to_string(float(value))
    return str(value)


# ── UTF-16 code units (JavaScript string length and indexing) ─────────────────────────────────


def units(value: str) -> list[int]:
    """The string as JavaScript sees it: its UTF-16 code units."""
    raw = value.encode("utf-16-le", "surrogatepass")
    return [int.from_bytes(raw[i : i + 2], "little") for i in range(0, len(raw), 2)]


def length(value: str) -> int:
    return len(value.encode("utf-16-le", "surrogatepass")) // 2


def slice_units(value: str, end: int) -> str:
    """`value.slice(0, end)` in code units (a split surrogate pair stays a lone surrogate)."""
    raw = value.encode("utf-16-le", "surrogatepass")[: max(end, 0) * 2]
    return raw.decode("utf-16-le", "surrogatepass")
