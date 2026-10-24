"""Delta writes only through storage/fenced_commit.py (consistency.md §4.2).

A write call anywhere else would commit without the fencing txn — a blind append that a stale
writer could use to corrupt a dataset.
"""

import re
from pathlib import Path

SRC = Path(__file__).resolve().parents[2] / "src" / "dataservice"
GATE = SRC / "storage" / "fenced_commit.py"
WRITE_CALL = re.compile(
    r"write_deltalake\(|\.merge\(|\.restore\(|\.vacuum\(|\.optimize\.|\.alter\.|\.delete\(|\.update\("
)


def test_no_delta_write_call_outside_the_fenced_gate() -> None:
    offenders = [
        f"{path.relative_to(SRC)}:{number}"
        for path in SRC.rglob("*.py")
        if path != GATE
        for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1)
        if WRITE_CALL.search(line)
    ]
    assert offenders == [], f"Delta writes must go through storage/fenced_commit.py: {offenders}"
