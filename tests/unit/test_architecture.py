"""Delta writes only through storage/fenced_commit.py (consistency.md §4.2).

A write call anywhere else would commit without the fencing txn — a blind append that a stale
writer could use to corrupt a dataset.
"""

import re
from pathlib import Path

SRC = Path(__file__).resolve().parents[2] / "src" / "dataservice"
GATE = SRC / "storage" / "fenced_commit.py"
# delta-rs write APIs. `update` / `delete` only with DeltaTable's own arguments, so a plain
# set.update(...) or dict.update(...) is not mistaken for a Delta write.
WRITE_CALL = re.compile(
    r"write_deltalake\(|create_write_transaction\(|\.merge\(|\.restore\(|\.vacuum\(|\.optimize\.|\.alter\."
    r"|\.delete\(\s*(predicate\b|\))|\.update\(\s*(updates|new_values|predicate)\b"
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


READER = SRC / "storage" / "reader.py"
# Reading Delta DATA (not just its log/metadata): only the one reader may, because only it
# applies the revision's patches — a raw read would show values the user already changed.
DATA_READ_CALL = re.compile(
    r"QueryBuilder\(|delta_scan|\.to_pyarrow|\.to_pandas\(|\.to_polars\(|scan_delta|read_delta|\.load_as_version\("
    r"|read_parquet|\.file_uris\("
)


def test_no_delta_data_read_outside_the_one_reader() -> None:
    offenders = [
        f"{path.relative_to(SRC)}:{number}"
        for path in SRC.rglob("*.py")
        if path != READER
        for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1)
        if DATA_READ_CALL.search(line)
    ]
    assert offenders == [], f"Delta data is read only through storage/reader.py: {offenders}"


TEST_ONLY_IMPORT = re.compile(r"^\s*(from|import)\s+(prototype|tests|scripts)\b")


def test_the_service_never_imports_prototype_or_test_code() -> None:
    # prototype/ is the reference Node logic for the Phase 0 tests, not part of the service.
    offenders = [
        f"{path.relative_to(SRC)}:{number}"
        for path in SRC.rglob("*.py")
        for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1)
        if TEST_ONLY_IMPORT.match(line)
    ]
    assert offenders == [], f"src/ must not import prototype/, tests/ or scripts/: {offenders}"
