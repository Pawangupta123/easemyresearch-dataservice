"""A job's staged input — what Node wrote for an import too big for its operation document."""

import gzip
import json
from pathlib import Path

import pyarrow as pa
import pytest

from dataservice.common.errors import AppError
from dataservice.modules.writer.service import _rows
from dataservice.storage.staging import read_staged_json

SCHEMA = pa.schema([("_rid", pa.string()), ("f_age", pa.float64())])


def test_staged_json_is_read_gzipped_or_plain(tmp_path: Path) -> None:
    plain, packed = tmp_path / "rows.json", tmp_path / "rows.json.gz"
    plain.write_bytes(b'{"rows": []}')
    packed.write_bytes(gzip.compress(b'{"rows": [{"_rid": "r1"}]}'))
    assert read_staged_json(str(plain)) == {"rows": []}
    assert read_staged_json(str(packed)) == {"rows": [{"_rid": "r1"}]}


def test_append_rows_reads_its_rows_from_the_staged_source(tmp_path: Path) -> None:
    staged = tmp_path / "import.json.gz"
    staged.write_bytes(gzip.compress(json.dumps({"rows": [{"_rid": "r1", "f_age": 41.0}]}).encode()))
    table = _rows({"source": str(staged), "rows": 1}, SCHEMA)
    assert table.to_pylist() == [{"_rid": "r1", "f_age": 41.0}]


def test_a_staged_file_that_is_not_rows_is_a_wrong_payload(tmp_path: Path) -> None:
    staged = tmp_path / "import.json"
    staged.write_bytes(b"[1, 2]")
    with pytest.raises(AppError):
        _rows({"source": str(staged)}, SCHEMA)
    with pytest.raises(AppError):
        _rows({"source": 42}, SCHEMA)


def test_a_missing_staged_file_is_not_a_wrong_payload_it_may_appear_on_retry(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        _rows({"source": str(tmp_path / "gone.json")}, SCHEMA)
