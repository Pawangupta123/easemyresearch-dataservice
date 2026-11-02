"""The `ingest_upload` job end to end on a local lake: the first table created empty (as at
confirm), the upload's rows cast with Node's rules and appended, read back through the one reader
— the same rows Node's own final import makes (the golden vectors)."""

import json
from collections.abc import Mapping
from datetime import date
from pathlib import Path

import pyarrow as pa
import pytest

from dataservice.common.errors import AppError
from dataservice.modules.datasets.schemas import TableCreateRequest
from dataservice.modules.datasets.table_create import create_table
from dataservice.modules.ingest import job as ingest
from dataservice.storage.fenced_commit import CommitContext, OpRef
from dataservice.storage.layout import table_uri
from dataservice.storage.operations import WriterTables
from dataservice.storage.reader import read_rows
from dataservice.storage.snapshot import PATCH_SCHEMA, FieldSpec, Snapshot

FIXTURES = Path(__file__).resolve().parent.parent / "fixtures" / "import_golden"
DATASET = "6a1f00000000000000000d02"


class _NoPatches:
    def patches(self, dataset_id: str, from_seq: int, to_seq: int) -> pa.Table:
        return PATCH_SCHEMA.empty_table()


def _golden(name: str) -> dict[str, object]:
    ingest_part: dict[str, object] = json.loads((FIXTURES / f"{name}.golden.json").read_text(encoding="utf-8"))[
        "ingest"
    ]
    return ingest_part


def _plain(value: object) -> object:
    return value.isoformat() if isinstance(value, date) else value


@pytest.fixture(autouse=True)
def _local_uploads(monkeypatch: pytest.MonkeyPatch) -> None:
    """Uploads are local files here (the bucket check is tested in test_ingest_detect.py)."""
    monkeypatch.setattr(ingest, "check_source", lambda source, bucket: None)


@pytest.mark.parametrize("name", ["categories", "dates", "numbers"])
def test_an_upload_is_appended_as_nodes_final_import_writes_it(tmp_path: Path, name: str) -> None:
    golden = _golden(name)
    spec: dict[str, object] = golden["spec"]  # type: ignore[assignment]
    fields: list[Mapping[str, str]] = spec["fields"]  # type: ignore[assignment]
    value_types = [{"fieldId": field["fieldId"], "valueType": field["valueType"]} for field in fields]
    empty = {"rid": [], "createdBy": [], "createdAt": [], "values": {f["fieldId"]: [] for f in fields}}
    created = create_table(
        str(tmp_path), DATASET, TableCreateRequest.model_validate({"fields": value_types, "rows": empty}), 60
    )
    uri = table_uri(str(tmp_path), DATASET, "table")

    payload = {**spec, "source": str(FIXTURES / f"{name}.xlsx")}
    context = CommitContext(2, (OpRef("op-ingest", 1),), ("base",))
    result = ingest.ingest_upload(payload, WriterTables({"base": uri}), context)

    rows: list[dict[str, object]] = golden["rows"]  # type: ignore[assignment]
    assert result.rows_added == len(rows)
    assert set(result.changed_field_ids) == {field["fieldId"] for field in fields}
    snapshot = Snapshot(
        DATASET,
        uri,
        created.version + 1,
        0,
        0,
        tuple(FieldSpec(field["fieldId"], field["valueType"]) for field in fields),  # type: ignore[arg-type]
    )
    read = {row.pop("_rid"): row for row in read_rows(snapshot, _NoPatches()).to_pylist()}
    for row in rows:
        stored = read[str(row["_rid"])]
        for key, value in row.items():
            if key.startswith("f_"):
                assert _plain(stored[key]) == value, f"{name} {row['_rid']} {key}"


def test_a_payload_naming_a_field_the_table_lacks_is_not_retried(tmp_path: Path) -> None:
    golden = _golden("labels")
    spec: dict[str, object] = golden["spec"]  # type: ignore[assignment]
    fields: list[Mapping[str, str]] = spec["fields"]  # type: ignore[assignment]
    first = fields[0]
    empty = {"rid": [], "createdBy": [], "createdAt": [], "values": {first["fieldId"]: []}}
    request = {"fields": [{"fieldId": first["fieldId"], "valueType": first["valueType"]}], "rows": empty}
    create_table(str(tmp_path), DATASET, TableCreateRequest.model_validate(request), 60)
    uri = table_uri(str(tmp_path), DATASET, "table")
    payload = {**spec, "source": str(FIXTURES / "labels.xlsx")}
    context = CommitContext(2, (OpRef("op-ingest", 1),), ("base",))
    with pytest.raises(AppError, match="no column"):
        ingest.ingest_upload(payload, WriterTables({"base": uri}), context)
    with pytest.raises(AppError, match="not valid"):
        ingest.ingest_upload({"source": "x"}, WriterTables({"base": uri}), context)
