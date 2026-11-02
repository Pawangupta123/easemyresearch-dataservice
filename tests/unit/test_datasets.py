"""The query service (api.md §2.1) over HTTP: a pinned revision's page, count and an edit's
before-values, every value in its JSON form, and every request checked before any read."""

import time
from collections.abc import Iterator, Mapping
from datetime import UTC, date, datetime
from pathlib import Path

import pyarrow as pa
import pytest
from deltalake import write_deltalake
from fastapi.testclient import TestClient

from dataservice.common.utils import dataset_token
from dataservice.common.utils.dataset_token import DatasetClaims, Scope
from dataservice.main import app
from dataservice.modules.datasets.router import lake_root, patch_source
from dataservice.modules.datasets.schemas import ManifestDto
from dataservice.modules.datasets.service import snapshot_from_manifest
from dataservice.storage.layout import table_uri
from dataservice.storage.snapshot import PATCH_SCHEMA, FieldSpec, TableVersion
from tests.conftest import SECRET

DATASET = "6a1f00000000000000000001"
OTHER = "6a1f00000000000000000002"
UTC_US = pa.timestamp("us", tz="UTC")
MADE = datetime(2026, 9, 1, 8, 0, tzinfo=UTC)
LATE = datetime(2026, 10, 24, 12, 0, tzinfo=UTC)
BASE = pa.table(
    {
        "_rid": ["6a1f000000000000000000a1", "6a1f000000000000000000a2", "6a1f000000000000000000a3"],
        "_deleted_at": pa.array([None, None, datetime(2026, 10, 1, tzinfo=UTC)], UTC_US),
        "_created_by": ["u1", "u2", "u1"],
        "_created_at": pa.array([MADE] * 3, UTC_US),
        "_updated_at": pa.array([MADE] * 3, UTC_US),
        "f_age": [30.0, None, 55.0],
        "f_name": ["Asha", "Ravi", "Meena"],
        "f_visit": pa.array([date(2026, 1, 5), None, None], pa.date32()),
        "f_sym": pa.array([["o_fever"], [], None], pa.list_(pa.string())),
    }
)
A1, A2, A3 = (str(rid) for rid in BASE.column("_rid").to_pylist())
FIELDS = [
    {"fieldId": "age", "valueType": "num"},
    {"fieldId": "name", "valueType": "str"},
    {"fieldId": "visit", "valueType": "date"},
    {"fieldId": "sym", "valueType": "opts"},
]
INSERTED = "6a1f000000000000000000b9"
PATCHES = pa.Table.from_pylist(
    [
        {"seq": 1, "op": "set", "row": A1, "follow_up": 0, "field": "age", "t": "num", "v_num": 31.0, "at": LATE},
        {"seq": 2, "op": "insert_row", "row": INSERTED, "follow_up": 0, "at": LATE, "actor": "u2"},
        {"seq": 3, "op": "set", "row": A2, "follow_up": 0, "field": "name", "t": "str", "v_str": "Later", "at": LATE},
    ],
    schema=PATCH_SCHEMA,
)


def _manifest(to_seq: int = 2, **changes: object) -> dict[str, object]:
    manifest: dict[str, object] = {
        "revision": 3,
        "tables": {"base": {"version": 0}, "followups": None},
        "patches": {"fromSeq": 0, "toSeq": to_seq},
        "fields": FIELDS,
        "rowCount": 3,
        "dataVersion": 3,
    }
    return manifest | changes


class _Source:
    def patches(self, dataset_id: str, from_seq: int, to_seq: int) -> pa.Table:
        assert dataset_id == DATASET
        return PATCHES


@pytest.fixture
def client(tmp_path: Path) -> Iterator[TestClient]:
    write_deltalake(table_uri(str(tmp_path), DATASET, "table"), BASE)
    app.dependency_overrides[lake_root] = lambda: str(tmp_path)
    app.dependency_overrides[patch_source] = _Source
    yield TestClient(app)
    app.dependency_overrides.clear()


def _token(dataset_id: str = DATASET, scope: Scope = "read", row_scope: str = "all") -> dict[str, str]:
    user = "u1" if row_scope == "all" else row_scope  # a row scope is always the caller's own
    claims = DatasetClaims(user_id=user, dataset_id=dataset_id, scope=scope, row_scope=row_scope)
    return {"Authorization": f"Bearer {dataset_token.sign(SECRET, claims, int(time.time()))}"}


def _post(
    client: TestClient, action: str, body: Mapping[str, object], *, dataset_id: str = DATASET, row_scope: str = "all"
) -> tuple[int, dict[str, object]]:
    headers = _token(dataset_id, row_scope=row_scope)
    response = client.post(f"/v1/datasets/{DATASET}/{action}", json=body, headers=headers)
    return response.status_code, response.json()


def _data(client: TestClient, action: str, body: Mapping[str, object], *, row_scope: str = "all") -> dict[str, object]:
    status, payload = _post(client, action, body, row_scope=row_scope)
    assert status == 200, payload
    assert payload["success"] is True
    data = payload["data"]
    assert isinstance(data, dict)
    return data


def test_a_page_comes_newest_first_with_meta_and_every_value_as_json(client: TestClient) -> None:
    page = _data(client, "rows:query", {"manifest": _manifest(), "limit": 10})
    assert page["revision"] == 3
    rows = page["rows"]
    assert isinstance(rows, list)
    assert [row["_rid"] for row in rows] == [INSERTED, A2, A1]  # A3 trashed; seq 3 is past toSeq (C13)
    assert rows[2] == {
        "_rid": A1,
        "createdBy": "u1",
        "createdAt": "2026-09-01T08:00:00Z",
        "updatedAt": "2026-10-24T12:00:00Z",  # its age patch moved it
        "values": {"age": 31.0, "name": "Asha", "visit": "2026-01-05", "sym": ["o_fever"]},
    }
    assert rows[1]["values"] == {"age": None, "name": "Ravi", "visit": None, "sym": []}
    assert (rows[0]["createdBy"], rows[0]["values"]) == ("u2", {"age": None, "name": None, "visit": None, "sym": None})


def test_a_page_takes_only_its_columns_offset_and_order(client: TestClient) -> None:
    body = {"manifest": _manifest(3), "columns": ["name"], "limit": 1, "offset": 1, "rowOrder": "asc"}
    rows = _data(client, "rows:query", body)["rows"]
    assert rows == [
        {
            "_rid": A2,
            "createdBy": "u2",
            "createdAt": "2026-09-01T08:00:00Z",
            "updatedAt": "2026-10-24T12:00:00Z",
            "values": {"name": "Later"},
        }
    ]


def test_a_count_is_of_the_pinned_revision(client: TestClient) -> None:
    assert _data(client, "rows:count", {"manifest": _manifest()}) == {"revision": 3, "count": 3}
    assert _data(client, "rows:count", {"manifest": _manifest(0)})["count"] == 2  # before the insert


def test_cells_read_gives_before_values_and_row_states_in_target_order(client: TestClient) -> None:
    body = {
        "manifest": _manifest(),
        "targets": [
            {"row": A3, "field": "name"},
            {"row": A1, "field": "age"},
            {"row": "6a1f000000000000000000ff", "field": "age"},  # no such row
            {"row": A1, "field": "sym"},
        ],
    }
    read = _data(client, "cells:read", body)
    assert read["cells"] == [
        {"target": {"row": A3, "field": "name"}, "value": "Meena"},
        {"target": {"row": A1, "field": "age"}, "value": 31.0},
        {"target": {"row": A1, "field": "sym"}, "value": ["o_fever"]},
    ]
    assert read["rows"] == [
        {"_rid": A1, "state": "live", "createdBy": "u1"},
        {"_rid": A3, "state": "deleted", "createdBy": "u1"},
    ]


def test_a_row_scoped_caller_sees_only_its_own_rows(client: TestClient) -> None:
    body = {"manifest": _manifest(), "targets": [{"row": A1, "field": "age"}, {"row": A2, "field": "age"}]}
    read = _data(client, "cells:read", body, row_scope="u2")
    assert (read["cells"], read["rows"]) == (
        [{"target": {"row": A2, "field": "age"}, "value": None}],
        [{"_rid": A2, "state": "live", "createdBy": "u2"}],
    )
    for action, request in (
        ("rows:query", {"manifest": _manifest(), "limit": 5}),
        ("rows:count", {"manifest": _manifest()}),
    ):
        assert _post(client, action, request, row_scope="u2")[0] == 403


def test_another_datasets_token_or_no_token_is_refused(client: TestClient) -> None:
    assert _post(client, "rows:count", {"manifest": _manifest()}, dataset_id=OTHER)[0] == 403
    assert client.post(f"/v1/datasets/{DATASET}/rows:count", json={"manifest": _manifest()}).status_code == 401
    bad_id = client.post(
        "/v1/datasets/not-a-dataset/rows:count", json={"manifest": _manifest()}, headers=_token("not-a-dataset")
    )
    assert bad_id.status_code == 400


@pytest.mark.parametrize(
    ("body", "field"),
    [
        ({"manifest": _manifest(), "limit": 0}, "limit"),
        ({"manifest": _manifest(), "limit": 5001}, "limit"),
        ({"manifest": _manifest(), "limit": 5, "filter": {"all": []}}, "filter"),  # not honoured yet: refused
        ({"manifest": _manifest(patches={"fromSeq": 4, "toSeq": 2}), "limit": 5}, "manifest.patches"),
        ({"manifest": _manifest(fields=[*FIELDS, FIELDS[0]]), "limit": 5}, "manifest"),
        (
            {"manifest": _manifest(fields=[{"fieldId": "a b", "valueType": "num"}]), "limit": 5},
            "manifest.fields.0.fieldId",
        ),
        ({"manifest": _manifest(), "columns": ["nope"], "limit": 5}, None),  # unknown field: the reader's check
    ],
)
def test_a_bad_request_is_refused_before_any_read(
    client: TestClient, body: dict[str, object], field: str | None
) -> None:
    status, payload = _post(client, "rows:query", body)
    assert status == 400 and payload["success"] is False
    errors = payload.get("errors")
    if field is not None:
        assert isinstance(errors, list) and field in [error["field"] for error in errors]


def test_the_dataset_routes_refuse_to_serve_without_a_lake_or_a_mongo() -> None:
    plain = TestClient(app, raise_server_exceptions=False)
    status = plain.post(f"/v1/datasets/{DATASET}/rows:count", json={"manifest": _manifest()}, headers=_token())
    assert status.status_code == 500  # LAKE_ROOT / MONGODB_URL unset in the test settings


def test_a_manifest_names_only_this_datasets_own_tables() -> None:
    manifest = ManifestDto.model_validate(_manifest(tables={"base": {"version": 4}, "followups": {"version": 2}}))
    snapshot = snapshot_from_manifest("s3://lake/root/", DATASET, manifest)
    assert (snapshot.base_uri, snapshot.base_version) == (f"s3://lake/root/datasets/{DATASET}/table", 4)
    assert snapshot.followups == TableVersion(f"s3://lake/root/datasets/{DATASET}/followups", 2)
    assert (snapshot.patch_from, snapshot.patch_to, snapshot.fields[0]) == (0, 2, FieldSpec("age", "num"))
    with pytest.raises(ValueError, match="not a dataset id"):
        table_uri("s3://lake", "../other", "table")
