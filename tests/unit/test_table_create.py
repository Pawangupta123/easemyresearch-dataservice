"""A dataset's first table over HTTP: the rows of its first import become a fenced Delta table
the query endpoints read at once; a second create, a wrong value or a read token is refused."""

import time
from collections.abc import Iterator
from pathlib import Path

import pyarrow as pa
import pytest
from deltalake import DeltaTable
from fastapi.testclient import TestClient

from dataservice.common.utils import dataset_token
from dataservice.common.utils.dataset_token import DatasetClaims, Scope
from dataservice.main import app
from dataservice.modules.datasets.router import lake_root, patch_source
from dataservice.modules.datasets.service import table_uri
from dataservice.storage.fenced_commit import WRITER_APP_ID
from dataservice.storage.snapshot import PATCH_SCHEMA
from tests.conftest import SECRET

DATASET = "6a1f00000000000000000d01"
OWNER = "6a1f000000000000000000e1"
ROWS = ["6a1f000000000000000000a1", "6a1f000000000000000000a2"]
FIELDS = [
    {"fieldId": "age", "valueType": "num"},
    {"fieldId": "name", "valueType": "str"},
    {"fieldId": "seen", "valueType": "date"},
    {"fieldId": "sex", "valueType": "opts"},
]
VALUES: dict[str, list[object]] = {
    "age": [31, None],
    "name": ["Asha", None],
    "seen": ["2026-01-05", None],
    "sex": [["o1"], []],
}
ROW_COLUMNS: dict[str, object] = {
    "rid": ROWS,
    "createdBy": [OWNER, OWNER],
    "createdAt": ["2026-10-26T05:00:00Z", "2026-10-26T05:00:01Z"],
    "values": VALUES,
}
BODY: dict[str, object] = {"fields": FIELDS, "rows": ROW_COLUMNS}


class _NoPatches:
    def patches(self, dataset_id: str, from_seq: int, to_seq: int) -> pa.Table:
        return PATCH_SCHEMA.empty_table()


@pytest.fixture
def client(tmp_path: Path) -> Iterator[TestClient]:
    app.dependency_overrides[lake_root] = lambda: str(tmp_path)
    app.dependency_overrides[patch_source] = _NoPatches
    yield TestClient(app)
    app.dependency_overrides.clear()


def _headers(scope: Scope = "job") -> dict[str, str]:
    claims = DatasetClaims(user_id=OWNER, dataset_id=DATASET, scope=scope, row_scope="all")
    return {"Authorization": f"Bearer {dataset_token.sign(SECRET, claims, int(time.time()))}"}


def _create(client: TestClient, body: object, scope: Scope = "job") -> tuple[int, dict[str, object]]:
    response = client.post(f"/v1/datasets/{DATASET}/table:create", json=body, headers=_headers(scope))
    return response.status_code, response.json()


def test_the_first_import_becomes_a_fenced_table_the_grid_reads_at_once(client: TestClient, tmp_path: Path) -> None:
    status, payload = _create(client, BODY)
    assert status == 200, payload
    assert payload["data"] == {"version": 0, "rowCount": 2}
    table = DeltaTable(table_uri(str(tmp_path), DATASET, "table"))
    assert table.transaction_version(WRITER_APP_ID) == 1
    manifest = {
        "revision": 1,
        "tables": {"base": {"version": 0}},
        "patches": {"fromSeq": 0, "toSeq": 0},
        "fields": FIELDS,
        "rowCount": 2,
        "dataVersion": 1,
    }
    page = client.post(
        f"/v1/datasets/{DATASET}/rows:query", json={"manifest": manifest, "limit": 5}, headers=_headers("read")
    ).json()["data"]
    assert page["rows"] == [
        {
            "_rid": ROWS[1],
            "createdBy": OWNER,
            "createdAt": "2026-10-26T05:00:01Z",
            "updatedAt": "2026-10-26T05:00:01Z",
            "values": {"age": None, "name": None, "seen": None, "sex": []},
        },
        {
            "_rid": ROWS[0],
            "createdBy": OWNER,
            "createdAt": "2026-10-26T05:00:00Z",
            "updatedAt": "2026-10-26T05:00:00Z",
            "values": {"age": 31.0, "name": "Asha", "seen": "2026-01-05", "sex": ["o1"]},
        },
    ]


def test_a_second_create_never_overwrites_the_first(client: TestClient) -> None:
    assert _create(client, BODY)[0] == 200
    assert _create(client, BODY)[0] == 409


@pytest.mark.parametrize(
    ("change", "message"),
    [
        ({"values": {**VALUES, "age": ["old", None]}}, "Field age: every value must be a number"),
        ({"values": {**VALUES, "seen": ["5 Jan", None]}}, "Field seen"),
        ({"rid": [ROWS[0], ROWS[0]]}, "Row ids must be unique"),
        ({"createdBy": [OWNER]}, "one value per row"),
        ({"values": {"age": [1, 2]}}, "exactly the listed fields"),
    ],
)
def test_a_wrong_table_is_refused_before_anything_is_written(
    client: TestClient, tmp_path: Path, change: dict[str, object], message: str
) -> None:
    rows: dict[str, object] = {**ROW_COLUMNS, **change}
    status, payload = _create(client, {**BODY, "rows": rows})
    assert status == 400
    assert message in str(payload)
    assert not DeltaTable.is_deltatable(table_uri(str(tmp_path), DATASET, "table"))


def test_only_a_job_token_creates_a_table(client: TestClient) -> None:
    assert _create(client, BODY, scope="read")[0] == 403
