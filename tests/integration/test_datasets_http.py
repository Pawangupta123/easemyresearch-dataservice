"""The query service end to end: HTTP, Node's token, the settings' lake root and Mongo, the patches
Node writes read back through the one reader — only those of the manifest's range (C13)."""

import time
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
import pytest
from deltalake import write_deltalake
from fastapi.testclient import TestClient
from pymongo.database import Database

from dataservice.common.utils import dataset_token
from dataservice.common.utils.dataset_token import DatasetClaims
from dataservice.config import Settings, get_settings
from dataservice.main import app
from dataservice.modules.datasets.service import table_uri
from dataservice.storage.snapshot import UTC_TIMESTAMP
from tests.conftest import SECRET
from tests.integration.conftest import TEST_MONGODB_URL, Document

DATASET = "6a1f00000000000000000011"
ROWS = [f"6a1f0000000000000000{index:04x}" for index in range(5)]
WRITTEN = datetime(2026, 10, 24, 12, 0, tzinfo=UTC)
MANIFEST = {
    "revision": 2,
    "tables": {"base": {"version": 0}},
    "patches": {"fromSeq": 0, "toSeq": 2},
    "fields": [{"fieldId": "age", "valueType": "num"}, {"fieldId": "seen", "valueType": "date"}],
    "rowCount": 6,
    "dataVersion": 2,
}


def _patch(seq: int, op: str, row: str, value: dict[str, object] | None = None, field: str | None = None) -> Document:
    return {
        "datasetId": DATASET,
        "seq": seq,
        "op": op,
        "target": {"row": row, "field": field, "scope": {"kind": "main", "followUp": 0}},
        "value": value,
        "opId": f"op:{seq}",
        "batchId": "batch-1",
        "actor": "6a1f000000000000000000e1",
        "at": WRITTEN,
    }


@pytest.fixture
def client(tmp_path: Path, mongo_db: Database[Document]) -> Iterator[TestClient]:
    write_deltalake(
        table_uri(str(tmp_path), DATASET, "table"),
        pa.table(
            {
                "_rid": ROWS,
                "_deleted_at": pa.array([None] * len(ROWS), UTC_TIMESTAMP),
                "f_age": [float(30 + index) for index in range(len(ROWS))],
            }
        ),
    )
    mongo_db["dataset_patches"].insert_many(
        [
            _patch(1, "set", ROWS[0], {"t": "num", "v": 31}, "age"),  # a BSON int: read as a number
            _patch(2, "insert_row", "6a1f0000000000000000ffff"),
            _patch(3, "set", ROWS[1], {"t": "date", "v": datetime(2026, 3, 9)}, "seen"),  # past toSeq
            {**_patch(1, "delete_row", ROWS[2]), "datasetId": "6a1f00000000000000000099"},  # another dataset
        ]
    )
    settings = Settings(
        service_token_secret=SECRET,
        storage_mode="local",
        lake_root=str(tmp_path),
        mongodb_url=TEST_MONGODB_URL,
        mongodb_db=mongo_db.name,
    )
    app.dependency_overrides[get_settings] = lambda: settings
    yield TestClient(app)
    app.dependency_overrides.clear()


def _post(client: TestClient, action: str, body: dict[str, object]) -> dict[str, object]:
    claims = DatasetClaims(user_id="u1", dataset_id=DATASET, scope="read", row_scope="all")
    headers = {"Authorization": f"Bearer {dataset_token.sign(SECRET, claims, int(time.time()))}"}
    response = client.post(f"/v1/datasets/{DATASET}/{action}", json=body, headers=headers)
    assert response.status_code == 200, response.json()
    data = response.json()["data"]
    assert isinstance(data, dict)
    return data


def test_a_page_reads_the_manifests_patches_from_mongo_and_nothing_else(client: TestClient) -> None:
    rows = _post(client, "rows:query", {"manifest": MANIFEST, "limit": 3})["rows"]
    assert isinstance(rows, list)
    assert [row["_rid"] for row in rows] == ["6a1f0000000000000000ffff", ROWS[4], ROWS[3]]
    page = _post(client, "rows:query", {"manifest": MANIFEST, "limit": 10, "rowOrder": "asc"})["rows"]
    assert isinstance(page, list)
    by_id = {row["_rid"]: row for row in page}
    assert by_id[ROWS[0]]["values"] == {"age": 31.0, "seen": None}
    assert by_id[ROWS[0]]["updatedAt"] == "2026-10-24T12:00:00Z"
    assert by_id[ROWS[1]]["values"]["seen"] is None  # seq 3 is past the manifest's toSeq
    assert ROWS[2] in by_id  # another dataset's delete never applies
    assert by_id["6a1f0000000000000000ffff"]["createdBy"] == "6a1f000000000000000000e1"


def test_count_and_cells_read_see_the_same_revision(client: TestClient) -> None:
    assert _post(client, "rows:count", {"manifest": MANIFEST}) == {"revision": 2, "count": 6}
    later = {**MANIFEST, "patches": {"fromSeq": 0, "toSeq": 3}}
    cells = _post(client, "cells:read", {"manifest": later, "targets": [{"row": ROWS[1], "field": "seen"}]})
    assert cells["cells"] == [{"target": {"row": ROWS[1], "field": "seen"}, "value": "2026-03-09"}]


def test_patches_node_writes_with_objectids_are_read(client: TestClient, mongo_db: Database[Document]) -> None:
    from bson import ObjectId

    mongo_db["dataset_patches"].insert_one(
        {
            **_patch(4, "set", ROWS[3], {"t": "num", "v": 99.5}, "age"),
            "datasetId": ObjectId(DATASET),  # as Node's Mongoose model writes it
            "actor": ObjectId("6a1f000000000000000000e1"),
        }
    )
    later = {**MANIFEST, "patches": {"fromSeq": 0, "toSeq": 4}}
    cells = _post(client, "cells:read", {"manifest": later, "targets": [{"row": ROWS[3], "field": "age"}]})
    assert cells["cells"] == [{"target": {"row": ROWS[3], "field": "age"}, "value": 99.5}]
