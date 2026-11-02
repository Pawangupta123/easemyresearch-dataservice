"""Export the Node ↔ Data Service contract: the OpenAPI document of the internal API and one real
request / response pair per query endpoint, produced by the service itself.

    uv run python -m scripts.export_contract [--out contracts]

Node keeps a copy of `contracts/` (backend tests/fixtures/dataservice/) and parses every fixture
with its Zod schemas; tests/unit/test_contract.py fails when this repo's copy no longer matches
what the service answers — so a changed body shows up in both repos' tests.
"""

import argparse
import json
import tempfile
import time
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, date, datetime
from pathlib import Path

import pyarrow as pa
from deltalake import write_deltalake
from fastapi.testclient import TestClient

from dataservice.common.utils import dataset_token
from dataservice.common.utils.dataset_token import DatasetClaims
from dataservice.config import get_settings
from dataservice.main import app
from dataservice.modules.datasets.router import lake_root, patch_source
from dataservice.storage.layout import table_uri
from dataservice.storage.snapshot import PATCH_SCHEMA, UTC_TIMESTAMP

DATASET = "6a1f00000000000000000c01"
OWNER, EDITOR = "6a1f000000000000000000e1", "6a1f000000000000000000e2"
ROWS = ["6a1f000000000000000000a1", "6a1f000000000000000000a2", "6a1f000000000000000000a3"]
INSERTED = "6a1f000000000000000000b1"
CREATED = datetime(2026, 9, 1, 8, 0, tzinfo=UTC)
EDITED = datetime(2026, 10, 24, 12, 0, tzinfo=UTC)
FIELDS = {"age": "num", "name": "str", "seen": "date", "symptoms": "opts"}
MANIFEST = {
    "revision": 3,
    "tables": {"base": {"version": 0}, "followups": None},
    "patches": {"fromSeq": 0, "toSeq": 2},
    "fields": [{"fieldId": field_id, "valueType": value_type} for field_id, value_type in FIELDS.items()],
    "visitFields": [],
    "rowCount": 3,
    "dataVersion": 3,
}
REQUESTS: dict[str, dict[str, object]] = {
    "rows-query": {"manifest": MANIFEST, "columns": None, "offset": 0, "limit": 50, "rowOrder": "desc"},
    "rows-count": {"manifest": MANIFEST},
    "cells-read": {
        "manifest": MANIFEST,
        "targets": [{"row": ROWS[0], "field": "age"}, {"row": ROWS[2], "field": "symptoms"}],
    },
}
ACTIONS = {"rows-query": "rows:query", "rows-count": "rows:count", "cells-read": "cells:read"}


def _base() -> pa.Table:
    return pa.table(
        {
            "_rid": ROWS,
            "_created_by": [OWNER, EDITOR, OWNER],
            "_created_at": pa.array([CREATED] * 3, UTC_TIMESTAMP),
            "_updated_at": pa.array([CREATED] * 3, UTC_TIMESTAMP),
            "_deleted_at": pa.array([None, None, datetime(2026, 10, 1, tzinfo=UTC)], UTC_TIMESTAMP),
            "f_age": [30.0, None, 55.0],
            "f_name": ["Asha", "Ravi", None],
            "f_seen": pa.array([date(2026, 1, 5), None, date(2026, 2, 1)], pa.date32()),
            "f_symptoms": pa.array([["6a1f0000000000000000f001"], [], None], pa.list_(pa.string())),
        }
    )


class _Patches:
    """What Node's edit transaction wrote: an age edit and an inserted row (a later patch is
    past the manifest's range, so the fixtures also show a pinned revision)."""

    def patches(self, dataset_id: str, from_seq: int, to_seq: int) -> pa.Table:
        rows = [
            {"seq": 1, "op": "set", "row": ROWS[0], "follow_up": 0, "field": "age", "t": "num", "v_num": 31.0},
            {"seq": 2, "op": "insert_row", "row": INSERTED, "follow_up": 0, "actor": EDITOR},
            {"seq": 3, "op": "set", "row": ROWS[1], "follow_up": 0, "field": "name", "t": "str", "v_str": "Later"},
        ]
        return pa.Table.from_pylist([row | {"at": EDITED} for row in rows], schema=PATCH_SCHEMA)


@contextmanager
def _service() -> Iterator[TestClient]:
    with tempfile.TemporaryDirectory() as lake:
        write_deltalake(table_uri(lake, DATASET, "table"), _base())
        app.dependency_overrides[lake_root] = lambda: lake
        app.dependency_overrides[patch_source] = _Patches
        try:
            yield TestClient(app)
        finally:
            app.dependency_overrides.clear()


def build() -> dict[str, object]:
    """Every contract file by its path under contracts/, as the service answers today."""
    claims = DatasetClaims(user_id=OWNER, dataset_id=DATASET, scope="read", row_scope="all")
    token = dataset_token.sign(get_settings().service_token_secret, claims, int(time.time()))
    files: dict[str, object] = {"dataservice-internal.openapi.json": app.openapi()}
    with _service() as client:
        for name, body in REQUESTS.items():
            response = client.post(
                f"/v1/datasets/{DATASET}/{ACTIONS[name]}", json=body, headers={"Authorization": f"Bearer {token}"}
            )
            if response.status_code != 200:
                raise RuntimeError(f"{name}: {response.status_code} {response.text}")
            files[f"fixtures/{name}.request.json"] = body
            files[f"fixtures/{name}.response.json"] = response.json()
    return files


def render(value: object) -> str:
    return json.dumps(value, indent=2, sort_keys=True, ensure_ascii=False) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--out", default="contracts")
    out = Path(parser.parse_args().out)
    for path, content in build().items():
        target = out / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(render(content).encode("utf-8"))
        print(f"wrote {target}")


if __name__ == "__main__":
    main()
