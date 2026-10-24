"""A small dataset wired end to end for the hybrid-edit tests: a local Delta base, the reference
Node edit transaction and compaction publish on a real Mongo replica set, read back through the
one reader with Mongo patches."""

from collections.abc import Mapping
from pathlib import Path

import pyarrow as pa
from deltalake import write_deltalake
from pymongo import MongoClient
from pymongo.database import Database

from dataservice.storage.mongo_patches import MongoPatchSource
from dataservice.storage.reader import read_rows
from dataservice.storage.snapshot import UTC_TIMESTAMP, FieldSpec, Snapshot
from prototype.node_side.edit_store import Edit, MongoEditStore, Target

Document = dict[str, object]
DATASET, FORM = "ds-1", "form-1"
FIELDS = (FieldSpec("age", "num"), FieldSpec("pid", "str"))
ROWS = [f"r{index:02d}" for index in range(10)]


def write_base(path: Path) -> str:
    uri = str(path / "base")
    write_deltalake(
        uri,
        pa.table(
            {
                "_rid": ROWS,
                "_deleted_at": pa.array([None] * len(ROWS), UTC_TIMESTAMP),
                "f_age": [float(30 + index) for index in range(len(ROWS))],
                "f_pid": [f"P-{index}" for index in range(len(ROWS))],
            }
        ),
    )
    return uri


def set_age(row: str, age: float) -> Edit:
    return Edit(row=row, op="set", field="age", t="num", v=age)


class Lake:
    def __init__(self, client: MongoClient[Document], db: Database[Document], base_uri: str) -> None:
        self.client, self.db, self.base_uri = client, db, base_uri
        types = {spec.field_id: spec.value_type for spec in FIELDS}
        self.store = MongoEditStore(client, db, types, unique_field="pid")
        self.patches = MongoPatchSource(db)
        self.store.create_dataset(DATASET, base_version=0, form_id=FORM)

    def current_revision(self) -> int:
        dataset = self.db["datasets"].find_one({"_id": DATASET})
        assert dataset is not None
        return int(str(dataset["currentRevision"]))

    def manifest(self, revision: int) -> Document:
        manifest = self.db["dataset_revisions"].find_one({"datasetId": DATASET, "revision": revision})
        assert manifest is not None
        return manifest

    def snapshot(self, revision: int | None = None) -> Snapshot:
        manifest = self.manifest(self.current_revision() if revision is None else revision)
        tables, patches = manifest["tables"], manifest["patches"]
        assert isinstance(tables, Mapping) and isinstance(patches, Mapping)
        base = tables["base"]
        assert isinstance(base, Mapping)
        return Snapshot(
            DATASET, self.base_uri, int(base["version"]), int(patches["fromSeq"]), int(patches["toSeq"]), FIELDS
        )

    def rows(self, revision: int | None = None, *, include_deleted: bool = False) -> dict[str, dict[str, object]]:
        table = read_rows(self.snapshot(revision), self.patches, include_deleted=include_deleted)
        return {row.pop("_rid"): row for row in table.to_pylist()}

    def before(self, revision: int, cells: list[Target]) -> Mapping[Target, object]:
        rows = self.rows(revision)
        return {(row, field): rows.get(row, {}).get(f"f_{field}") for row, field in cells}

    def in_base(self, revision: int, field: str, value: object) -> bool:
        return any(row.get(f"f_{field}") == value for row in self.rows(revision).values())

    def edit(self, key: str, *edits: Edit, actor: str = "user-a") -> int:
        return self.store.apply(DATASET, actor, key, list(edits), self.before, self.in_base).revision
