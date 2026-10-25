"""A small dataset wired end to end for the hybrid-edit tests: a local Delta base, the reference
Node edit transaction and compaction publish on a real Mongo replica set, read back through the
one reader with Mongo patches."""

from collections.abc import Mapping
from pathlib import Path

import pyarrow as pa
from deltalake import DeltaTable, write_deltalake
from pymongo import MongoClient
from pymongo.database import Database

from dataservice.storage.compaction import compact
from dataservice.storage.fenced_commit import CommitContext, OpRef, max_token_seen
from dataservice.storage.lease import Lease
from dataservice.storage.mongo_patches import MongoPatchSource
from dataservice.storage.reader import read_rows
from dataservice.storage.snapshot import UTC_TIMESTAMP, FieldSpec, Snapshot
from prototype.node_side.compaction_store import MongoCompactionStore
from prototype.node_side.edit_store import Edit, MongoEditStore, Target
from prototype.node_side.lease_store import MongoLeaseStore
from prototype.node_side.quota_store import MongoQuotaStore

Document = dict[str, object]
DATASET, FORM = "ds-1", "form-1"
FIELDS = (FieldSpec("age", "num"), FieldSpec("pid", "str"))
ROWS = [f"r{index:02d}" for index in range(10)]
OWNER = "owner-1"
ENTRY_LIMIT = 1000  # roomy: quota tests set their own


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
        self.quota = MongoQuotaStore(client, db)
        db["payments"].insert_one(
            {"userId": OWNER, "entries_limit": ENTRY_LIMIT, "used_entries": len(ROWS), "reserved_entries": 0}
        )
        self.store = MongoEditStore(client, db, types, "pid", self.quota, self.row_states)
        self.patches = MongoPatchSource(db)
        self.store.create_dataset(DATASET, base_version=0, form_id=FORM, owner_id=OWNER)

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

    def row_states(self, revision: int, rows: list[str]) -> Mapping[str, str]:
        known = self.rows(revision, include_deleted=True)
        return {
            row: "absent" if row not in known else "deleted" if known[row]["_deleted_at"] is not None else "live"
            for row in rows
        }

    def quota_counters(self) -> tuple[int, int]:
        payment = self.db["payments"].find_one({"userId": OWNER})
        assert payment is not None
        return int(str(payment["used_entries"])), int(str(payment["reserved_entries"]))

    def before(self, revision: int, cells: list[Target]) -> Mapping[Target, object]:
        rows = self.rows(revision)
        return {(row, field): rows.get(row, {}).get(f"f_{field}") for row, field in cells}

    def in_base(self, revision: int, field: str, value: object) -> bool:
        return any(row.get(f"f_{field}") == value for row in self.rows(revision).values())

    def edit(self, key: str, *edits: Edit, actor: str = "user-a", batch: str | None = None) -> int:
        return self.store.apply(DATASET, actor, key, list(edits), self.before, self.in_base, batch_id=batch).revision


class Compactor:
    def __init__(self, lake: Lake, db: Database[Document]) -> None:
        self.lake = lake
        self.leases = MongoLeaseStore(db, ttl_ms=60_000)
        self.store = MongoCompactionStore(lake.client, db, self.leases)

    def lease(self) -> Lease:
        lease = self.leases.acquire(DATASET, "compactor", max_token_seen([DeltaTable(self.lake.base_uri)]))
        assert lease is not None
        return lease

    def prepare(self, base_revision: int, lease: Lease) -> tuple[int, int]:
        """Data Service side: fold the base revision's patches into a new base."""
        context = CommitContext(lease.token, (OpRef(f"compact-{base_revision}", 1),), ("base",))
        result = compact(self.lake.snapshot(base_revision), self.lake.patches, context)
        return result.base_version, result.folded_to

    def run(self) -> int:
        base_revision, lease = self.lake.current_revision(), self.lease()
        base_version, folded_to = self.prepare(base_revision, lease)
        return self.store.publish(DATASET, lease, base_revision, base_version, folded_to)
