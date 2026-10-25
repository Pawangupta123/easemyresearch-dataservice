"""REFERENCE implementation of Node's side of compaction (consistency.md §15.2–§15.3).

Prototype only — Node ports it to TypeScript. Two steps:

- `publish`: in one transaction, guard the writer's lease (write the lease row), refuse if any
  revision since the compaction's base revision was not a small edit (a base rewrite or schema
  change landed — the job must re-prepare), and insert revision n+1: the new base version,
  `patches = { fromSeq: W, toSeq: n.toSeq }` (edits made during the compaction stay on top), and
  n's `dataVersion` — values did not change, so nothing is marked stale and `Form.dataVersion`
  stays. Unique-identifier claims with `seq ≤ W` go: those values are in the base now.
- `purge_patches`: delete patches no retained revision reads — at or below the lowest `fromSeq`
  among the current revision and every pinned one.
"""

from collections.abc import Mapping
from datetime import UTC, datetime

from pymongo import MongoClient
from pymongo.client_session import ClientSession
from pymongo.database import Database

from dataservice.storage.lease import Lease
from prototype.node_side.documents import int_at as _int
from prototype.node_side.errors import EditRejected
from prototype.node_side.lease_store import MongoLeaseStore

Document = dict[str, object]


class MongoCompactionStore:
    def __init__(self, client: MongoClient[Document], database: Database[Document], leases: MongoLeaseStore) -> None:
        self.client, self.db, self.leases = client, database, leases

    def publish(self, dataset_id: str, lease: Lease, base_revision: int, base_version: int, folded_to: int) -> int:
        def transaction(session: ClientSession) -> int:
            return self._publish(session, dataset_id, lease, base_revision, base_version, folded_to)

        with self.client.start_session() as session:
            return session.with_transaction(transaction)

    def _publish(
        self,
        session: ClientSession,
        dataset_id: str,
        lease: Lease,
        base_revision: int,
        base_version: int,
        folded_to: int,
    ) -> int:
        db = self.db
        if not self.leases.guard_publish(session, lease, f"compact:{dataset_id}:{base_revision}"):
            raise EditRejected("FENCED", "the lease was lost before publishing")
        dataset = db["datasets"].find_one({"_id": dataset_id}, session=session)
        if dataset is None:
            raise EditRejected("NOT_FOUND", "dataset not found")
        current = _int(dataset, "currentRevision")

        base = db["dataset_revisions"].find_one({"datasetId": dataset_id, "revision": base_revision}, session=session)
        if base is None or _int(base, "patches", "toSeq") != folded_to:
            raise EditRejected("CONFLICT", "the compaction did not fold its base revision's patches")
        since = db["dataset_revisions"].count_documents(
            {"datasetId": dataset_id, "revision": {"$gt": base_revision}, "kind": {"$ne": "edit"}}, session=session
        )
        if since:
            raise EditRejected("CONFLICT", "a base rewrite or schema change landed since; re-prepare")

        moved = db["datasets"].update_one(
            {"_id": dataset_id, "currentRevision": current}, {"$set": {"currentRevision": current + 1}}, session=session
        )
        if moved.matched_count != 1:
            raise EditRejected("CONFLICT", "dataset moved; retry")
        latest = db["dataset_revisions"].find_one({"datasetId": dataset_id, "revision": current}, session=session)
        latest_tables = None if latest is None else latest.get("tables")
        if latest is None or not isinstance(latest_tables, Mapping):
            raise ValueError("revision manifest missing")
        tables: Document = {str(name): version for name, version in latest_tables.items()}
        tables["base"] = {"version": base_version}
        db["dataset_revisions"].insert_one(
            {
                "datasetId": dataset_id,
                "revision": current + 1,
                "tables": tables,
                "patches": {"fromSeq": folded_to, "toSeq": _int(latest, "patches", "toSeq")},
                "dataVersion": _int(latest, "dataVersion"),  # values unchanged (C39)
                "kind": "compact",
                "writerToken": lease.token,
                "publishedAt": datetime.now(UTC),
            },
            session=session,
        )
        db["dataset_unique_claims"].delete_many({"datasetId": dataset_id, "seq": {"$lte": folded_to}}, session=session)
        return current + 1

    def pin(self, dataset_id: str, revision: int, until: datetime) -> None:
        """A job or a pagination session holds revision `revision` until `until`."""
        self.db["dataset_revisions"].update_one(
            {"datasetId": dataset_id, "revision": revision}, {"$set": {"pinnedUntil": until}}
        )

    def purge_patches(self, dataset_id: str, now: datetime) -> int:
        dataset = self.db["datasets"].find_one({"_id": dataset_id})
        if dataset is None:
            raise EditRejected("NOT_FOUND", "dataset not found")
        retained = self.db["dataset_revisions"].find(
            {
                "datasetId": dataset_id,
                "$or": [{"revision": _int(dataset, "currentRevision")}, {"pinnedUntil": {"$gt": now}}],
            },
            {"patches.fromSeq": 1},
        )
        floor = min(_int(revision, "patches", "fromSeq") for revision in retained)
        return self.db["dataset_patches"].delete_many({"datasetId": dataset_id, "seq": {"$lte": floor}}).deleted_count
