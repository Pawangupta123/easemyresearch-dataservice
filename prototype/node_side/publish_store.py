"""REFERENCE implementation of Node's publish transaction for Delta jobs (consistency.md §6).

Prototype only — Node ports it to TypeScript. One `withTransaction`, everything or nothing:

1. already applied → the same revision (a lost response never publishes twice, C15);
2. the lease row is WRITTEN (guard_publish) — a takeover and a publish never both commit;
3. the operation is `committed` with this token and attempt; the form is still active;
4. rebase: no base rewrite since the job's base revision, and no small edit since on a field
   whose existing values the job rewrote (`touchedFieldIds`);
5. revision n+1 (the job's table versions, the patch range unchanged, dataVersion n+1),
   Form.dataVersion, analysis staleness for `changedFieldIds`, the operation `applied`, the
   event outbox.
"""

from collections.abc import Mapping
from datetime import UTC, datetime

from pymongo import MongoClient
from pymongo.client_session import ClientSession
from pymongo.database import Database

from dataservice.storage.lease import Lease
from dataservice.storage.operations import PublishRequest
from prototype.node_side.documents import int_at as _int
from prototype.node_side.edit_store import EditRejected
from prototype.node_side.lease_store import MongoLeaseStore

Document = dict[str, object]


class MongoPublishStore:
    def __init__(self, client: MongoClient[Document], database: Database[Document], leases: MongoLeaseStore) -> None:
        self.client, self.db, self.leases = client, database, leases

    def publish(self, request: PublishRequest, lease: Lease) -> int:
        def transaction(session: ClientSession) -> int:
            return self._publish(session, request, lease)

        with self.client.start_session() as session:
            return session.with_transaction(transaction)

    def _publish(self, session: ClientSession, request: PublishRequest, lease: Lease) -> int:
        db = self.db
        op = db["data_operations"].find_one({"opId": request.op_id}, session=session)
        if op is None:
            raise EditRejected("NOT_FOUND", "operation not found")
        if op.get("status") == "applied":
            return _int(op, "appliedRevision")  # C15: the response was lost, the publish was not
        if not self.leases.guard_publish(session, lease, request.op_id):
            raise EditRejected("FENCED", "the lease was lost before publishing")
        commit_token = lease.token if request.commit_token is None else request.commit_token
        if (op.get("status"), op.get("token"), op.get("attempt")) != ("committed", commit_token, request.attempt):
            raise EditRejected("CONFLICT", f"operation is {op.get('status')}, not committed by this attempt")
        dataset_id, form_id = str(op["datasetId"]), str(op["formId"])
        form = db["forms"].find_one({"_id": form_id}, session=session)
        if form is None or form.get("status") == "inactive":
            raise EditRejected("FORM_DELETED", "the form was deleted before the operation published")

        dataset = db["datasets"].find_one({"_id": dataset_id}, session=session)
        if dataset is None:
            raise EditRejected("NOT_FOUND", "dataset not found")
        current = _int(dataset, "currentRevision")
        self._rebase(session, dataset_id, request)

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
        tables.update({name: {"version": version} for name, version in request.commit.table_versions.items()})
        now, revision = datetime.now(UTC), current + 1
        db["dataset_revisions"].insert_one(
            {
                "datasetId": dataset_id,
                "revision": revision,
                "tables": tables,
                "patches": latest["patches"],  # the job folded no patch: the range is unchanged
                "dataVersion": revision,
                "kind": op["type"],
                "opId": request.op_id,
                "writerToken": lease.token,
                "publishedAt": now,
            },
            session=session,
        )
        db["forms"].update_one({"_id": form_id}, {"$set": {"dataVersion": revision}}, session=session)
        if request.commit.result.changed_field_ids:
            db["analyses"].update_many(
                {"formId": form_id, "usedFieldIds": {"$in": list(request.commit.result.changed_field_ids)}},
                {"$inc": {"dataVersion": 1}},
                session=session,
            )
        db["data_operations"].update_one(
            {"opId": request.op_id},
            {
                "$set": {
                    "status": "applied",
                    "appliedRevision": revision,
                    "appliedAt": now,
                    "result": {
                        "rowsAdded": request.commit.result.rows_added,
                        "rowsChanged": request.commit.result.rows_changed,
                        "changedFieldIds": list(request.commit.result.changed_field_ids),
                    },
                }
            },
            session=session,
        )
        db["dataset_events"].insert_one(
            {
                "datasetId": dataset_id,
                "revision": revision,
                "opId": request.op_id,
                "changedFieldIds": list(request.commit.result.changed_field_ids),
                "consumers": {"recompute": "pending", "notify": "pending"},
            },
            session=session,
        )
        return revision

    def _rebase(self, session: ClientSession, dataset_id: str, request: PublishRequest) -> None:
        db = self.db
        rewrites = db["dataset_revisions"].count_documents(
            {"datasetId": dataset_id, "revision": {"$gt": request.commit.base_revision}, "kind": {"$ne": "edit"}},
            session=session,
        )
        if rewrites:
            raise EditRejected("CONFLICT", "another Delta job published since the base revision; re-prepare")
        if not request.commit.result.touched_field_ids:
            return
        base = db["dataset_revisions"].find_one(
            {"datasetId": dataset_id, "revision": request.commit.base_revision}, session=session
        )
        if base is None:
            raise EditRejected("NOT_FOUND", "base revision not found")
        clashes = db["dataset_patches"].count_documents(
            {
                "datasetId": dataset_id,
                "seq": {"$gt": _int(base, "patches", "toSeq")},
                "$or": [
                    {"target.field": {"$in": list(request.commit.result.touched_field_ids)}},
                    {"op": {"$ne": "set"}},
                ],
            },
            session=session,
        )
        if clashes:
            raise EditRejected("CONFLICT", "small edits touched the job's fields since its base; re-prepare")
