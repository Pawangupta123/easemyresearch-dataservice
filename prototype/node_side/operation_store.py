"""REFERENCE implementation of Node's Delta-job operation queue (consistency.md §5).

Prototype only — Node ports it to TypeScript. A Delta job (import, type cast, transform, set-based
edit) is first a durable `data_operations` document; every status change is a compare-and-set on
`(opId, expected status, token, attempt)` exactly as the status machine table says, so a delayed
callback from an older attempt or an older lease token can never complete, fail or publish a
newer one (C28), and cancel races `committed` on one document (C32).

Not in the prototype: payload encryption / S3 offload (§5.1) and the dispatcher + sweeper.
"""

import hashlib
import json
from collections.abc import Mapping
from datetime import UTC, datetime
from uuid import uuid4

from pymongo import MongoClient, ReturnDocument
from pymongo.client_session import ClientSession
from pymongo.database import Database
from pymongo.errors import DuplicateKeyError

from prototype.node_side.edit_store import EditRejected

Document = dict[str, object]
CANCELLABLE = ("held", "pending", "dispatched", "running")


def _payload_hash(op_type: str, payload: Mapping[str, object]) -> str:
    return hashlib.sha256(json.dumps([op_type, payload], sort_keys=True, default=str).encode()).hexdigest()


class MongoOperationStore:
    def __init__(self, client: MongoClient[Document], database: Database[Document]) -> None:
        self.client, self.db = client, database
        self.ops = database["data_operations"]
        self.ops.create_index("opId", unique=True)
        self.ops.create_index([("actor", 1), ("idempotencyKey", 1)], unique=True)
        self.ops.create_index(
            [("datasetId", 1), ("seq", 1)], unique=True, partialFilterExpression={"seq": {"$type": "number"}}
        )

    # ── submit ───────────────────────────────────────────────────────────────

    def submit(
        self, dataset_id: str, form_id: str, actor: str, key: str, op_type: str, payload: Mapping[str, object]
    ) -> str:
        """One transaction: the caller's key → the same operation on a retry (C31), a different
        payload under the same key → IDEMPOTENCY_MISMATCH, else a new `pending` operation with
        the next `seq` of the dataset."""
        payload_hash = _payload_hash(op_type, payload)

        def transaction(session: ClientSession) -> str:
            seen = self.ops.find_one({"actor": actor, "idempotencyKey": key}, session=session)
            if seen is not None:
                if seen.get("payloadHash") != payload_hash:
                    raise EditRejected("IDEMPOTENCY_MISMATCH", "this key was used for a different request")
                return str(seen["opId"])
            counter = self.db["datasets"].find_one_and_update(
                {"_id": dataset_id}, {"$inc": {"opSeq": 1}}, return_document=ReturnDocument.AFTER, session=session
            )
            if counter is None:
                raise EditRejected("NOT_FOUND", "dataset not found")
            op_id = uuid4().hex
            self.ops.insert_one(
                {
                    "opId": op_id,
                    "idempotencyKey": key,
                    "payloadHash": payload_hash,
                    "seq": counter["opSeq"],
                    "datasetId": dataset_id,
                    "formId": form_id,
                    "actor": actor,
                    "type": op_type,
                    "payload": {"inline": dict(payload)},
                    "status": "pending",
                    "token": None,
                    "attempt": 0,
                    "rolledBackAttempts": [],
                    "committedTableVersions": None,
                    "appliedRevision": None,
                    "result": None,
                    "error": None,
                    "createdAt": datetime.now(UTC),
                },
                session=session,
            )
            return op_id

        with self.client.start_session() as session:
            try:
                return session.with_transaction(transaction)
            except DuplicateKeyError:  # two submits with one key raced: the other one landed
                return self.submit(dataset_id, form_id, actor, key, op_type, payload)

    # ── status machine ───────────────────────────────────────────────────────

    def get(self, op_id: str) -> Document:
        op = self.ops.find_one({"opId": op_id}, {"_id": 0})
        if op is None:
            raise EditRejected("NOT_FOUND", "operation not found")
        return op

    def dispatch(self, op_id: str) -> None:
        self._cas(op_id, {"status": "pending"}, {"status": "dispatched"}, "only a pending operation is dispatched")

    def running(self, op_id: str, token: int) -> int:
        """→ running, attempt + 1. The same token again while running returns the same attempt
        (no increment); a higher token takes over a running operation (the old holder lost its
        lease) as a new attempt; anything else is refused."""
        op = self.get(op_id)
        if op.get("status") == "running" and op.get("token") == token:
            return int(str(op["attempt"]))
        moved = self.ops.find_one_and_update(
            {
                "opId": op_id,
                "$or": [{"status": "dispatched"}, {"status": "running", "token": {"$lt": token}}],
            },
            {"$set": {"status": "running", "token": token}, "$inc": {"attempt": 1}},
            return_document=ReturnDocument.AFTER,
        )
        if moved is None:
            raise EditRejected("CONFLICT", f"operation is {op.get('status')} under token {op.get('token')}")
        return int(str(moved["attempt"]))

    def committed(self, op_id: str, token: int, attempt: int, table_versions: Mapping[str, int]) -> None:
        guard: Document = {"token": token, "attempt": attempt}
        op = self.get(op_id)
        if op.get("status") == "committed" and all(op.get(k) == v for k, v in guard.items()):
            return  # the same call again
        self._cas(
            op_id,
            {"status": "running", **guard},
            {"status": "committed", "committedTableVersions": dict(table_versions), "committedAt": datetime.now(UTC)},
            "not the running attempt",
        )

    def failed(self, op_id: str, token: int, attempt: int, error: Mapping[str, str], rolled_back: bool) -> None:
        update: Document = {"status": "rolled_back" if rolled_back else "failed", "error": dict(error)}
        self._cas(op_id, {"status": "running", "token": token, "attempt": attempt}, update, "not the running attempt")
        if rolled_back:
            self.ops.update_one({"opId": op_id}, {"$addToSet": {"rolledBackAttempts": attempt}})

    def cancel(self, op_id: str) -> None:
        """Wins only before `committed` (C32); after it, TOO_LATE — the operation publishes."""
        moved = self.ops.find_one_and_update(
            {"opId": op_id, "status": {"$in": list(CANCELLABLE)}},
            {"$set": {"status": "cancelled"}},
        )
        if moved is not None:
            return
        status = self.get(op_id).get("status")
        if status == "cancelled":
            return
        raise EditRejected("TOO_LATE", f"the operation is already {status}; it completes normally")

    def _cas(self, op_id: str, expected: Document, update: Document, reason: str) -> None:
        moved = self.ops.find_one_and_update({"opId": op_id} | expected, {"$set": update})
        if moved is None:
            op = self.get(op_id)
            raise EditRejected("CONFLICT", f"{reason}: operation is {op.get('status')} (attempt {op.get('attempt')})")
