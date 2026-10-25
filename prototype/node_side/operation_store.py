"""REFERENCE implementation of Node's Delta-job operation queue (consistency.md §5).

Prototype only — Node ports it to TypeScript. A Delta job (import, type cast, transform, set-based
edit) is first a durable `data_operations` document; every status change is a compare-and-set on
`(opId, expected status, token, attempt)` exactly as the status machine table says, so a delayed
callback from an older attempt or an older lease token can never complete, fail or publish a
newer one (C28), and cancel races `committed` on one document (C32).

The queue (§4.4): operations run strictly in `seq` order — `head` is the lowest-seq operation
that is not finished, and nothing behind it runs first. The sweeper re-dispatches operations left
`pending` (a crash between the insert and the wake-up, C5). A failing attempt goes back to
`dispatched` for a retry until `max_attempts`; then it is `failed` for good (a poison operation,
C6) and the queue moves on.

Not in the prototype: payload encryption / S3 offload (§5.1) and BullMQ itself (the wake-up).
"""

import hashlib
import json
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from uuid import uuid4

from pymongo import MongoClient, ReturnDocument
from pymongo.client_session import ClientSession
from pymongo.database import Database
from pymongo.errors import DuplicateKeyError

from prototype.node_side.edit_store import EditRejected

Document = dict[str, object]
CANCELLABLE = ("held", "pending", "dispatched", "running")
UNFINISHED = ("held", "pending", "dispatched", "running", "committed")


def _payload_hash(op_type: str, payload: Mapping[str, object]) -> str:
    return hashlib.sha256(json.dumps([op_type, payload], sort_keys=True, default=str).encode()).hexdigest()


class MongoOperationStore:
    def __init__(self, client: MongoClient[Document], database: Database[Document], max_attempts: int) -> None:
        """`max_attempts` comes from Node's configuration (poison threshold, prd §4.8)."""
        self.client, self.db, self.max_attempts = client, database, max_attempts
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
        lease) as a new attempt; anything else is refused. An operation that has used up its
        attempts is failed for good instead (poison, C6)."""
        op = self.get(op_id)
        if op.get("status") == "running" and op.get("token") == token:
            return int(str(op["attempt"]))
        if int(str(op.get("attempt", 0))) >= self.max_attempts:
            self.ops.update_one(
                {"opId": op_id, "status": {"$in": ["dispatched", "running"]}},
                {"$set": {"status": "failed", "error": {"code": "OPERATION_FAILED", "message": "too many attempts"}}},
            )
            raise EditRejected("OPERATION_FAILED", f"operation {op_id} failed after {self.max_attempts} attempts")
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

    def committed(
        self, op_id: str, token: int, attempt: int, table_versions: Mapping[str, int], commit: Document
    ) -> None:
        """`commit` = what a later lease holder needs to publish it without re-running it."""
        guard: Document = {"token": token, "attempt": attempt}
        op = self.get(op_id)
        if op.get("status") == "committed" and all(op.get(k) == v for k, v in guard.items()):
            return  # the same call again
        self._cas(
            op_id,
            {"status": "running", **guard},
            {
                "status": "committed",
                "committedTableVersions": dict(table_versions),
                "commit": commit,
                "committedAt": datetime.now(UTC),
            },
            "not the running attempt",
        )

    def failed(self, op_id: str, token: int, attempt: int, error: Mapping[str, str], retryable: bool) -> bool:
        """The writer gave up this attempt (and rolled its commits back). Retryable and attempts
        left → `dispatched` again (the next `running` is attempt + 1); otherwise `failed` for good.
        Returns True when the operation is finished."""
        final = not retryable or attempt >= self.max_attempts
        update: Document = {"status": "failed" if final else "dispatched", "error": dict(error)}
        self._cas(op_id, {"status": "running", "token": token, "attempt": attempt}, update, "not the running attempt")
        if not final:
            self.ops.update_one({"opId": op_id}, {"$addToSet": {"rolledBackAttempts": attempt}})
        return final

    # ── the queue ────────────────────────────────────────────────────────────

    def head(self, dataset_id: str) -> Document | None:
        """The lowest-seq operation not finished yet — the only one that may run (§4.4)."""
        return self.ops.find_one(
            {"datasetId": dataset_id, "status": {"$in": list(UNFINISHED)}, "seq": {"$type": "number"}},
            {"_id": 0},
            sort=[("seq", 1)],
        )

    def sweep(self, now: datetime, stale_after: timedelta) -> list[str]:
        """Dispatch operations left `pending` longer than `stale_after` (the wake-up was lost, C5).
        Returns the datasets to wake."""
        stale = list(
            self.ops.find({"status": "pending", "createdAt": {"$lt": now - stale_after}}, {"opId": 1, "datasetId": 1})
        )
        for op in stale:
            self.ops.update_one({"opId": op["opId"], "status": "pending"}, {"$set": {"status": "dispatched"}})
        return sorted({str(op["datasetId"]) for op in stale})

    def committed_for(self, dataset_id: str) -> list[Document]:
        return list(self.ops.find({"datasetId": dataset_id, "status": "committed"}, {"_id": 0}))

    def rolled_back(self, op_id: str, attempt: int, token: int) -> None:
        """Reconciliation (by the holder of `token`) removed `attempt`'s commits: remember the
        attempt and make the operation runnable again. Only an attempt of an OLDER token that
        never reached `committed` can be rolled back this way."""
        moved = self.ops.find_one_and_update(
            {"opId": op_id, "attempt": attempt, "status": "running", "token": {"$lt": token}},
            {"$set": {"status": "dispatched"}, "$addToSet": {"rolledBackAttempts": attempt}},
        )
        if moved is None:
            op = self.get(op_id)
            recorded = op.get("rolledBackAttempts")
            if isinstance(recorded, list) and attempt in recorded:
                return  # already recorded
            raise EditRejected("CONFLICT", f"cannot roll back attempt {attempt}: operation is {op.get('status')}")
        # a dispatched operation runs again with the next attempt (running() accepts dispatched)

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
