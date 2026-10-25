"""REFERENCE implementation of Node's small-edit transaction (hybrid edits).

Prototype only — proves the protocol on a real Mongo replica set; Node ports exactly this to
TypeScript. One `withTransaction` writes everything a small edit changes, or nothing:

    idempotency key → CAS datasets.currentRevision n → n+1 → patches (seq from patchSeq) →
    change log (before/after) → unique-identifier claims → revision n+1 → form dataVersion →
    dataset_events outbox

Two concurrent edits write the same `datasets` document, so Mongo serialises them (one retries
on the write conflict): on the same cell the later commit wins, and both stay in the change log.
"""

import hashlib
import json
from collections.abc import Callable, Mapping
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta

from pymongo import MongoClient
from pymongo.client_session import ClientSession
from pymongo.database import Database
from pymongo.errors import DuplicateKeyError

from prototype.node_side.errors import EditRejected
from prototype.node_side.quota_store import MongoQuotaStore

__all__ = ["Edit", "EditRejected", "EditResult", "MongoEditStore"]

Document = dict[str, object]
Target = tuple[str, str]  # (row, field)
IDEMPOTENCY_TTL = timedelta(hours=24)  # config in Node; the reference keeps the documented default
BeforeValues = Callable[[int, list[Target]], Mapping[Target, object]]
InBase = Callable[[int, str, object], bool]
# Runs inside the transaction once revision n is fixed (the CAS holds it): raise EditRejected to
# abort. Undo uses it to re-check its losses at the exact revision it writes on.
Check = Callable[[int], None]
# Each row's state at a revision: 'live', 'deleted' or 'absent' (the quota counts with it).
RowStates = Callable[[int, list[str]], Mapping[str, str]]


@dataclass(frozen=True)
class Edit:
    row: str
    op: str  # set | insert_row | delete_row | restore_row
    field: str | None = None
    t: str | None = None  # value type, "null" to clear
    v: object = None


@dataclass(frozen=True)
class EditResult:
    revision: int
    seqs: list[int]


def _stored_result(stored: object) -> EditResult:
    if isinstance(stored, Mapping):
        revision, seqs = stored.get("revision"), stored.get("seqs")
        if isinstance(revision, int) and isinstance(seqs, list) and all(isinstance(seq, int) for seq in seqs):
            return EditResult(revision=revision, seqs=list(seqs))
    raise ValueError(f"malformed idempotency record: {stored}")


def _payload_hash(edits: list[Edit]) -> str:
    return hashlib.sha256(json.dumps([asdict(edit) for edit in edits], default=str).encode()).hexdigest()


def _typed(value_type: str, value: object) -> Document:
    """A change-log value as `{ t, v }` (docs/design/data-model.md, dataset_changes)."""
    return {"t": "null", "v": None} if value is None else {"t": value_type, "v": value}


class MongoEditStore:
    """Collections and fields exactly as docs/design/data-model.md names them."""

    def __init__(
        self,
        client: MongoClient[Document],
        database: Database[Document],
        field_types: Mapping[str, str],
        unique_field: str | None,
        quota: MongoQuotaStore,
        row_states: RowStates,
    ) -> None:
        self.client, self.db = client, database
        self.field_types, self.unique_field = dict(field_types), unique_field
        self.quota, self.row_states = quota, row_states
        database["dataset_patches"].create_index([("datasetId", 1), ("seq", 1)], unique=True)
        database["dataset_unique_claims"].create_index([("datasetId", 1), ("fieldId", 1), ("value", 1)], unique=True)
        database["dataset_idempotency"].create_index([("actor", 1), ("idempotencyKey", 1)], unique=True)
        database["dataset_idempotency"].create_index("expiresAt", expireAfterSeconds=0)

    def create_dataset(self, dataset_id: str, base_version: int, form_id: str, owner_id: str) -> None:
        self.db["datasets"].insert_one(
            {"_id": dataset_id, "formId": form_id, "ownerId": owner_id, "currentRevision": 1, "patchSeq": 0}
        )
        self.db["dataset_revisions"].insert_one(
            {
                "datasetId": dataset_id,
                "revision": 1,
                "tables": {"base": {"version": base_version}},
                "patches": {"fromSeq": 0, "toSeq": 0},
                "dataVersion": 1,
                "kind": "ingest",
                "writerToken": None,
            }
        )
        self.db["forms"].insert_one({"_id": form_id, "dataVersion": 1})

    def apply(
        self,
        dataset_id: str,
        actor: str,
        key: str,
        edits: list[Edit],
        before: BeforeValues,
        in_base: InBase,
        check: Check | None = None,
        batch_id: str | None = None,
    ) -> EditResult:
        """`key` is the caller's idempotency key (one per request); `batch_id` (≡ today's
        mutationId, default: the key) groups the parts of one save for history and undo."""
        payload_hash = _payload_hash(edits)
        batch = batch_id or key

        def transaction(session: ClientSession) -> EditResult:
            return self._apply(session, dataset_id, actor, key, batch, edits, payload_hash, before, in_base, check)

        with self.client.start_session() as session:
            return session.with_transaction(transaction)

    def _apply(
        self,
        session: ClientSession,
        dataset_id: str,
        actor: str,
        key: str,
        batch: str,
        edits: list[Edit],
        payload_hash: str,
        before: BeforeValues,
        in_base: InBase,
        check: Check | None,
    ) -> EditResult:
        db = self.db
        seen = db["dataset_idempotency"].find_one({"actor": actor, "idempotencyKey": key}, session=session)
        if seen is not None:  # a browser retry of a save that already landed
            if seen["payloadHash"] != payload_hash:
                raise EditRejected("IDEMPOTENCY_MISMATCH", "this key was used for a different save")
            return _stored_result(seen.get("result"))

        dataset = db["datasets"].find_one({"_id": dataset_id}, session=session)
        if dataset is None:
            raise EditRejected("NOT_FOUND", "dataset not found")
        revision, patch_seq = dataset.get("currentRevision"), dataset.get("patchSeq")
        if not (isinstance(revision, int) and isinstance(patch_seq, int)):
            raise ValueError(f"malformed dataset row: {dataset}")
        first_seq = patch_seq + 1
        moved = db["datasets"].update_one(
            {"_id": dataset_id, "currentRevision": revision},
            {"$set": {"currentRevision": revision + 1}, "$inc": {"patchSeq": len(edits)}},
            session=session,
        )
        if moved.matched_count != 1:
            raise EditRejected("CONFLICT", "dataset moved; retry")
        if check is not None:
            check(revision)

        seqs = list(range(first_seq, first_seq + len(edits)))
        cells = [(edit.row, edit.field) for edit in edits if edit.op == "set" and edit.field is not None]
        old = before(revision, cells)
        now = datetime.now(UTC)
        db["dataset_patches"].insert_many(
            [
                {
                    "datasetId": dataset_id,
                    "seq": seq,
                    "op": edit.op,
                    "target": {"row": edit.row, "field": edit.field, "scope": {"kind": "main", "followUp": 0}},
                    "value": {"t": edit.t, "v": edit.v} if edit.op == "set" else None,
                    "opId": f"{key}:{seq}",
                    "batchId": batch,
                    "actor": actor,
                    "at": now,
                }
                for seq, edit in zip(seqs, edits, strict=True)
            ],
            session=session,
        )
        changes: list[Document] = [
            {
                "datasetId": dataset_id,
                "revision": revision + 1,
                "batchId": batch,
                "target": {"row": edit.row, "field": edit.field, "scope": {"kind": "main", "followUp": 0}},
                "before": _typed(self.field_types[edit.field], old.get((edit.row, edit.field))),
                "after": _typed(self.field_types[edit.field], None if edit.t == "null" else edit.v),
                "actor": actor,
                "at": now,
            }
            for edit in edits
            if edit.op == "set" and edit.field is not None
        ]
        if changes:
            db["dataset_changes"].insert_many(changes, session=session)
        self._claim_unique_values(session, dataset_id, revision, edits, seqs, in_base)
        self.quota.charge(session, self.quota.owner_of(dataset_id, session), self._entries_delta(revision, edits))

        last = db["dataset_revisions"].find_one({"datasetId": dataset_id, "revision": revision}, session=session)
        if last is None:
            raise ValueError("revision manifest missing")
        patches = last.get("patches")
        if not isinstance(patches, Mapping):
            raise ValueError("revision manifest has no patch range")
        db["dataset_revisions"].insert_one(
            {
                "datasetId": dataset_id,
                "revision": revision + 1,
                "tables": last["tables"],  # small edits never move a Delta table
                "patches": {"fromSeq": patches["fromSeq"], "toSeq": seqs[-1]},
                "dataVersion": revision + 1,  # a small edit always changes values
                "kind": "edit",
                "batchId": batch,
                "writerToken": None,
                "publishedAt": now,
            },
            session=session,
        )
        db["forms"].update_one({"_id": dataset["formId"]}, {"$set": {"dataVersion": revision + 1}}, session=session)
        db["dataset_events"].insert_one(
            {
                "datasetId": dataset_id,
                "revision": revision + 1,
                "consumers": {"recompute": "pending", "notify": "pending"},
            },
            session=session,
        )
        result = EditResult(revision=revision + 1, seqs=seqs)
        db["dataset_idempotency"].insert_one(
            {
                "actor": actor,
                "idempotencyKey": key,
                "payloadHash": payload_hash,
                "datasetId": dataset_id,
                "revision": result.revision,
                "result": {"revision": result.revision, "seqs": seqs},
                "expiresAt": now + IDEMPOTENCY_TTL,
            },
            session=session,
        )
        return result

    def _entries_delta(self, revision: int, edits: list[Edit]) -> int:
        """Live rows this edit adds (+) or removes (−), from each row's state at the revision it
        writes on and in edit order — a delete then restore in one save counts zero."""
        rows = sorted({edit.row for edit in edits if edit.op != "set"})
        if not rows:
            return 0
        state = dict(self.row_states(revision, rows))
        delta = 0
        for edit in edits:
            now = state.get(edit.row, "absent")
            if edit.op == "insert_row" and now == "absent":
                state[edit.row], delta = "live", delta + 1
            elif edit.op == "restore_row" and now == "deleted":
                state[edit.row], delta = "live", delta + 1
            elif edit.op == "delete_row" and now == "live":
                state[edit.row], delta = "deleted", delta - 1
        return delta

    def _claim_unique_values(
        self,
        session: ClientSession,
        dataset_id: str,
        revision: int,
        edits: list[Edit],
        seqs: list[int],
        in_base: InBase,
    ) -> None:
        claims = self.db["dataset_unique_claims"]
        for seq, edit in zip(seqs, edits, strict=True):
            if self.unique_field is None or edit.op != "set" or edit.field != self.unique_field or edit.t == "null":
                continue
            if in_base(revision, self.unique_field, edit.v):
                raise EditRejected("DUPLICATE_UNIQUE_ID", f"{edit.v} already exists")
            claims.delete_many({"datasetId": dataset_id, "fieldId": edit.field, "row": edit.row}, session=session)
            try:
                claims.insert_one(
                    {"datasetId": dataset_id, "fieldId": edit.field, "value": edit.v, "row": edit.row, "seq": seq},
                    session=session,
                )
            except DuplicateKeyError as duplicate:
                raise EditRejected("DUPLICATE_UNIQUE_ID", f"{edit.v} already exists") from duplicate
