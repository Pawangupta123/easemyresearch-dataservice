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

from pymongo import MongoClient
from pymongo.client_session import ClientSession
from pymongo.database import Database
from pymongo.errors import DuplicateKeyError

Document = dict[str, object]
Target = tuple[str, str]  # (row, field)
BeforeValues = Callable[[int, list[Target]], Mapping[Target, object]]
InBase = Callable[[int, str, object], bool]


class EditRejected(Exception):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


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


class MongoEditStore:
    def __init__(self, client: MongoClient[Document], database: Database[Document], unique_field: str | None) -> None:
        self.client, self.db, self.unique_field = client, database, unique_field
        database["dataset_patches"].create_index([("datasetId", 1), ("seq", 1)], unique=True)
        database["dataset_unique_claims"].create_index([("datasetId", 1), ("field", 1), ("value", 1)], unique=True)
        database["dataset_edit_keys"].create_index([("datasetId", 1), ("actor", 1), ("key", 1)], unique=True)

    def create_dataset(self, dataset_id: str, base_version: int, form_id: str) -> None:
        self.db["datasets"].insert_one({"_id": dataset_id, "formId": form_id, "currentRevision": 1, "patchSeq": 0})
        self.db["dataset_revisions"].insert_one(
            {"datasetId": dataset_id, "revision": 1, "baseVersion": base_version, "patchFrom": 0, "patchTo": 0}
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
    ) -> EditResult:
        payload_hash = _payload_hash(edits)

        def transaction(session: ClientSession) -> EditResult:
            return self._apply(session, dataset_id, actor, key, edits, payload_hash, before, in_base)

        with self.client.start_session() as session:
            return session.with_transaction(transaction)

    def _apply(
        self,
        session: ClientSession,
        dataset_id: str,
        actor: str,
        key: str,
        edits: list[Edit],
        payload_hash: str,
        before: BeforeValues,
        in_base: InBase,
    ) -> EditResult:
        db = self.db
        seen = db["dataset_edit_keys"].find_one({"datasetId": dataset_id, "actor": actor, "key": key}, session=session)
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

        seqs = list(range(first_seq, first_seq + len(edits)))
        cells = [(edit.row, edit.field) for edit in edits if edit.op == "set" and edit.field is not None]
        old = before(revision, cells)
        db["dataset_patches"].insert_many(
            [
                {
                    "datasetId": dataset_id,
                    "seq": seq,
                    "op": edit.op,
                    "target": {"row": edit.row, "field": edit.field, "scope": {"kind": "main", "followUp": 0}},
                    "value": {"t": edit.t, "v": edit.v} if edit.op == "set" else None,
                    "batchId": key,
                    "actor": actor,
                }
                for seq, edit in zip(seqs, edits, strict=True)
            ],
            session=session,
        )
        changes = [
            {"datasetId": dataset_id, "revision": revision + 1, "batchId": key, "row": edit.row, "field": edit.field}
            | {"before": old.get((edit.row, edit.field)), "after": edit.v, "actor": actor, "seq": seq}
            for seq, edit in zip(seqs, edits, strict=True)
            if edit.op == "set" and edit.field is not None
        ]
        if changes:
            db["dataset_changes"].insert_many(changes, session=session)
        self._claim_unique_values(session, dataset_id, revision, edits, in_base)

        last = db["dataset_revisions"].find_one({"datasetId": dataset_id, "revision": revision}, session=session)
        if last is None:
            raise ValueError("revision manifest missing")
        db["dataset_revisions"].insert_one(
            {
                "datasetId": dataset_id,
                "revision": revision + 1,
                "baseVersion": last["baseVersion"],
                "patchFrom": last["patchFrom"],
                "patchTo": seqs[-1],
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
        db["dataset_edit_keys"].insert_one(
            {
                "datasetId": dataset_id,
                "actor": actor,
                "key": key,
                "payloadHash": payload_hash,
                "result": {"revision": result.revision, "seqs": seqs},
            },
            session=session,
        )
        return result

    def _claim_unique_values(
        self, session: ClientSession, dataset_id: str, revision: int, edits: list[Edit], in_base: InBase
    ) -> None:
        claims = self.db["dataset_unique_claims"]
        for edit in edits:
            if self.unique_field is None or edit.op != "set" or edit.field != self.unique_field or edit.t == "null":
                continue
            if in_base(revision, self.unique_field, edit.v):
                raise EditRejected("DUPLICATE_UNIQUE_ID", f"{edit.v} already exists")
            claims.delete_many({"datasetId": dataset_id, "field": edit.field, "row": edit.row}, session=session)
            try:
                claims.insert_one(
                    {"datasetId": dataset_id, "field": edit.field, "value": edit.v, "row": edit.row}, session=session
                )
            except DuplicateKeyError as duplicate:
                raise EditRejected("DUPLICATE_UNIQUE_ID", f"{edit.v} already exists") from duplicate
