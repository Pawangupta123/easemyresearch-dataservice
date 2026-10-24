"""REFERENCE implementation of Node's batch undo on the change log (consistency.md §8.2).

Prototype only — Node ports it to TypeScript behind today's routes
(`GET /revisions/batches/:mutationId/restore-preview`, `POST …/restore`).

Per cell (a deliberate change from today's whole-row pre-images): for every target the batch
changed, the undo writes the batch's first `before` value back.

- the cell still holds the batch's last `after` → restored, nothing else is touched;
- someone changed it since → a **loss**: restoring overwrites their value, so the caller must
  consent with the preview's `lossSetHash`;
- the row is gone (deleted since) → **excluded**, skipped.

The undo itself is one small edit (idempotency key `undo:<batchId>`), so it lands in the change
log too — undoing the undo is a redo. Its losses are re-checked INSIDE the edit transaction at the
exact revision it writes on, so an edit landing between preview and apply is never overwritten
without consent.
"""

import hashlib
import json
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import date, datetime

from pymongo.database import Database

from prototype.node_side.edit_store import Edit, EditRejected, InBase, MongoEditStore, Target

Document = dict[str, object]
RowsAt = Callable[[int], Mapping[str, Mapping[str, object]]]  # live rows of a revision, by _rid


@dataclass(frozen=True)
class UndoEntry:
    row: str
    field: str
    value_type: str
    before: object  # what the undo writes back (None = clear)
    after: object  # what the batch wrote last
    current: object  # what the cell holds now


@dataclass(frozen=True)
class _Span:
    """What one batch did to one cell: its value type, first before and last after."""

    value_type: str
    before: object
    after: object


@dataclass(frozen=True)
class UndoPlan:
    revision: int
    entries: tuple[UndoEntry, ...]
    losses: tuple[UndoEntry, ...]
    excluded: tuple[Target, ...]
    loss_set_hash: str

    def same_outcome(self, other: "UndoPlan") -> bool:
        return (self.entries, self.losses, self.excluded) == (other.entries, other.losses, other.excluded)

    def edits(self) -> list[Edit]:
        return [
            Edit(row=entry.row, op="set", field=entry.field, t="null")
            if entry.before is None
            else Edit(row=entry.row, op="set", field=entry.field, t=entry.value_type, v=entry.before)
            for entry in self.entries
        ]


@dataclass(frozen=True)
class UndoResult:
    revision: int
    restored: int
    skipped: int
    undo_batch_id: str


def _value(typed: object) -> tuple[str, object]:
    if not isinstance(typed, Mapping):
        raise ValueError(f"malformed change value: {typed}")
    t, v = typed.get("t"), typed.get("v")
    if not isinstance(t, str):
        raise ValueError(f"malformed change value: {typed}")
    if t == "date" and isinstance(v, datetime):
        v = v.date()  # BSON has no date-only type
    return t, (None if t == "null" else v)


def _loss_set_hash(losses: tuple[UndoEntry, ...]) -> str:
    def plain(value: object) -> object:
        return value.isoformat() if isinstance(value, date) else value

    payload = sorted([entry.row, entry.field, plain(entry.current)] for entry in losses)
    return hashlib.sha256(json.dumps(payload, default=str).encode()).hexdigest()


class MongoUndoStore:
    def __init__(self, database: Database[Document], edits: MongoEditStore, rows_at: RowsAt, in_base: InBase) -> None:
        self.db, self.edit_store, self.rows_at, self.in_base = database, edits, rows_at, in_base

    def current_revision(self, dataset_id: str) -> int:
        dataset = self.db["datasets"].find_one({"_id": dataset_id})
        revision = None if dataset is None else dataset.get("currentRevision")
        if not isinstance(revision, int):
            raise EditRejected("NOT_FOUND", "dataset not found")
        return revision

    def preview(self, dataset_id: str, batch_id: str, revision: int | None = None) -> UndoPlan:
        at = self.current_revision(dataset_id) if revision is None else revision
        targets: dict[Target, _Span] = {}
        for change in (
            self.db["dataset_changes"].find({"datasetId": dataset_id, "batchId": batch_id}).sort("revision", 1)
        ):
            target = change.get("target")
            if not isinstance(target, Mapping):
                raise ValueError(f"malformed change: {change}")
            key = (str(target["row"]), str(target["field"]))
            before_type, before = _value(change.get("before"))
            after_type, after = _value(change.get("after"))
            known = targets.get(key)
            if known is None:
                targets[key] = _Span(after_type if after_type != "null" else before_type, before, after)
            else:  # the batch touched the cell again: keep its first before, take its last after
                targets[key] = _Span(known.value_type if after_type == "null" else after_type, known.before, after)
        if not targets:
            raise EditRejected("NOT_FOUND", "nothing to undo for this batch")

        rows = self.rows_at(at)
        entries: list[UndoEntry] = []
        losses: list[UndoEntry] = []
        excluded: list[Target] = []
        for (row, field), span in targets.items():
            if row not in rows:
                excluded.append((row, field))
                continue
            entry = UndoEntry(row, field, span.value_type, span.before, span.after, rows[row].get(f"f_{field}"))
            entries.append(entry)
            if entry.current != span.after:
                losses.append(entry)
        return UndoPlan(at, tuple(entries), tuple(losses), tuple(excluded), _loss_set_hash(tuple(losses)))

    def apply(self, dataset_id: str, actor: str, batch_id: str, consent: str | None = None) -> UndoResult:
        plan = self.preview(dataset_id, batch_id)
        undo_batch_id = f"undo:{batch_id}"
        if not plan.entries:
            return UndoResult(plan.revision, 0, len(plan.excluded), undo_batch_id)
        # A retried undo that already landed: its cells now hold the before values, which would
        # read as losses — skip the checks; the edit store returns the stored result for the key.
        landed = self.db["dataset_idempotency"].count_documents({"actor": actor, "idempotencyKey": undo_batch_id}) > 0

        def check(revision: int) -> None:
            if landed:
                return
            now = self.preview(dataset_id, batch_id, revision)
            if not now.same_outcome(plan):
                raise EditRejected("CONFLICT", "the data changed since the preview; preview again")
            if now.losses and consent != now.loss_set_hash:
                raise EditRejected("UNDO_CONFLICT", "later edits touched these cells; consent is required")

        check(plan.revision)  # fail fast before opening the transaction (no-op for a landed retry)

        def before_values(revision: int, cells: list[Target]) -> Mapping[Target, object]:
            rows = self.rows_at(revision)
            return {(row, field): rows.get(row, {}).get(f"f_{field}") for row, field in cells}

        result = self.edit_store.apply(
            dataset_id, actor, undo_batch_id, plan.edits(), before_values, self.in_base, check=check
        )
        return UndoResult(result.revision, len(plan.entries), len(plan.excluded), undo_batch_id)
