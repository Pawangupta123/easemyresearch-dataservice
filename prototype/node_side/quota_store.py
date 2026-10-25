"""REFERENCE implementation of Node's entry quota for the Data Service (consistency.md §7, §14.2).

Prototype only — Node ports it to TypeScript. The owner's quota document carries
`entries_limit`, `used_entries` (published rows) and `reserved_entries` (held reservations of
Delta jobs). Every increase passes ONE guard, as a single conditional `$inc`:

    used_entries + reserved_entries + n ≤ entries_limit

- A Delta job reserves its rows in the transaction that inserts its operation (`reserve`).
- Its publish settles the reservation (`settle`): the reserved count leaves `reserved_entries`,
  the rows it actually added join `used_entries` — re-checked against the limit if the
  reservation was already released.
- A failed or cancelled operation releases it (`release`); the sweeper releases what is left
  only for operations that are finished or gone — never for a committed or running one (C7).
- A small edit charges its inserts / restores (and credits its deletes) directly (`charge`).

The quota document's lookup is Node's (its active payment for the owner); here it is keyed by
`userId`.
"""

from datetime import UTC, datetime

from pymongo import MongoClient
from pymongo.client_session import ClientSession
from pymongo.database import Database

from prototype.node_side.errors import EditRejected

Document = dict[str, object]
RESOLVED = ("failed", "rolled_back", "cancelled")


def _utc(value: object) -> datetime | None:
    """pymongo hands back naive UTC datetimes."""
    if not isinstance(value, datetime):
        return None
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value


def _within_limit(n: int, *, releasing: int = 0) -> Document:
    """The guard as an aggregation expression: used + reserved − releasing + n ≤ limit."""
    return {
        "$expr": {
            "$lte": [
                {"$add": ["$used_entries", {"$ifNull": ["$reserved_entries", 0]}, n, -releasing]},
                "$entries_limit",
            ]
        }
    }


class MongoQuotaStore:
    def __init__(self, client: MongoClient[Document], database: Database[Document]) -> None:
        self.client, self.db = client, database
        self.payments = database["payments"]
        self.reservations = database["entry_reservations"]
        self.reservations.create_index("opId", unique=True)

    def owner_of(self, dataset_id: str, session: ClientSession | None = None) -> str:
        dataset = self.db["datasets"].find_one({"_id": dataset_id}, {"ownerId": 1}, session=session)
        owner = None if dataset is None else dataset.get("ownerId")
        if not isinstance(owner, str):
            raise ValueError(f"dataset {dataset_id} has no owner")
        return owner

    # ── Delta jobs ───────────────────────────────────────────────────────────

    def reserve(self, session: ClientSession, owner: str, op_id: str, entries: int, until: datetime) -> None:
        """Inside the operation's insert transaction: no capacity → QUOTA_EXCEEDED aborts both."""
        moved = self.payments.update_one(
            {"userId": owner, **_within_limit(entries)}, {"$inc": {"reserved_entries": entries}}, session=session
        )
        if moved.matched_count != 1:
            raise EditRejected("QUOTA_EXCEEDED", f"{entries} more entries do not fit the plan")
        self.reservations.insert_one(
            {"ownerId": owner, "opId": op_id, "entries": entries, "status": "held", "expiresAt": until},
            session=session,
        )

    def settle(self, session: ClientSession, op_id: str, rows_added: int) -> None:
        """Inside the publish transaction. A held reservation turns into `rows_added` used entries
        (checked: an import may add more rows than it reserved); a reservation released meanwhile
        is re-validated from scratch. No capacity → QUOTA_EXCEEDED aborts the publish."""
        reservation = self.reservations.find_one({"opId": op_id}, session=session)
        if reservation is None:
            if rows_added:
                raise EditRejected("QUOTA_EXCEEDED", "the operation added rows without a reservation")
            return
        owner, entries = str(reservation["ownerId"]), int(str(reservation["entries"]))
        held = reservation.get("status") == "held"
        if reservation.get("status") == "settled":
            return  # an idempotent re-publish
        releasing = entries if held else 0
        moved = self.payments.update_one(
            {"userId": owner, **_within_limit(rows_added, releasing=releasing)},
            {"$inc": {"used_entries": rows_added, "reserved_entries": -releasing}},
            session=session,
        )
        if moved.matched_count != 1:
            raise EditRejected("QUOTA_EXCEEDED", "the plan has no room left for these entries")
        self.reservations.update_one({"opId": op_id}, {"$set": {"status": "settled"}}, session=session)

    def release(self, op_id: str) -> bool:
        """Give a held reservation back (its operation failed or was cancelled) — the status flip
        and the counter move in one transaction. Idempotent."""

        def transaction(session: ClientSession) -> bool:
            reservation = self.reservations.find_one_and_update(
                {"opId": op_id, "status": "held"}, {"$set": {"status": "released"}}, session=session
            )
            if reservation is None:
                return False
            entries = int(str(reservation["entries"]))
            self.payments.update_one(
                {"userId": reservation["ownerId"]}, {"$inc": {"reserved_entries": -entries}}, session=session
            )
            return True

        with self.client.start_session() as session:
            return session.with_transaction(transaction)

    def renew(self, dataset_id: str, until: datetime) -> int:
        """The writer's heartbeat: keep the reservations of EVERY unfinished operation alive."""
        unfinished = [
            str(op["opId"])
            for op in self.db["data_operations"].find(
                {"datasetId": dataset_id, "status": {"$nin": ["applied", *RESOLVED]}}, {"opId": 1}
            )
        ]
        renewed = self.reservations.update_many(
            {"opId": {"$in": unfinished}, "status": "held"}, {"$set": {"expiresAt": until}}
        )
        return renewed.modified_count

    def sweep(self, now: datetime) -> list[str]:
        """Release reservations whose operation is finished without publishing, or gone (after
        expiry). A committed, running or queued operation keeps its reservation, expired or not —
        that is what keeps two imports from overrunning the limit (C7)."""
        released = []
        for reservation in self.reservations.find({"status": "held"}, {"opId": 1, "expiresAt": 1}):
            op_id = str(reservation["opId"])
            op = self.db["data_operations"].find_one({"opId": op_id}, {"status": 1})
            expires_at = _utc(reservation.get("expiresAt"))
            expired = expires_at is not None and expires_at < now
            gone_or_done = (op is None and expired) or (op is not None and op.get("status") in RESOLVED)
            if gone_or_done and self.release(op_id):
                released.append(op_id)
        return released

    # ── small edits ──────────────────────────────────────────────────────────

    def charge(self, session: ClientSession, owner: str, delta: int) -> None:
        """Inside the edit transaction: inserts / restores add (guarded), deletes subtract."""
        if delta == 0:
            return
        guard = _within_limit(delta) if delta > 0 else {}
        moved = self.payments.update_one({"userId": owner, **guard}, {"$inc": {"used_entries": delta}}, session=session)
        if moved.matched_count != 1:
            raise EditRejected("QUOTA_EXCEEDED", f"{delta} more entries do not fit the plan")
