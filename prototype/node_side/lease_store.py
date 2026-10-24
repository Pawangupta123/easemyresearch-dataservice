"""REFERENCE implementation of Node's side of the writer lease (consistency.md §4.1).

Prototype only — runs in tests against a real Mongo replica set to prove the protocol; Node
ports exactly this logic to TypeScript in Phase 2. Every change to the lease row is a
compare-and-set, and every time is Mongo's server clock (`$$NOW`), never a process clock.
"""

from datetime import UTC, datetime

from pymongo import ReturnDocument
from pymongo.client_session import ClientSession
from pymongo.collection import Collection
from pymongo.database import Database
from pymongo.errors import DuplicateKeyError

from dataservice.storage.lease import Lease

Document = dict[str, object]
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


def _expiry(ttl_ms: int) -> Document:
    return {"$dateAdd": {"startDate": "$$NOW", "unit": "millisecond", "amount": ttl_ms}}


def _lease(document: Document | None) -> Lease | None:
    if document is None:
        return None
    dataset_id, owner, token, expires_at = (document.get(k) for k in ("datasetId", "owner", "token", "expiresAt"))
    if not (isinstance(dataset_id, str) and isinstance(owner, str) and isinstance(token, int)):
        raise ValueError(f"malformed lease row: {document}")
    if not isinstance(expires_at, datetime):
        raise ValueError(f"malformed lease expiry: {document}")
    return Lease(dataset_id=dataset_id, owner=owner, token=token, expires_at=expires_at.replace(tzinfo=UTC))


class MongoLeaseStore:
    def __init__(self, database: Database[Document], ttl_ms: int, max_attempts: int = 5) -> None:
        self.leases: Collection[Document] = database["dataset_leases"]
        self.leases.create_index("datasetId", unique=True)
        self.ttl_ms = ttl_ms
        self.max_attempts = max_attempts

    def acquire(self, dataset_id: str, owner: str, max_token_seen: int) -> Lease | None:
        for _ in range(self.max_attempts):
            current = self.leases.find_one({"datasetId": dataset_id})
            if current is None:
                lease = self._create(dataset_id, owner, max_token_seen + 1)
            else:
                previous = current.get("token")
                if not isinstance(previous, int):
                    raise ValueError(f"malformed lease row: {current}")
                lease = self.compare_and_set(dataset_id, owner, previous, max(previous, max_token_seen) + 1)
            if lease is not None:
                return lease
            if self._held_by_another(dataset_id, owner):
                return None
            # The row moved between the read and the write (another acquire): read again.
        return None

    def compare_and_set(self, dataset_id: str, owner: str, expected_token: int, new_token: int) -> Lease | None:
        """One acquire step: succeeds only if the token is still `expected_token` and the lease
        is expired or already ours — so an acquire computed from a stale read matches nothing
        and two holders can never share a token (scenario C29)."""
        return _lease(
            self.leases.find_one_and_update(
                {
                    "datasetId": dataset_id,
                    "token": expected_token,
                    "$expr": {"$or": [{"$lt": ["$expiresAt", "$$NOW"]}, {"$eq": ["$owner", owner]}]},
                },
                [{"$set": {"owner": owner, "token": new_token, "expiresAt": _expiry(self.ttl_ms)}}],
                return_document=ReturnDocument.AFTER,
            )
        )

    def renew(self, lease: Lease) -> Lease | None:
        return _lease(
            self.leases.find_one_and_update(
                {
                    "datasetId": lease.dataset_id,
                    "owner": lease.owner,
                    "token": lease.token,
                    "$expr": {"$gt": ["$expiresAt", "$$NOW"]},
                },
                [{"$set": {"expiresAt": _expiry(self.ttl_ms)}}],
                return_document=ReturnDocument.AFTER,
            )
        )

    def release(self, lease: Lease) -> None:
        self.leases.update_one(
            {"datasetId": lease.dataset_id, "owner": lease.owner, "token": lease.token},
            {"$set": {"expiresAt": EPOCH}},
        )

    def guard_publish(self, session: ClientSession, lease: Lease, published: str) -> bool:
        """Publish step 1 (consistency §6): WRITE the lease row inside the publish transaction.
        A takeover writes the same row, so the two can never both commit (scenario C18)."""
        result = self.leases.update_one(
            {"datasetId": lease.dataset_id, "token": lease.token, "$expr": {"$gt": ["$expiresAt", "$$NOW"]}},
            {"$set": {"lastPublished": published}},
            session=session,
        )
        return result.matched_count == 1

    def _create(self, dataset_id: str, owner: str, token: int) -> Lease | None:
        try:
            return _lease(
                self.leases.find_one_and_update(
                    {"datasetId": dataset_id, "token": {"$exists": False}},
                    [
                        {
                            "$set": {
                                "datasetId": dataset_id,
                                "owner": owner,
                                "token": token,
                                "expiresAt": _expiry(self.ttl_ms),
                            }
                        }
                    ],
                    upsert=True,
                    return_document=ReturnDocument.AFTER,
                )
            )
        except DuplicateKeyError:
            return None  # another writer created it first

    def _held_by_another(self, dataset_id: str, owner: str) -> bool:
        return (
            self.leases.count_documents(
                {"datasetId": dataset_id, "owner": {"$ne": owner}, "$expr": {"$gt": ["$expiresAt", "$$NOW"]}}
            )
            > 0
        )
