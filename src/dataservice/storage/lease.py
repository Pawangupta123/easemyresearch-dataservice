"""The writer lease as the Data Service sees it (consistency.md §4.1).

Node owns the lease (`dataset_leases` in Mongo) and serves it over its internal endpoints; the
Data Service only asks. `LeaseAuthority` is that contract — implemented by the Node client in
production and by prototype/node_side/lease_store.py (the reference Node logic) in tests.
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Protocol


@dataclass(frozen=True)
class Lease:
    dataset_id: str
    owner: str
    token: int
    expires_at: datetime


class LeaseAuthority(Protocol):
    def acquire(self, dataset_id: str, owner: str, max_token_seen: int) -> Lease | None:
        """A lease with a token above both the stored one and `max_token_seen`, or None while
        another owner holds a live lease."""
        ...

    def renew(self, lease: Lease) -> Lease | None:
        """The same lease with a later expiry, or None once it is lost (stop before the next commit)."""
        ...

    def release(self, lease: Lease) -> None:
        """Give the lease up now so the next writer need not wait for expiry."""
        ...
