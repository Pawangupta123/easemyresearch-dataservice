"""The writer lease (consistency.md §4.1) on a real Mongo replica set — scenarios C29 and C18.

Runs the reference Node logic (prototype/node_side/lease_store.py) the way the Data Service
will call it.
"""

import threading
import time
from datetime import UTC, datetime

import pytest
from pymongo import MongoClient
from pymongo.database import Database
from pymongo.errors import OperationFailure

from prototype.node_side.lease_store import EPOCH, MongoLeaseStore
from tests.integration.conftest import Document

DATASET = "ds-1"
TTL_MS = 60_000


@pytest.fixture
def store(mongo_db: Database[Document]) -> MongoLeaseStore:
    return MongoLeaseStore(mongo_db, ttl_ms=TTL_MS)


def _expire(store: MongoLeaseStore) -> None:
    """Simulate the holder dying: its lease runs out (no sleeping in tests)."""
    store.leases.update_one({"datasetId": DATASET}, {"$set": {"expiresAt": EPOCH}})


def test_first_acquire_starts_above_every_token_in_the_tables(store: MongoLeaseStore) -> None:
    lease = store.acquire(DATASET, "writer-a", max_token_seen=0)
    assert lease is not None and lease.token == 1 and lease.owner == "writer-a"
    assert lease.expires_at > datetime.now(UTC)


def test_a_live_lease_blocks_another_owner(store: MongoLeaseStore) -> None:
    assert store.acquire(DATASET, "writer-a", 0) is not None
    assert store.acquire(DATASET, "writer-b", 0) is None


def test_an_expired_lease_is_taken_over_with_a_higher_token(store: MongoLeaseStore) -> None:
    first = store.acquire(DATASET, "writer-a", 0)
    assert first is not None
    _expire(store)
    second = store.acquire(DATASET, "writer-b", 0)
    assert second is not None and second.owner == "writer-b" and second.token == first.token + 1


def test_a_mongo_restore_cannot_lower_the_token_below_the_delta_logs(store: MongoLeaseStore) -> None:
    assert store.acquire(DATASET, "writer-a", 0) is not None  # stored token 1
    _expire(store)
    lease = store.acquire(DATASET, "writer-b", max_token_seen=9)  # the tables saw token 9
    assert lease is not None and lease.token == 10


def test_c29_an_acquire_computed_from_a_stale_read_matches_nothing(store: MongoLeaseStore) -> None:
    first = store.acquire(DATASET, "writer-a", 0)
    assert first is not None
    _expire(store)
    stale_read_token = first.token  # writer-c reads token 1 ...
    taken = store.acquire(DATASET, "writer-b", 0)  # ... writer-b takes over (token 2) ...
    assert taken is not None
    _expire(store)
    # ... then writer-c's delayed write, still expecting token 1, must not land.
    assert store.compare_and_set(DATASET, "writer-c", stale_read_token, stale_read_token + 1) is None
    row = store.leases.find_one({"datasetId": DATASET})
    assert row is not None and row["owner"] == "writer-b" and row["token"] == taken.token


def test_renew_extends_a_live_lease_and_fails_after_a_takeover(store: MongoLeaseStore) -> None:
    lease = store.acquire(DATASET, "writer-a", 0)
    assert lease is not None
    renewed = store.renew(lease)
    assert renewed is not None and renewed.expires_at >= lease.expires_at
    _expire(store)
    assert store.acquire(DATASET, "writer-b", 0) is not None
    assert store.renew(lease) is None


def test_release_lets_the_next_writer_in_at_once(store: MongoLeaseStore) -> None:
    lease = store.acquire(DATASET, "writer-a", 0)
    assert lease is not None
    store.release(lease)
    assert store.acquire(DATASET, "writer-b", 0) is not None


def test_c18_a_takeover_after_the_publish_snapshot_cannot_publish(
    mongo_client: MongoClient[Document], store: MongoLeaseStore
) -> None:
    lease = store.acquire(DATASET, "writer-a", 0)
    assert lease is not None
    with mongo_client.start_session() as session:
        session.start_transaction()
        assert store.leases.find_one({"datasetId": DATASET}, session=session) is not None  # snapshot taken
        _expire(store)
        assert store.acquire(DATASET, "writer-b", 0) is not None  # takeover commits outside
        try:
            published = store.guard_publish(session, lease, "op-1")
        except OperationFailure as conflict:  # write conflict — retried, see below
            assert conflict.has_error_label("TransientTransactionError")
            published = False
        session.abort_transaction()
    assert published is False
    with mongo_client.start_session() as session, session.start_transaction():
        assert store.guard_publish(session, lease, "op-1") is False  # the retry sees the new token


def test_c18_a_publish_in_flight_holds_off_the_takeover_until_it_commits(
    mongo_client: MongoClient[Document], store: MongoLeaseStore
) -> None:
    lease = store.acquire(DATASET, "writer-a", 0)
    assert lease is not None
    takeover_done_at: list[float] = []

    def take_over() -> None:
        _expire(store)  # blocks behind the open transaction's write to the same row
        if store.acquire(DATASET, "writer-b", 0) is not None:
            takeover_done_at.append(time.monotonic())

    with mongo_client.start_session() as session:
        session.start_transaction()
        assert store.guard_publish(session, lease, "op-1") is True
        thread = threading.Thread(target=take_over)
        thread.start()
        time.sleep(0.3)
        assert takeover_done_at == []  # cannot land while the publish is open
        published_at = time.monotonic()
        session.commit_transaction()
    thread.join(timeout=10)
    assert takeover_done_at and takeover_done_at[0] > published_at  # strictly after the publish
    row = store.leases.find_one({"datasetId": DATASET})
    assert row is not None and row["lastPublished"] == "op-1" and row["owner"] == "writer-b"
