"""The entry quota (consistency.md §7, §14.2) on a real Mongo replica set — C7 and every way a
reservation is made, settled, released, swept and renewed; small edits charge directly."""

import threading
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta

import pytest
from deltalake import DeltaTable
from pymongo.database import Database

from dataservice.modules.writer.service import WriterTables, drain, run_append_rows
from dataservice.storage.fenced_commit import max_token_seen
from dataservice.storage.lease import Lease
from dataservice.storage.operations import Commit, PublishRequest
from prototype.node_side.authority import NodeAuthority
from prototype.node_side.edit_store import Edit, EditRejected
from prototype.node_side.lease_store import MongoLeaseStore
from prototype.node_side.operation_store import MongoOperationStore
from prototype.node_side.publish_store import MongoPublishStore
from tests.integration.conftest import Document
from tests.integration.lake import DATASET, FORM, OWNER, ROWS, Lake, set_age

LIMIT = 15  # the base already holds 10 rows: 5 entries of room


def _rows(*ids: int) -> Mapping[str, object]:
    return {"rows": [{"_rid": f"r{n}", "f_age": float(n), "f_pid": f"P-{n}"} for n in ids]}


class Quota:
    def __init__(self, lake: Lake, db: Database[Document]) -> None:
        self.lake, self.db = lake, db
        db["payments"].update_one({"userId": OWNER}, {"$set": {"entries_limit": LIMIT}})
        self.leases = MongoLeaseStore(db, ttl_ms=60_000)
        self.operations = MongoOperationStore(lake.client, db, 3, lake.quota, timedelta(minutes=10))
        self.publisher = MongoPublishStore(lake.client, db, self.leases, lake.quota)
        self.authority = NodeAuthority(self.operations, self.publisher, self.leases)
        self.tables = WriterTables({"base": lake.base_uri})

    def lease(self) -> Lease:
        lease = self.leases.acquire(DATASET, "writer", max_token_seen([DeltaTable(self.lake.base_uri)]))
        assert lease is not None
        return lease

    def submit(self, key: str, *ids: int, entries: int | None = None) -> str:
        count = len(ids) if entries is None else entries
        op_id = self.operations.submit(DATASET, FORM, "user-a", key, "append_rows", _rows(*ids), count)
        self.operations.dispatch(op_id)
        return op_id

    def reservation(self, op_id: str) -> Document:
        found = self.db["entry_reservations"].find_one({"opId": op_id})
        assert found is not None
        return found


@pytest.fixture
def quota(lake: Lake, mongo_db: Database[Document]) -> Quota:
    return Quota(lake, mongo_db)


def test_an_import_reserves_its_rows_with_the_operation(lake: Lake, quota: Quota) -> None:
    op_id = quota.submit("a", 50, 51, 52)
    assert lake.quota_counters() == (10, 3)
    assert quota.reservation(op_id)["status"] == "held"


def test_no_room_means_no_operation_either(lake: Lake, quota: Quota) -> None:
    with pytest.raises(EditRejected) as refused:
        quota.submit("a", *range(50, 56))  # 6 > 5 left
    assert refused.value.code == "QUOTA_EXCEEDED"
    assert quota.db["data_operations"].count_documents({}) == 0  # the insert rolled back with it
    assert lake.quota_counters() == (10, 0)


def test_two_imports_racing_for_the_last_room_cannot_both_reserve(lake: Lake, quota: Quota) -> None:
    outcomes: list[str] = []

    def submit(key: str) -> None:
        try:
            quota.submit(key, 60, 61, 62, 63)  # 4 each, 5 left
            outcomes.append("reserved")
        except EditRejected as rejected:
            outcomes.append(rejected.code)

    threads = [threading.Thread(target=submit, args=(key,)) for key in ("a", "b")]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert sorted(outcomes) == ["QUOTA_EXCEEDED", "reserved"]
    used, reserved = lake.quota_counters()
    assert used + reserved <= LIMIT


def test_c7_an_expired_reservation_of_a_live_import_still_holds_its_room(lake: Lake, quota: Quota) -> None:
    long_import = quota.submit("long", 50, 51, 52, 53)  # 4 of the 5 left
    quota.db["entry_reservations"].update_one({"opId": long_import}, {"$set": {"expiresAt": datetime(2020, 1, 1)}})
    assert lake.quota.sweep(datetime.now(UTC)) == []  # still queued: never released by expiry alone
    with pytest.raises(EditRejected) as refused:
        quota.submit("second", 60, 61)  # needs 2, only 1 left
    assert refused.value.code == "QUOTA_EXCEEDED"
    drain(DATASET, quota.lease(), quota.tables, quota.authority)
    used, reserved = lake.quota_counters()
    assert (used, reserved) == (14, 0) and used <= LIMIT


def test_the_publish_settles_reserved_into_used(lake: Lake, quota: Quota) -> None:
    op_id = quota.submit("a", 50, 51)
    run_append_rows(DATASET, op_id, quota.lease(), quota.tables, quota.authority)
    assert lake.quota_counters() == (12, 0)
    assert quota.reservation(op_id)["status"] == "settled"


def test_a_retried_publish_settles_only_once(lake: Lake, quota: Quota) -> None:
    op_id, lease = quota.submit("a", 50, 51), quota.lease()
    run_append_rows(DATASET, op_id, lease, quota.tables, quota.authority)
    op = quota.operations.get(op_id)
    commit = Commit(1, {"base": 1})
    quota.publisher.publish(PublishRequest(op_id, int(str(op["attempt"])), commit), lease)  # the lost-response retry
    assert lake.quota_counters() == (12, 0)


def test_a_final_failure_and_a_cancel_give_the_room_back(lake: Lake, quota: Quota) -> None:
    bad = quota.operations.submit(
        DATASET, FORM, "user-a", "bad", "append_rows", {"rows": [{"_rid": "r9", "f_x": 1}]}, 1
    )
    quota.operations.dispatch(bad)
    cancelled = quota.submit("c", 60, 61)
    assert lake.quota_counters() == (10, 3)
    quota.operations.cancel(cancelled)
    drain(DATASET, quota.lease(), quota.tables, quota.authority)  # `bad` fails at once
    assert lake.quota_counters() == (10, 0)
    assert {quota.reservation(bad)["status"], quota.reservation(cancelled)["status"]} == {"released"}


def test_the_sweeper_releases_only_for_finished_or_vanished_operations(lake: Lake, quota: Quota) -> None:
    finished, queued = quota.submit("a", 50), quota.submit("b", 51)
    quota.db["data_operations"].update_one({"opId": finished}, {"$set": {"status": "failed"}})  # release missed
    assert lake.quota.sweep(datetime.now(UTC)) == [finished]
    assert quota.reservation(queued)["status"] == "held"
    assert lake.quota_counters() == (10, 1)


def test_the_heartbeat_renews_every_unfinished_operations_reservation(lake: Lake, quota: Quota) -> None:
    first, second = quota.submit("a", 50), quota.submit("b", 51)
    until = datetime.now(UTC) + timedelta(hours=2)
    assert lake.quota.renew(DATASET, until) == 2
    for op_id in (first, second):
        expires_at = quota.reservation(op_id)["expiresAt"]
        assert isinstance(expires_at, datetime) and expires_at.replace(tzinfo=UTC) > until - timedelta(seconds=1)


def test_a_released_reservation_is_revalidated_at_publish(lake: Lake, quota: Quota) -> None:
    op_id, lease = quota.submit("a", 50, 51, 52, 53, 54), quota.lease()  # all 5 left
    captured: list[PublishRequest] = []

    class Capture(NodeAuthority):
        def publish(self, request: PublishRequest, lease: Lease) -> int:
            captured.append(request)
            return -1

    run_append_rows(DATASET, op_id, lease, quota.tables, Capture(quota.operations, quota.publisher, quota.leases))
    lake.quota.release(op_id)  # e.g. a sweeper bug released it while committed
    lake.edit("fill", Edit(row="r90", op="insert_row"))  # a small edit takes one entry meanwhile
    with pytest.raises(EditRejected) as refused:
        quota.publisher.publish(captured[0], lease)
    assert refused.value.code == "QUOTA_EXCEEDED"
    used, reserved = lake.quota_counters()
    assert (used, reserved) == (11, 0) and used <= LIMIT


def test_small_edits_charge_inserts_and_restores_and_credit_deletes(lake: Lake, quota: Quota) -> None:
    lake.edit("add", Edit(row="r90", op="insert_row"), set_age("r90", 1.0))
    assert lake.quota_counters() == (11, 0)
    lake.edit("del", Edit(row=ROWS[0], op="delete_row"))
    assert lake.quota_counters() == (10, 0)
    lake.edit("bounce", Edit(row=ROWS[1], op="delete_row"), Edit(row=ROWS[1], op="restore_row"))
    lake.edit("again", Edit(row="r90", op="insert_row"))  # inserting a row that exists adds nothing
    assert lake.quota_counters() == (10, 0)


def test_a_small_edit_over_the_limit_writes_nothing(lake: Lake, quota: Quota) -> None:
    lake.edit("fill", *[Edit(row=f"r9{n}", op="insert_row") for n in range(5)])  # the 5 left
    before = lake.db["dataset_patches"].count_documents({})
    with pytest.raises(EditRejected) as refused:
        lake.edit("one-more", Edit(row="r99", op="insert_row"), set_age("r00", 77.0))
    assert refused.value.code == "QUOTA_EXCEEDED"
    assert lake.db["dataset_patches"].count_documents({}) == before and lake.rows()["r00"]["f_age"] == 30.0
    assert lake.quota_counters() == (LIMIT, 0)


def test_a_late_duplicate_running_call_never_frees_a_committed_operations_room(lake: Lake, quota: Quota) -> None:
    """Review finding: on its last attempt, a delayed duplicate `running` that arrived after
    `committed` released the reservation through the poison path (its CAS matched nothing)."""
    op_id, lease = quota.submit("a", 50, 51), quota.lease()
    for _ in range(2):  # attempts 1 and 2 fail and go back to the queue
        attempt = quota.operations.running(op_id, lease.token)
        quota.operations.failed(op_id, lease.token, attempt, {"code": "X", "message": "flaky"}, retryable=True)
    captured: list[PublishRequest] = []

    class Capture(NodeAuthority):
        def publish(self, request: PublishRequest, lease: Lease) -> int:
            captured.append(request)
            return -1

    run_append_rows(DATASET, op_id, lease, quota.tables, Capture(quota.operations, quota.publisher, quota.leases))
    assert quota.operations.get(op_id)["status"] == "committed" and captured[0].attempt == 3
    with pytest.raises(EditRejected):
        quota.operations.running(op_id, lease.token)  # the delayed duplicate of attempt 3's running
    assert quota.reservation(op_id)["status"] == "held"
    assert quota.publisher.publish(captured[0], lease) == 2
    assert lake.quota_counters() == (12, 0)
