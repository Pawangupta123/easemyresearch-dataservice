"""The writer drains the queue in `seq` order (consistency.md §4.4) — C5 (a lost wake-up is swept),
C6 (a poison operation fails after its attempts and the queue moves on), order and blocking."""

from collections.abc import Mapping
from datetime import UTC, datetime, timedelta

import pyarrow as pa
import pytest
from deltalake import DeltaTable
from pymongo.database import Database

from dataservice.common.errors import FenceError
from dataservice.modules.writer.service import WriterTables, drain
from dataservice.storage.fenced_commit import max_token_seen
from dataservice.storage.lease import Lease
from dataservice.storage.operations import Commit
from prototype.node_side.authority import NodeAuthority
from prototype.node_side.lease_store import EPOCH, MongoLeaseStore
from prototype.node_side.operation_store import MongoOperationStore
from prototype.node_side.publish_store import MongoPublishStore
from tests.integration.conftest import Document
from tests.integration.lake import DATASET, FORM, Lake

MAX_ATTEMPTS = 3


def _rows(*ids: int) -> Mapping[str, object]:
    return {"rows": [{"_rid": f"r{n}", "f_age": float(n), "f_pid": f"P-{n}"} for n in ids]}


class FlakyAuthority(NodeAuthority):
    """Node's door, except that `committed` for one operation always fails (a network error after
    the Delta commit) — the attempt must be rolled back every time."""

    flaky: str = ""

    def committed(self, op_id: str, token: int, attempt: int, commit: Commit) -> None:
        if op_id == self.flaky:
            raise RuntimeError("network error talking to Node")
        super().committed(op_id, token, attempt, commit)


class Queue:
    def __init__(self, lake: Lake, db: Database[Document]) -> None:
        self.lake = lake
        self.leases = MongoLeaseStore(db, ttl_ms=60_000)
        self.operations = MongoOperationStore(lake.client, db, MAX_ATTEMPTS, lake.quota, timedelta(minutes=10))
        publisher = MongoPublishStore(lake.client, db, self.leases, lake.quota)
        self.authority = FlakyAuthority(self.operations, publisher, self.leases)
        self.tables = WriterTables({"base": lake.base_uri})

    def lease(self) -> Lease:
        lease = self.leases.acquire(DATASET, "writer", max_token_seen([DeltaTable(self.lake.base_uri)]))
        assert lease is not None
        return lease

    def submit(self, key: str, payload: Mapping[str, object], *, dispatch: bool = True) -> str:
        rows = payload.get("rows")
        entries = len(rows) if isinstance(rows, list) else 0
        op_id = self.operations.submit(DATASET, FORM, "user-a", key, "append_rows", payload, entries)
        if dispatch:
            self.operations.dispatch(op_id)
        return op_id

    def base_rows(self) -> int:
        actions = pa.table(DeltaTable(self.lake.base_uri).get_add_actions(flatten=True))
        return int(sum(actions.column("num_records").to_pylist()))


@pytest.fixture
def queue(lake: Lake, mongo_db: Database[Document]) -> Queue:
    return Queue(lake, mongo_db)


def test_operations_run_once_each_in_seq_order(lake: Lake, queue: Queue) -> None:
    first, second = queue.submit("a", _rows(50)), queue.submit("b", _rows(51, 52))
    result = drain(DATASET, queue.lease(), queue.tables, queue.authority)
    assert result.applied == {first: 2, second: 3} and not result.failed
    assert len(lake.rows()) == 13
    again = drain(DATASET, queue.lease(), queue.tables, queue.authority)
    assert not again.applied and lake.current_revision() == 3  # nothing runs twice


def test_c5_an_operation_whose_wake_up_was_lost_runs_after_the_sweep(lake: Lake, queue: Queue) -> None:
    op_id = queue.submit("a", _rows(50), dispatch=False)  # Node crashed between insert and wake-up
    assert not drain(DATASET, queue.lease(), queue.tables, queue.authority).applied
    woken = queue.operations.sweep(datetime.now(UTC) + timedelta(minutes=10), timedelta(minutes=5))
    assert woken == [DATASET]
    assert drain(DATASET, queue.lease(), queue.tables, queue.authority).applied == {op_id: 2}
    assert len(lake.rows()) == 11


def test_nothing_behind_a_head_that_is_not_ready_runs_first(lake: Lake, queue: Queue) -> None:
    queue.submit("a", _rows(50), dispatch=False)  # the head: not dispatched yet
    behind = queue.submit("b", _rows(51))
    assert not drain(DATASET, queue.lease(), queue.tables, queue.authority).applied
    assert queue.operations.get(behind)["status"] == "dispatched" and lake.current_revision() == 1


def test_c6_a_poison_operation_fails_after_its_attempts_and_the_queue_moves_on(lake: Lake, queue: Queue) -> None:
    poison, behind = queue.submit("a", _rows(50)), queue.submit("b", _rows(51))
    queue.authority.flaky = poison
    result = drain(DATASET, queue.lease(), queue.tables, queue.authority)
    assert result.failed == [poison] and result.applied == {behind: 2}
    op = queue.operations.get(poison)
    assert (op["status"], op["attempt"], op["rolledBackAttempts"]) == ("failed", MAX_ATTEMPTS, [1, 2])
    assert queue.base_rows() == 11  # every poison attempt was rolled back; only `behind` landed
    rows = lake.rows()
    assert "r51" in rows and "r50" not in rows
    form = lake.db["forms"].find_one({"_id": FORM})
    assert form is not None and form["dataVersion"] == 2  # one revision, nothing regressed


def test_a_wrong_payload_fails_at_once_without_retries(lake: Lake, queue: Queue) -> None:
    bad, good = queue.submit("a", {"rows": [{"_rid": "r9", "f_nope": 1}]}), queue.submit("b", _rows(51))
    result = drain(DATASET, queue.lease(), queue.tables, queue.authority)
    assert result.failed == [bad] and result.applied == {good: 2}
    assert queue.operations.get(bad)["attempt"] == 1


def test_a_lost_lease_stops_the_drain_and_leaves_the_operation_alone(lake: Lake, queue: Queue) -> None:
    lease = queue.lease()
    op_id = queue.submit("a", _rows(50))
    queue.leases.leases.update_one({"datasetId": DATASET}, {"$set": {"expiresAt": EPOCH}})  # lost
    with pytest.raises(FenceError):
        drain(DATASET, lease, queue.tables, queue.authority)
    assert queue.operations.get(op_id)["status"] == "dispatched"  # the next holder will run it


def test_an_operation_abandoned_before_its_first_commit_is_taken_over(lake: Lake, queue: Queue) -> None:
    """Review finding: a writer that died after `running` but before any Delta commit left the
    operation `running` forever — never re-run, its reservation held, the queue stuck."""
    op_id = queue.submit("a", _rows(50, 51))
    writer_a = queue.lease()
    assert queue.authority.running(op_id, writer_a.token) == 1  # ... then writer A dies
    queue.leases.leases.update_one({"datasetId": DATASET}, {"$set": {"expiresAt": EPOCH}})
    writer_b = queue.lease()
    result = drain(DATASET, writer_b, queue.tables, queue.authority)
    assert result.applied == {op_id: 2}
    op = queue.operations.get(op_id)
    assert (op["status"], op["attempt"]) == ("applied", 2)
    assert lake.quota_counters() == (12, 0)
