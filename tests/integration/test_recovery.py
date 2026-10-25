"""A writer dies mid-job; the next lease holder recovers (consistency.md §4.3, §5.3) — C3 on the
full path (Delta + Node), C20 (a rolled-back attempt never counts), and a stale writer waking up."""

from datetime import timedelta

import pyarrow as pa
import pytest
from deltalake import DeltaTable
from pymongo.database import Database

from dataservice.common.errors import FenceError
from dataservice.modules.writer.service import WriterTables, recover, run_append_rows
from dataservice.storage.fenced_commit import CommitContext, OpRef, append, max_token_seen
from dataservice.storage.lease import Lease
from dataservice.storage.operations import Commit, JobResult, PublishRequest
from prototype.node_side.authority import NodeAuthority
from prototype.node_side.edit_store import EditRejected
from prototype.node_side.lease_store import EPOCH, MongoLeaseStore
from prototype.node_side.operation_store import MongoOperationStore
from prototype.node_side.publish_store import MongoPublishStore
from tests.integration.conftest import Document
from tests.integration.lake import DATASET, FORM, Lake

ROWS = {"rows": [{"_rid": "r50", "f_age": 61.0, "f_pid": "P-50"}, {"_rid": "r51", "f_age": 62.0, "f_pid": "P-51"}]}


class Cluster:
    def __init__(self, lake: Lake, db: Database[Document]) -> None:
        self.lake = lake
        self.leases = MongoLeaseStore(db, ttl_ms=60_000)
        self.operations = MongoOperationStore(lake.client, db, 3, lake.quota, timedelta(minutes=10))
        publisher = MongoPublishStore(lake.client, db, self.leases, lake.quota)
        self.authority = NodeAuthority(self.operations, publisher, self.leases)
        self.tables = WriterTables({"base": lake.base_uri})

    def lease(self, owner: str) -> Lease:
        lease = self.leases.acquire(DATASET, owner, max_token_seen([DeltaTable(self.lake.base_uri)]))
        assert lease is not None
        return lease

    def take_over(self, owner: str) -> Lease:
        self.leases.leases.update_one({"datasetId": DATASET}, {"$set": {"expiresAt": EPOCH}})  # the holder died
        return self.lease(owner)

    def submit(self) -> str:
        op_id = self.operations.submit(DATASET, FORM, "user-a", "import-1", "append_rows", ROWS, 2)
        self.operations.dispatch(op_id)
        return op_id

    def crash_after_commit(self, op_id: str, lease: Lease, *, mark_committed: bool) -> tuple[int, Commit]:
        """Writer A's steps, stopping before the publish (and before `committed` unless asked)."""
        base_revision = self.authority.manifest(DATASET).revision
        attempt = self.authority.running(op_id, lease.token)
        table = DeltaTable(self.lake.base_uri)
        rows = pa.Table.from_pylist(ROWS["rows"], schema=pa.schema(table.schema().to_arrow()))
        append(table, rows, CommitContext(lease.token, (OpRef(op_id, attempt),), ("base",)))
        commit = Commit(base_revision, {"base": DeltaTable(self.lake.base_uri).version()}, JobResult(rows_added=2))
        if mark_committed:
            self.authority.committed(op_id, lease.token, attempt, commit)
        return attempt, commit


@pytest.fixture
def cluster(lake: Lake, mongo_db: Database[Document]) -> Cluster:
    return Cluster(lake, mongo_db)


def _base_rows(lake: Lake) -> int:
    actions = pa.table(DeltaTable(lake.base_uri).get_add_actions(flatten=True))
    return int(sum(actions.column("num_records").to_pylist()))


def test_c3_an_uncommitted_attempt_is_rolled_back_and_retried_once(lake: Lake, cluster: Cluster) -> None:
    op_id = cluster.submit()
    first_attempt, _ = cluster.crash_after_commit(op_id, cluster.lease("writer-a"), mark_committed=False)
    assert _base_rows(lake) == 12  # writer A's rows are in Delta, unpublished

    writer_b = cluster.take_over("writer-b")
    recovery = recover(DATASET, writer_b, cluster.tables, cluster.authority)
    assert recovery.rolled_back == (OpRef(op_id, first_attempt),) and not recovery.rolled_forward
    assert _base_rows(lake) == 10  # A's commit is gone
    op = cluster.operations.get(op_id)
    assert (op["status"], op["rolledBackAttempts"]) == ("dispatched", [first_attempt])

    revision = run_append_rows(DATASET, op_id, writer_b, cluster.tables, cluster.authority)  # attempt 2
    rows = lake.rows(revision)
    assert len(rows) == 12 and _base_rows(lake) == 12  # the rows landed exactly once (C20)
    assert cluster.operations.get(op_id)["attempt"] == first_attempt + 1


def test_c3_a_committed_operation_is_rolled_forward_without_running_again(lake: Lake, cluster: Cluster) -> None:
    op_id = cluster.submit()
    attempt, _ = cluster.crash_after_commit(op_id, cluster.lease("writer-a"), mark_committed=True)

    recovery = recover(DATASET, cluster.take_over("writer-b"), cluster.tables, cluster.authority)
    assert recovery.rolled_forward == {op_id: 2} and not recovery.rolled_back
    op = cluster.operations.get(op_id)
    assert (op["status"], op["attempt"], op["appliedRevision"]) == ("applied", attempt, 2)  # never re-run
    assert len(lake.rows(2)) == 12 and _base_rows(lake) == 12


def test_a_stale_writer_that_wakes_up_can_neither_write_nor_publish(lake: Lake, cluster: Cluster) -> None:
    op_id = cluster.submit()
    writer_a = cluster.lease("writer-a")
    attempt, commit = cluster.crash_after_commit(op_id, writer_a, mark_committed=True)
    recover(DATASET, cluster.take_over("writer-b"), cluster.tables, cluster.authority)

    rows = pa.Table.from_pylist([{"_rid": "r77"}], schema=pa.schema(DeltaTable(lake.base_uri).schema().to_arrow()))
    with pytest.raises(FenceError):  # every table was fenced by writer B
        append(DeltaTable(lake.base_uri), rows, CommitContext(writer_a.token, (OpRef(op_id, attempt),), ("base",)))
    # Its own operation was rolled forward by B: a late retry of its publish just reads the result.
    assert cluster.authority.publish(PublishRequest(op_id, attempt, commit), writer_a) == 2

    other = cluster.operations.submit(DATASET, FORM, "user-a", "import-2", "append_rows", ROWS, 2)
    cluster.operations.dispatch(other)
    with pytest.raises(FenceError):  # Node refuses its token before any work
        run_append_rows(DATASET, other, writer_a, cluster.tables, cluster.authority)
    assert cluster.operations.get(other)["status"] == "dispatched"  # untouched, writer B will run it
    with pytest.raises(EditRejected) as refused:  # nor publish with its lost lease
        cluster.authority.publish(PublishRequest(other, 1, commit), writer_a)
    assert refused.value.code == "FENCED"
    assert lake.current_revision() == 2


def test_recovery_with_nothing_to_do_changes_nothing(lake: Lake, cluster: Cluster) -> None:
    op_id = cluster.submit()
    run_append_rows(DATASET, op_id, cluster.lease("writer-a"), cluster.tables, cluster.authority)
    recovery = recover(DATASET, cluster.take_over("writer-b"), cluster.tables, cluster.authority)
    assert not recovery.rolled_back and not recovery.rolled_forward
    assert len(lake.rows()) == 12 and lake.current_revision() == 2
