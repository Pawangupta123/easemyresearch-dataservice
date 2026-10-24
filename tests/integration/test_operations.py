"""The Delta-job operation queue (consistency.md §5) on a real Mongo replica set — C28, C31 for
jobs, C32, and every row of the status-machine table."""

import pytest
from pymongo import MongoClient
from pymongo.database import Database

from prototype.node_side.edit_store import EditRejected
from prototype.node_side.operation_store import MongoOperationStore
from tests.integration.conftest import Document

DATASET, FORM = "ds-1", "form-1"
PAYLOAD = {"rows": [{"_rid": "r1", "f_age": 31.0}]}
COMMIT: Document = {"baseRevision": 1, "tableVersions": {"base": 3}}  # what a later holder would publish


@pytest.fixture
def ops(mongo_client: MongoClient[Document], mongo_db: Database[Document]) -> MongoOperationStore:
    mongo_db["datasets"].insert_one({"_id": DATASET, "opSeq": 0})
    return MongoOperationStore(mongo_client, mongo_db)


def _running(ops: MongoOperationStore, token: int = 1) -> tuple[str, int]:
    op_id = ops.submit(DATASET, FORM, "user-a", "k1", "append_rows", PAYLOAD)
    ops.dispatch(op_id)
    return op_id, ops.running(op_id, token)


def test_c31_a_retried_submit_is_the_same_operation_and_a_reused_key_is_refused(ops: MongoOperationStore) -> None:
    first = ops.submit(DATASET, FORM, "user-a", "k1", "append_rows", PAYLOAD)
    assert ops.submit(DATASET, FORM, "user-a", "k1", "append_rows", PAYLOAD) == first
    second = ops.submit(DATASET, FORM, "user-a", "k2", "append_rows", PAYLOAD)
    assert (ops.get(first)["seq"], ops.get(second)["seq"]) == (1, 2)
    with pytest.raises(EditRejected) as refused:
        ops.submit(DATASET, FORM, "user-a", "k1", "append_rows", {"rows": []})
    assert refused.value.code == "IDEMPOTENCY_MISMATCH"


def test_running_counts_attempts_and_a_duplicate_call_does_not(ops: MongoOperationStore) -> None:
    op_id, attempt = _running(ops, token=1)
    assert attempt == 1
    assert ops.running(op_id, 1) == 1  # the same call again: no increment
    assert ops.running(op_id, 2) == 2  # a new lease holder takes over as attempt 2
    with pytest.raises(EditRejected):
        ops.running(op_id, 1)  # the old holder cannot come back
    assert ops.get(op_id)["token"] == 2


def test_only_a_pending_operation_is_dispatched_and_only_a_dispatched_one_runs(ops: MongoOperationStore) -> None:
    op_id = ops.submit(DATASET, FORM, "user-a", "k1", "append_rows", PAYLOAD)
    with pytest.raises(EditRejected):
        ops.running(op_id, 1)  # not dispatched yet
    ops.dispatch(op_id)
    with pytest.raises(EditRejected):
        ops.dispatch(op_id)


def test_committed_needs_the_running_token_and_attempt_and_repeats_safely(ops: MongoOperationStore) -> None:
    op_id, attempt = _running(ops, token=1)
    with pytest.raises(EditRejected):
        ops.committed(op_id, 1, attempt + 1, {"base": 3}, COMMIT)
    with pytest.raises(EditRejected):
        ops.committed(op_id, 9, attempt, {"base": 3}, COMMIT)
    ops.committed(op_id, 1, attempt, {"base": 3}, COMMIT)
    ops.committed(op_id, 1, attempt, {"base": 3}, COMMIT)  # the same call again is fine
    op = ops.get(op_id)
    assert (op["status"], op["committedTableVersions"]) == ("committed", {"base": 3})


def test_c28_late_callbacks_from_an_older_attempt_change_nothing(ops: MongoOperationStore) -> None:
    op_id, first = _running(ops, token=1)
    second = ops.running(op_id, 2)  # the writer was replaced; attempt 2 runs
    with pytest.raises(EditRejected):
        ops.committed(op_id, 1, first, {"base": 3}, COMMIT)  # attempt 1's delayed committed
    with pytest.raises(EditRejected):
        ops.failed(op_id, 1, first, {"code": "X", "message": "late"}, rolled_back=False)
    op = ops.get(op_id)
    assert (op["status"], op["attempt"], op["token"]) == ("running", second, 2)


def test_a_rolled_back_attempt_is_remembered(ops: MongoOperationStore) -> None:
    op_id, attempt = _running(ops, token=1)
    ops.failed(op_id, 1, attempt, {"code": "OPERATION_FAILED", "message": "retrying"}, rolled_back=True)
    op = ops.get(op_id)
    assert (op["status"], op["rolledBackAttempts"]) == ("rolled_back", [attempt])


def test_c32_cancel_wins_before_committed_and_is_too_late_after(ops: MongoOperationStore) -> None:
    op_id, attempt = _running(ops, token=1)
    ops.cancel(op_id)
    ops.cancel(op_id)  # already cancelled: fine
    with pytest.raises(EditRejected):
        ops.committed(op_id, 1, attempt, {"base": 3}, COMMIT)  # the writer must roll back instead
    with pytest.raises(EditRejected):
        ops.running(op_id, 2)

    other = ops.submit(DATASET, FORM, "user-a", "k2", "append_rows", PAYLOAD)
    ops.dispatch(other)
    ops.committed(other, 1, ops.running(other, 1), {"base": 4}, COMMIT)
    with pytest.raises(EditRejected) as late:
        ops.cancel(other)
    assert late.value.code == "TOO_LATE"
    assert ops.get(other)["status"] == "committed"
