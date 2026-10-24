"""A Delta job end to end: queued operation → writer (fenced commit, committed) → Node's publish
transaction (consistency.md §5–§6) — C15, the rebase rule, staleness, FENCED, FORM_DELETED."""

from collections.abc import Mapping
from dataclasses import replace

import pytest
from deltalake import DeltaTable
from pymongo.database import Database

from dataservice.modules.writer.service import WriterTables, run_append_rows
from dataservice.storage.fenced_commit import max_token_seen
from dataservice.storage.lease import Lease
from dataservice.storage.operations import JobResult, PublishRequest
from prototype.node_side.authority import NodeAuthority
from prototype.node_side.edit_store import Edit, EditRejected
from prototype.node_side.lease_store import EPOCH, MongoLeaseStore
from prototype.node_side.operation_store import MongoOperationStore
from prototype.node_side.publish_store import MongoPublishStore
from tests.integration.conftest import Document
from tests.integration.lake import DATASET, FORM, Lake, set_age

NEW_ROWS: Mapping[str, object] = {
    "rows": [{"_rid": "r50", "f_age": 61.0, "f_pid": "P-50"}, {"_rid": "r51", "f_age": 62.0, "f_pid": "P-51"}]
}


class Jobs:
    def __init__(self, lake: Lake, db: Database[Document]) -> None:
        self.lake, self.db = lake, db
        self.leases = MongoLeaseStore(db, ttl_ms=60_000)
        self.operations = MongoOperationStore(lake.client, db)
        self.publisher = MongoPublishStore(lake.client, db, self.leases)
        self.authority = NodeAuthority(self.operations, self.publisher)
        self.tables = WriterTables(lake.base_uri, {})

    def lease(self, owner: str = "writer-a") -> Lease:
        lease = self.leases.acquire(DATASET, owner, max_token_seen([DeltaTable(self.lake.base_uri)]))
        assert lease is not None
        return lease

    def submit(self, key: str = "import-1", payload: Mapping[str, object] = NEW_ROWS) -> str:
        op_id = self.operations.submit(DATASET, FORM, "user-a", key, "append_rows", payload)
        self.operations.dispatch(op_id)
        return op_id

    def commit_only(self, op_id: str, lease: Lease) -> PublishRequest:
        """The writer's steps up to `committed`, so a test can act before the publish."""
        captured: list[PublishRequest] = []

        class Capture(NodeAuthority):
            def publish(self, request: PublishRequest, lease: Lease) -> int:
                captured.append(request)
                return -1

        run_append_rows(
            op_id, self.lake.current_revision(), lease, self.tables, Capture(self.operations, self.publisher)
        )
        return captured[0]


@pytest.fixture
def jobs(lake: Lake, mongo_db: Database[Document]) -> Jobs:
    mongo_db["analyses"].insert_many(
        [
            {"formId": FORM, "name": "age table", "usedFieldIds": ["age"], "dataVersion": 0},
            {"formId": "form-2", "name": "other form", "usedFieldIds": ["age"], "dataVersion": 0},
        ]
    )
    return Jobs(lake, mongo_db)


def test_an_import_lands_as_one_published_revision(lake: Lake, jobs: Jobs) -> None:
    op_id = jobs.submit()
    revision = run_append_rows(op_id, lake.current_revision(), jobs.lease(), jobs.tables, jobs.authority)
    assert revision == 2 and lake.current_revision() == 2
    rows = lake.rows(revision)
    assert (rows["r50"]["f_age"], rows["r51"]["f_pid"]) == (61.0, "P-51") and len(rows) == 12
    assert len(lake.rows(1)) == 10  # the revision before the import is untouched
    op = jobs.operations.get(op_id)
    assert (op["status"], op["appliedRevision"]) == ("applied", 2)
    form = lake.db["forms"].find_one({"_id": FORM})
    assert form is not None and form["dataVersion"] == 2
    stale = {doc["name"]: doc["dataVersion"] for doc in lake.db["analyses"].find()}
    assert stale == {"age table": 1, "other form": 0}  # only this form's analyses on changed fields
    assert lake.db["dataset_events"].count_documents({"opId": op_id}) == 1


def test_c15_a_publish_retried_after_a_lost_response_publishes_once(lake: Lake, jobs: Jobs) -> None:
    lease = jobs.lease()
    request = jobs.commit_only(jobs.submit(), lease)
    first = jobs.publisher.publish(request, lease)
    again = jobs.publisher.publish(request, lease)  # the writer never saw the first answer
    assert first == again == 2
    assert lake.db["dataset_revisions"].count_documents({"datasetId": DATASET}) == 2


def test_a_lost_lease_cannot_publish_and_the_operation_stays_committed(lake: Lake, jobs: Jobs) -> None:
    lease = jobs.lease()
    request = jobs.commit_only(jobs.submit(), lease)
    jobs.leases.leases.update_one({"datasetId": DATASET}, {"$set": {"expiresAt": EPOCH}})
    jobs.lease("writer-b")  # taken over before the publish
    with pytest.raises(EditRejected) as refused:
        jobs.publisher.publish(request, lease)
    assert refused.value.code == "FENCED"
    assert jobs.operations.get(request.op_id)["status"] == "committed"  # left for roll-forward
    assert lake.current_revision() == 1


def test_small_edits_meanwhile_conflict_only_on_the_fields_the_job_rewrote(lake: Lake, jobs: Jobs) -> None:
    lease = jobs.lease()
    request = jobs.commit_only(jobs.submit(), lease)
    lake.edit("meanwhile", Edit(row="r00", op="set", field="pid", t="str", v="P-NEW"))
    rewrote_age = replace(request, result=JobResult(rows_changed=10, touched_field_ids=("age",)))
    rewrote_pid = replace(request, result=JobResult(rows_changed=10, touched_field_ids=("pid",)))
    with pytest.raises(EditRejected) as refused:
        jobs.publisher.publish(rewrote_pid, lease)  # the edit touched pid: re-prepare
    assert refused.value.code == "CONFLICT"
    revision = jobs.publisher.publish(rewrote_age, lease)  # pid edits do not clash with an age rewrite
    rows = lake.rows(revision)
    assert rows["r00"]["f_pid"] == "P-NEW" and "r50" in rows  # the edit stays on top of the job


def test_an_append_never_clashes_with_edits_made_meanwhile(lake: Lake, jobs: Jobs) -> None:
    lease = jobs.lease()
    request = jobs.commit_only(jobs.submit(), lease)
    lake.edit("meanwhile", set_age("r00", 31.0))
    revision = jobs.publisher.publish(request, lease)
    rows = lake.rows(revision)
    assert rows["r00"]["f_age"] == 31.0 and rows["r50"]["f_age"] == 61.0


def test_another_delta_job_published_since_the_base_is_a_conflict(lake: Lake, jobs: Jobs) -> None:
    lease = jobs.lease()
    stale = jobs.commit_only(jobs.submit("import-1"), lease)
    other = jobs.submit("import-2", {"rows": [{"_rid": "r60", "f_age": 70.0, "f_pid": "P-60"}]})
    run_append_rows(other, lake.current_revision(), lease, jobs.tables, jobs.authority)
    with pytest.raises(EditRejected) as refused:
        jobs.publisher.publish(stale, lease)
    assert refused.value.code == "CONFLICT"


def test_a_deleted_form_refuses_the_publish(lake: Lake, jobs: Jobs) -> None:
    lease = jobs.lease()
    request = jobs.commit_only(jobs.submit(), lease)
    lake.db["forms"].update_one({"_id": FORM}, {"$set": {"status": "inactive"}})
    with pytest.raises(EditRejected) as refused:
        jobs.publisher.publish(request, lease)
    assert refused.value.code == "FORM_DELETED"


def test_a_publish_for_another_attempt_is_refused(lake: Lake, jobs: Jobs) -> None:
    lease = jobs.lease()
    request = jobs.commit_only(jobs.submit(), lease)
    with pytest.raises(EditRejected) as refused:
        jobs.publisher.publish(replace(request, attempt=request.attempt + 1), lease)
    assert refused.value.code == "CONFLICT"


def test_a_bad_payload_fails_the_attempt_and_commits_nothing(lake: Lake, jobs: Jobs) -> None:
    op_id = jobs.submit("bad", {"rows": [{"_rid": "r70", "f_unknown": 1}]})
    before = DeltaTable(lake.base_uri).version()
    with pytest.raises(Exception, match="columns the dataset does not have"):
        run_append_rows(op_id, lake.current_revision(), jobs.lease(), jobs.tables, jobs.authority)
    op = jobs.operations.get(op_id)
    assert op["status"] == "failed" and DeltaTable(lake.base_uri).version() == before
