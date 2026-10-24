"""Compaction folds patches into the Delta base without changing a single value (consistency.md
§15) — C39, C41, C42, edits during compaction, the rebase refusal and unique claims."""

from datetime import UTC, datetime, timedelta

import pytest
from deltalake import DeltaTable
from pymongo.database import Database

from dataservice.common.errors import ConflictError
from dataservice.storage.compaction import compact
from dataservice.storage.fenced_commit import CommitContext, OpRef, max_token_seen
from dataservice.storage.lease import Lease
from prototype.node_side.compaction_store import MongoCompactionStore
from prototype.node_side.edit_store import Edit, EditRejected
from prototype.node_side.lease_store import MongoLeaseStore
from tests.integration.conftest import Document
from tests.integration.lake import DATASET, FORM, Lake, set_age


class Compactor:
    def __init__(self, lake: Lake, db: Database[Document]) -> None:
        self.lake = lake
        self.leases = MongoLeaseStore(db, ttl_ms=60_000)
        self.store = MongoCompactionStore(lake.client, db, self.leases)

    def lease(self) -> Lease:
        lease = self.leases.acquire(DATASET, "compactor", max_token_seen([DeltaTable(self.lake.base_uri)]))
        assert lease is not None
        return lease

    def prepare(self, base_revision: int, lease: Lease) -> tuple[int, int]:
        """Data Service side: fold the base revision's patches into a new base."""
        context = CommitContext(lease.token, (OpRef(f"compact-{base_revision}", 1),), ("base",))
        result = compact(self.lake.snapshot(base_revision), self.lake.patches, context)
        return result.base_version, result.folded_to

    def run(self) -> int:
        base_revision, lease = self.lake.current_revision(), self.lease()
        base_version, folded_to = self.prepare(base_revision, lease)
        return self.store.publish(DATASET, lease, base_revision, base_version, folded_to)


@pytest.fixture
def compactor(lake: Lake, mongo_db: Database[Document]) -> Compactor:
    return Compactor(lake, mongo_db)


def _edit_everything(lake: Lake) -> None:
    lake.edit("set", set_age("r00", 31.0))
    lake.edit("clear", Edit(row="r01", op="set", field="age", t="null"))
    lake.edit("pid", Edit(row="r02", op="set", field="pid", t="str", v="P-X"))
    lake.edit("insert", Edit(row="r99", op="insert_row"), set_age("r99", 12.0))
    lake.edit("delete", Edit(row="r03", op="delete_row"))
    lake.edit("bounce", Edit(row="r04", op="delete_row"), Edit(row="r04", op="restore_row"))


def test_c39_every_value_and_row_state_is_identical_after_compaction(lake: Lake, compactor: Compactor) -> None:
    _edit_everything(lake)
    before_revision = lake.current_revision()
    before = lake.rows(include_deleted=True)
    form_version = lake.db["forms"].find_one({"_id": FORM})
    assert form_version is not None

    after_revision = compactor.run()

    assert after_revision == before_revision + 1
    assert lake.rows(after_revision, include_deleted=True) == before
    assert lake.rows(before_revision, include_deleted=True) == before  # the old revision still reads the same
    assert lake.manifest(after_revision)["dataVersion"] == lake.manifest(before_revision)["dataVersion"]
    assert lake.db["forms"].find_one({"_id": FORM}) == form_version  # Form.dataVersion unchanged
    assert before["r01"]["f_age"] is None and before["r99"]["f_age"] == 12.0 and before["r04"]["_deleted_at"] is None


def test_c42_the_folded_base_alone_holds_inserted_deleted_and_trashed_rows(lake: Lake, compactor: Compactor) -> None:
    _edit_everything(lake)
    deleted = lake.db["dataset_patches"].find_one({"op": "delete_row", "target.row": "r03"})
    assert deleted is not None
    revision = compactor.run()
    snapshot = lake.snapshot(revision)
    assert snapshot.patch_from == snapshot.patch_to  # no patch left to apply: the base alone answers
    rows = lake.rows(revision, include_deleted=True)
    assert rows["r99"]["f_age"] == 12.0 and "r03" not in lake.rows(revision)
    deleted_at = rows["r03"]["_deleted_at"]
    assert isinstance(deleted_at, datetime) and isinstance(deleted["at"], datetime)
    assert abs(deleted_at - deleted["at"].replace(tzinfo=UTC)) < timedelta(milliseconds=1)  # trash window kept


def test_edits_made_during_a_compaction_stay_on_top(lake: Lake, compactor: Compactor) -> None:
    lake.edit("first", set_age("r00", 31.0))
    base_revision, lease = lake.current_revision(), compactor.lease()
    base_version, folded_to = compactor.prepare(base_revision, lease)
    lake.edit("during", set_age("r00", 99.0), set_age("r05", 55.0))  # lands while compaction runs
    revision = compactor.store.publish(DATASET, lease, base_revision, base_version, folded_to)
    rows = lake.rows(revision)
    assert (rows["r00"]["f_age"], rows["r05"]["f_age"]) == (99.0, 55.0)
    assert lake.snapshot(revision).patch_from == folded_to


def test_a_compaction_on_a_stale_base_is_refused_on_both_sides(lake: Lake, compactor: Compactor) -> None:
    lake.edit("first", set_age("r00", 31.0))
    stale_revision = lake.current_revision()
    compactor.run()  # another base rewrite lands first
    lease = compactor.lease()
    with pytest.raises(ConflictError):  # Data Service: the base got a data commit since
        compactor.prepare(stale_revision, lease)
    with pytest.raises(EditRejected) as refused:  # Node: a non-edit revision landed since
        compactor.store.publish(DATASET, lease, stale_revision, 1, 1)
    assert refused.value.code == "CONFLICT"


def test_c41_patches_stay_while_a_pinned_revision_needs_them(lake: Lake, compactor: Compactor) -> None:
    _edit_everything(lake)
    old_revision = lake.current_revision()
    old_rows = lake.rows(old_revision, include_deleted=True)
    now = datetime.now(UTC)
    compactor.store.pin(DATASET, old_revision, now + timedelta(hours=1))  # e.g. a running export
    compactor.run()
    assert compactor.store.purge_patches(DATASET, now) == 0
    assert lake.rows(old_revision, include_deleted=True) == old_rows

    compactor.store.pin(DATASET, old_revision, now - timedelta(seconds=1))  # the export finished
    assert compactor.store.purge_patches(DATASET, now) > 0
    assert lake.db["dataset_patches"].count_documents({}) == 0
    assert lake.rows(include_deleted=True) == old_rows  # the current revision reads from the base alone


def test_after_compaction_a_unique_value_is_guarded_by_the_base(lake: Lake, compactor: Compactor) -> None:
    lake.edit("pid", Edit(row="r02", op="set", field="pid", t="str", v="P-X"))
    assert lake.db["dataset_unique_claims"].count_documents({}) == 1
    compactor.run()
    assert lake.db["dataset_unique_claims"].count_documents({}) == 0  # the value is in the base now
    with pytest.raises(EditRejected) as refused:
        lake.edit("again", Edit(row="r05", op="set", field="pid", t="str", v="P-X"))
    assert refused.value.code == "DUPLICATE_UNIQUE_ID"
