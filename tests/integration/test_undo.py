"""Batch undo on the change log, per cell (consistency.md §8.2) — C10, C11, the undo half of C35,
losses re-checked inside the transaction, excluded rows, clears, retries and redo."""

import pytest

from prototype.node_side.edit_store import Edit, EditRejected
from prototype.node_side.undo_store import MongoUndoStore
from tests.integration.lake import DATASET, Lake, set_age


@pytest.fixture
def undo(lake: Lake) -> MongoUndoStore:
    return MongoUndoStore(lake.db, lake.store, lambda revision: lake.rows(revision), lake.in_base)


def test_undo_writes_the_before_values_back_as_a_new_revision(lake: Lake, undo: MongoUndoStore) -> None:
    lake.edit("batch-a", set_age("r00", 31.0), set_age("r01", 41.0))
    result = undo.apply(DATASET, "user-a", "batch-a")
    rows = lake.rows()
    assert (rows["r00"]["f_age"], rows["r01"]["f_age"]) == (30.0, 31.0)
    assert (result.restored, result.skipped, result.revision) == (2, 0, 3)
    assert lake.db["dataset_changes"].count_documents({"batchId": "undo:batch-a"}) == 2  # the undo is history too


def test_c10_undoing_one_batch_leaves_another_batch_intact(lake: Lake, undo: MongoUndoStore) -> None:
    lake.edit("batch-a", set_age("r00", 31.0))
    lake.edit("batch-b", set_age("r05", 55.0))
    undo.apply(DATASET, "user-a", "batch-a")
    rows = lake.rows()
    assert (rows["r00"]["f_age"], rows["r05"]["f_age"]) == (30.0, 55.0)


def test_c35_undoing_the_later_of_two_edits_brings_the_earlier_value_back(lake: Lake, undo: MongoUndoStore) -> None:
    lake.edit("first", set_age("r00", 31.0), actor="user-a")
    lake.edit("second", set_age("r00", 32.0), actor="user-b")
    undo.apply(DATASET, "user-b", "second")
    assert lake.rows()["r00"]["f_age"] == 31.0


def test_c11_a_later_edit_to_the_same_cell_is_a_loss_that_needs_consent(lake: Lake, undo: MongoUndoStore) -> None:
    lake.edit("batch-a", set_age("r00", 31.0), set_age("r01", 41.0))
    lake.edit("later", set_age("r00", 99.0), actor="user-b")
    plan = undo.preview(DATASET, "batch-a")
    assert [(loss.row, loss.current) for loss in plan.losses] == [("r00", 99.0)]
    with pytest.raises(EditRejected) as refused:
        undo.apply(DATASET, "user-a", "batch-a")
    assert refused.value.code == "UNDO_CONFLICT"
    assert lake.rows()["r00"]["f_age"] == 99.0  # nothing written
    undo.apply(DATASET, "user-a", "batch-a", consent=plan.loss_set_hash)
    rows = lake.rows()
    assert (rows["r00"]["f_age"], rows["r01"]["f_age"]) == (30.0, 31.0)


def test_an_edit_after_the_preview_is_refused(lake: Lake, undo: MongoUndoStore) -> None:
    lake.edit("batch-a", set_age("r00", 31.0))
    plan = undo.preview(DATASET, "batch-a")
    assert not plan.losses
    lake.edit("sneaky", set_age("r00", 77.0), actor="user-b")  # lands after the preview
    with pytest.raises(EditRejected) as refused:
        undo.apply(DATASET, "user-a", "batch-a", consent=plan.loss_set_hash)
    assert refused.value.code == "UNDO_CONFLICT"
    assert lake.rows()["r00"]["f_age"] == 77.0


def test_an_edit_racing_the_undo_is_caught_inside_the_transaction(lake: Lake) -> None:
    """The edit lands after apply's own up-front check passed, just before its transaction: only
    the re-check inside the transaction (at the revision it writes on) can see it."""
    lake.edit("batch-a", set_age("r00", 31.0))
    calls = 0

    def rows_at(revision: int) -> dict[str, dict[str, object]]:
        nonlocal calls
        calls += 1
        rows = lake.rows(revision)
        if calls == 2:  # apply's up-front check has its rows: now the racing edit commits
            lake.edit("racing", set_age("r00", 77.0), actor="user-b")
        return rows

    racing = MongoUndoStore(lake.db, lake.store, rows_at, lake.in_base)
    with pytest.raises(EditRejected) as refused:
        racing.apply(DATASET, "user-a", "batch-a")
    assert refused.value.code == "CONFLICT"  # the plan changed under it: preview again
    assert lake.rows()["r00"]["f_age"] == 77.0  # the racing edit is kept, nothing overwritten


def test_a_consent_for_other_losses_is_refused(lake: Lake, undo: MongoUndoStore) -> None:
    lake.edit("batch-a", set_age("r00", 31.0))
    lake.edit("later", set_age("r00", 50.0))
    stale_consent = undo.preview(DATASET, "batch-a").loss_set_hash
    lake.edit("later-again", set_age("r00", 60.0))
    with pytest.raises(EditRejected) as refused:
        undo.apply(DATASET, "user-a", "batch-a", consent=stale_consent)
    assert refused.value.code == "UNDO_CONFLICT"


def test_rows_deleted_since_are_skipped(lake: Lake, undo: MongoUndoStore) -> None:
    lake.edit("batch-a", set_age("r00", 31.0), set_age("r01", 41.0))
    lake.edit("delete", Edit(row="r01", op="delete_row"))
    result = undo.apply(DATASET, "user-a", "batch-a")
    assert (result.restored, result.skipped) == (1, 1)
    assert lake.rows()["r00"]["f_age"] == 30.0


def test_undoing_a_clear_restores_the_value(lake: Lake, undo: MongoUndoStore) -> None:
    lake.edit("clear", Edit(row="r00", op="set", field="age", t="null"))
    assert lake.rows()["r00"]["f_age"] is None
    undo.apply(DATASET, "user-a", "clear")
    assert lake.rows()["r00"]["f_age"] == 30.0


def test_a_retried_undo_lands_once_and_undoing_the_undo_is_a_redo(lake: Lake, undo: MongoUndoStore) -> None:
    lake.edit("batch-a", set_age("r00", 31.0))
    first = undo.apply(DATASET, "user-a", "batch-a")
    again = undo.apply(DATASET, "user-a", "batch-a")  # a browser retry
    assert again.revision == first.revision
    assert lake.db["dataset_changes"].count_documents({"batchId": "undo:batch-a"}) == 1
    undo.apply(DATASET, "user-a", "undo:batch-a")  # redo
    assert lake.rows()["r00"]["f_age"] == 31.0


def test_undo_of_an_unknown_batch_is_refused(lake: Lake, undo: MongoUndoStore) -> None:
    with pytest.raises(EditRejected) as refused:
        undo.preview(DATASET, "no-such-batch")
    assert refused.value.code == "NOT_FOUND"


def test_a_save_in_parts_that_touched_a_cell_twice_undoes_to_its_first_before(lake: Lake, undo: MongoUndoStore) -> None:
    # One bulk edit sent in two parts: two requests (two idempotency keys), one mutationId.
    lake.edit("part-1", set_age("r00", 31.0), batch="bulk-1")
    lake.edit("part-2", set_age("r00", 32.0), batch="bulk-1")
    assert lake.rows()["r00"]["f_age"] == 32.0
    result = undo.apply(DATASET, "user-a", "bulk-1")
    assert result.restored == 1 and lake.rows()["r00"]["f_age"] == 30.0
