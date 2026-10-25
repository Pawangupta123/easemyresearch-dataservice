"""Reads of one revision through the one reader, end to end with the reference Node edit
transaction on a real Mongo replica set — C19 (inserted rows before any compaction), C43 (grid,
analysis and export give the same rows), C40 (an analysis reads its pinned revision while a
compaction commits and publishes)."""

from datetime import UTC, datetime, timedelta

import pyarrow as pa
import pytest
from pymongo.database import Database

from dataservice.storage.reader import Query, Sort, count, query, scan
from prototype.node_side.edit_store import Edit
from tests.integration.conftest import Document
from tests.integration.lake import DATASET, ROWS, Compactor, Lake, set_age


@pytest.fixture
def compactor(lake: Lake, mongo_db: Database[Document]) -> Compactor:
    return Compactor(lake, mongo_db)


def _rows(table: pa.Table) -> dict[str, dict[str, object]]:
    return {str(row.pop("_rid")): row for row in table.to_pylist()}


def _edit_a_little_of_everything(lake: Lake) -> None:
    lake.edit("set", set_age("r00", 31.0))
    lake.edit("clear", Edit(row="r01", op="set", field="age", t="null"))
    lake.edit("insert", Edit(row="r90", op="insert_row"), set_age("r90", 9.0))
    lake.edit("delete", Edit(row="r03", op="delete_row"))


def test_c19_inserted_rows_are_read_at_once_and_their_delete_and_restore_apply(lake: Lake) -> None:
    inserted = lake.edit("add", Edit(row="r90", op="insert_row"), set_age("r90", 9.0), Edit(row="r91", op="insert_row"))
    rows = lake.rows(inserted)  # no compaction: the rows come from patches alone
    assert rows["r90"] == {"f_age": 9.0, "f_pid": None} and rows["r91"] == {"f_age": None, "f_pid": None}
    assert len(rows) == len(ROWS) + 2

    deleted = lake.edit("remove", Edit(row="r90", op="delete_row"))
    assert "r90" not in lake.rows(deleted)
    trashed_at = lake.rows(deleted, include_deleted=True)["r90"]["_deleted_at"]
    assert isinstance(trashed_at, datetime) and trashed_at > datetime.now(UTC) - timedelta(minutes=1)

    restored = lake.edit("bring back", Edit(row="r90", op="restore_row"))
    assert lake.rows(restored)["r90"] == {"f_age": 9.0, "f_pid": None}  # its values came back with it
    assert "r90" in lake.rows(inserted) and "r90" not in lake.rows(deleted)  # each revision keeps its own view

    snapshot = lake.snapshot(restored)
    page = query(snapshot, lake.patches, Query(sort=(Sort("age", descending=True),), limit=3))  # two passes
    assert [row["_rid"] for row in page.to_pylist()] == ["r09", "r08", "r07"]
    assert count(snapshot, lake.patches) == len(ROWS) + 2


def test_c43_grid_analysis_and_export_read_the_same_rows(lake: Lake) -> None:
    _edit_a_little_of_everything(lake)
    snapshot = lake.snapshot()
    pages, offset = [], 0
    while True:  # the grid: pages of 4, in row-id order
        page = query(snapshot, lake.patches, Query(limit=4, offset=offset)).to_pylist()
        if not page:
            break
        pages += page
        offset += 4
    grid = {str(row.pop("_rid")): row for row in pages}
    analysis = _rows(query(snapshot, lake.patches, Query(fields=("age", "pid"))))
    with scan(snapshot, lake.patches) as reader:  # the export
        export = _rows(reader.read_all())
    assert grid == analysis == export
    assert count(snapshot, lake.patches) == len(grid) == len(ROWS)  # one inserted, one deleted
    assert grid["r00"]["f_age"] == 31.0 and grid["r01"]["f_age"] is None and grid["r90"]["f_age"] == 9.0


def test_c40_an_analysis_reads_its_pinned_revision_while_a_compaction_lands(lake: Lake, compactor: Compactor) -> None:
    _edit_a_little_of_everything(lake)
    pinned = lake.current_revision()
    expected = lake.rows(pinned)
    compactor.store.pin(DATASET, pinned, datetime.now(UTC) + timedelta(hours=1))  # the analysis's pin

    with scan(lake.snapshot(pinned), lake.patches, batch_rows=2) as reader:
        first = reader.read_next_batch()
        published = compactor.run()  # a new base, a new revision, while the analysis is mid-read
        assert compactor.store.purge_patches(DATASET, datetime.now(UTC)) == 0  # the pin keeps the patches
        later = lake.edit("later", set_age("r00", 77.0), Edit(row="r05", op="delete_row"))  # after the pin
        rest = reader.read_all()
    streamed = _rows(pa.Table.from_batches([first, *rest.to_batches()]))

    assert published == pinned + 1
    assert streamed == expected  # its own revision only: no newer base, no newer patch
    assert lake.rows(pinned) == expected and lake.rows(published) == expected  # compaction changed no value
    newest = lake.rows(later)
    assert newest["r00"]["f_age"] == 77.0 and "r05" not in newest and expected["r00"]["f_age"] == 31.0
