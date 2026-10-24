"""Hybrid small edits end to end: Node's edit transaction (prototype) on a real Mongo replica set,
read back through the one reader (Delta base + Mongo patches) — C31, C34, C35, C44."""

import threading
from collections.abc import Mapping
from pathlib import Path

import pyarrow as pa
import pytest
from deltalake import write_deltalake
from pymongo import MongoClient
from pymongo.database import Database

from dataservice.storage.mongo_patches import MongoPatchSource
from dataservice.storage.reader import read_rows
from dataservice.storage.snapshot import FieldSpec, Snapshot
from prototype.node_side.edit_store import Edit, EditRejected, MongoEditStore, Target
from tests.integration.conftest import Document

DATASET, FORM = "ds-1", "form-1"
FIELDS = (FieldSpec("age", "num"), FieldSpec("pid", "str"))
ROWS = [f"r{index:02d}" for index in range(10)]


class Lake:
    def __init__(self, client: MongoClient[Document], db: Database[Document], base_uri: str) -> None:
        self.db, self.base_uri = db, base_uri
        self.store = MongoEditStore(client, db, unique_field="pid")
        self.patches = MongoPatchSource(db)
        self.store.create_dataset(DATASET, base_version=0, form_id=FORM)

    def rows(self, revision: int | None = None) -> dict[str, dict[str, object]]:
        if revision is None:
            dataset = self.db["datasets"].find_one({"_id": DATASET})
            assert dataset is not None
            revision = int(str(dataset["currentRevision"]))
        manifest = self.db["dataset_revisions"].find_one({"datasetId": DATASET, "revision": revision})
        assert manifest is not None
        snapshot = Snapshot(
            DATASET,
            self.base_uri,
            int(str(manifest["baseVersion"])),
            int(str(manifest["patchFrom"])),
            int(str(manifest["patchTo"])),
            FIELDS,
        )
        return {row.pop("_rid"): row for row in read_rows(snapshot, self.patches).to_pylist()}

    def before(self, revision: int, cells: list[Target]) -> Mapping[Target, object]:
        rows = self.rows(revision)
        return {(row, field): rows.get(row, {}).get(f"f_{field}") for row, field in cells}

    def in_base(self, revision: int, field: str, value: object) -> bool:
        return any(row.get(f"f_{field}") == value for row in self.rows(revision).values())

    def edit(self, key: str, *edits: Edit, actor: str = "user-a") -> int:
        return self.store.apply(DATASET, actor, key, list(edits), self.before, self.in_base).revision


@pytest.fixture
def lake(mongo_client: MongoClient[Document], mongo_db: Database[Document], tmp_path: Path) -> Lake:
    uri = str(tmp_path / "base")
    write_deltalake(
        uri,
        pa.table(
            {
                "_rid": ROWS,
                "_deleted_at": pa.array([None] * len(ROWS), pa.timestamp("us")),
                "f_age": [float(30 + index) for index in range(len(ROWS))],
                "f_pid": [f"P-{index}" for index in range(len(ROWS))],
            }
        ),
    )
    return Lake(mongo_client, mongo_db, uri)


def _set_age(row: str, age: float) -> Edit:
    return Edit(row=row, op="set", field="age", t="num", v=age)


def test_an_edit_is_visible_at_its_revision_and_old_revisions_stay_as_they_were(lake: Lake) -> None:
    revision = lake.edit("k1", _set_age("r00", 31.0))
    assert revision == 2
    assert lake.rows(2)["r00"]["f_age"] == 31.0
    assert lake.rows(1)["r00"]["f_age"] == 30.0  # a pinned reader of revision 1 is unaffected
    form = lake.db["forms"].find_one({"_id": FORM})
    assert form is not None and form["dataVersion"] == 2
    assert lake.db["dataset_events"].count_documents({"datasetId": DATASET, "revision": 2}) == 1


def test_c35_the_later_save_wins_and_history_keeps_both(lake: Lake) -> None:
    lake.edit("k1", _set_age("r00", 31.0), actor="user-a")
    lake.edit("k2", _set_age("r00", 32.0), actor="user-b")
    assert lake.rows()["r00"]["f_age"] == 32.0
    history = list(lake.db["dataset_changes"].find({"row": "r00", "field": "age"}).sort("seq", 1))
    assert [(change["before"], change["after"], change["actor"]) for change in history] == [
        (30.0, 31.0, "user-a"),
        (31.0, 32.0, "user-b"),
    ]


def test_c34_ten_users_on_different_cells_all_keep_their_values(lake: Lake) -> None:
    errors: list[Exception] = []

    def save(index: int) -> None:
        try:
            lake.edit(f"k{index}", _set_age(ROWS[index], 100.0 + index), actor=f"user-{index}")
        except Exception as error:
            errors.append(error)

    threads = [threading.Thread(target=save, args=(index,)) for index in range(len(ROWS))]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert errors == []
    rows = lake.rows()
    assert {row: rows[row]["f_age"] for row in ROWS} == {row: 100.0 + index for index, row in enumerate(ROWS)}
    revisions = sorted(int(str(doc["revision"])) for doc in lake.db["dataset_revisions"].find({"datasetId": DATASET}))
    assert revisions == list(range(1, len(ROWS) + 2))  # one revision per save, no gaps, no duplicates
    seqs = sorted(int(str(doc["seq"])) for doc in lake.db["dataset_patches"].find({"datasetId": DATASET}))
    assert seqs == list(range(1, len(ROWS) + 1))


def test_two_users_on_the_same_cell_at_once_the_later_commit_wins(lake: Lake) -> None:
    threads = [
        threading.Thread(target=lake.edit, args=(f"k{age}", _set_age("r01", float(age))), kwargs={"actor": f"u{age}"})
        for age in (70, 80)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    last = lake.db["dataset_patches"].find_one({"target.row": "r01"}, sort=[("seq", -1)])
    assert last is not None and isinstance(last["value"], Mapping)
    assert lake.rows()["r01"]["f_age"] == last["value"]["v"]
    assert lake.db["dataset_changes"].count_documents({"row": "r01"}) == 2


def test_c31_a_retried_save_lands_once_and_a_reused_key_is_refused(lake: Lake) -> None:
    first = lake.edit("same-key", _set_age("r02", 50.0))
    again = lake.edit("same-key", _set_age("r02", 50.0))
    assert first == again == 2
    assert lake.db["dataset_patches"].count_documents({"datasetId": DATASET}) == 1
    with pytest.raises(EditRejected) as refused:
        lake.edit("same-key", _set_age("r02", 51.0))
    assert refused.value.code == "IDEMPOTENCY_MISMATCH"


def test_c44_a_unique_identifier_is_claimed_once_and_a_rejected_save_writes_nothing(lake: Lake) -> None:
    outcomes: list[str] = []

    def claim(row: str) -> None:
        try:
            lake.edit(f"claim-{row}", Edit(row=row, op="set", field="pid", t="str", v="P-99"), actor=row)
            outcomes.append("saved")
        except EditRejected as rejected:
            outcomes.append(rejected.code)

    threads = [threading.Thread(target=claim, args=(row,)) for row in ("r03", "r04")]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert sorted(outcomes) == ["DUPLICATE_UNIQUE_ID", "saved"]

    patches_before = lake.db["dataset_patches"].count_documents({})
    with pytest.raises(EditRejected) as in_base:
        lake.edit("taken", _set_age("r05", 1.0), Edit(row="r05", op="set", field="pid", t="str", v="P-0"))
    assert in_base.value.code == "DUPLICATE_UNIQUE_ID"
    assert lake.db["dataset_patches"].count_documents({}) == patches_before  # the age edit did not land either
    assert lake.rows()["r05"]["f_age"] == 35.0


def test_inserted_and_deleted_rows_through_mongo_patches(lake: Lake) -> None:
    lake.edit("ins", Edit(row="r99", op="insert_row"), _set_age("r99", 12.0))
    lake.edit("del", Edit(row="r00", op="delete_row"))
    rows = lake.rows()
    assert rows["r99"] == {"f_age": 12.0, "f_pid": None}
    assert "r00" not in rows and len(rows) == len(ROWS)
