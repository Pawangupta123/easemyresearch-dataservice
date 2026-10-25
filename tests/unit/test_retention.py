"""Old Delta versions (consistency.md §11) — C14: a fenced VACUUM keeps the files of a pinned
revision while a job still reads it; C16: an old version stays readable after log cleanup
within the retention window, and only within it."""

from pathlib import Path

import pyarrow as pa
import pytest
from deltalake import DeltaTable, write_deltalake

from dataservice.common.errors import FenceError
from dataservice.storage.fenced_commit import WRITER_APP_ID, fence, retention_configuration, vacuum
from dataservice.storage.reader import count, query
from dataservice.storage.snapshot import PATCH_SCHEMA, UTC_TIMESTAMP, FieldSpec, Snapshot

FIELDS = (FieldSpec("age", "num"),)
NO_RETENTION = {"delta.deletedFileRetentionDuration": "interval 0 seconds"}  # tests: vacuum may remove at once


class _NoPatches:
    def patches(self, dataset_id: str, from_seq: int, to_seq: int) -> pa.Table:
        return pa.Table.from_pylist([], schema=PATCH_SCHEMA)


def _rows(*ages: float) -> pa.Table:
    ids = [f"r{n}" for n in range(len(ages))]
    return pa.table({"_rid": ids, "_deleted_at": pa.array([None] * len(ids), UTC_TIMESTAMP), "f_age": list(ages)})


def _three_versions(uri: str) -> None:
    write_deltalake(uri, _rows(1.0, 2.0), configuration=NO_RETENTION)  # v0: pinned by a running job
    write_deltalake(uri, _rows(5.0), mode="overwrite")  # v1: a compaction rewrote the base
    write_deltalake(uri, _rows(9.0), mode="overwrite")  # v2


def _read(uri: str, version: int) -> list[object]:
    return list(query(Snapshot("ds", uri, version, 0, 0, FIELDS), _NoPatches()).column("f_age").to_pylist())


def test_c14_a_vacuum_keeps_a_pinned_revisions_files_while_a_job_reads_it(tmp_path: Path) -> None:
    uri = str(tmp_path / "base")
    _three_versions(uri)
    removed = vacuum(DeltaTable(uri), token=3, keep_versions=[0], retention_hours=0)
    assert len(removed) == 1  # v1's file: neither current nor pinned
    assert _read(uri, 0) == [1.0, 2.0]  # the job's pinned revision is intact
    assert _read(uri, 2) == [9.0]
    assert DeltaTable(uri).transaction_version(WRITER_APP_ID) == 3  # fenced like every other commit
    kinds = [entry.get("kind") for entry in DeltaTable(uri).history(2)]
    assert kinds == ["vacuum", "vacuum"]  # VACUUM START / END carry the token and kind

    vacuum(DeltaTable(uri), token=3, keep_versions=[], retention_hours=0)  # the job finished: pin gone
    with pytest.raises(Exception):  # noqa: B017 — DuckDB or delta-rs, whichever opens the file first
        count(Snapshot("ds", uri, 0, 0, 0, FIELDS), _NoPatches())


def test_a_vacuum_from_a_stale_writer_is_refused_and_removes_nothing(tmp_path: Path) -> None:
    uri = str(tmp_path / "base")
    _three_versions(uri)
    fence(DeltaTable(uri), 5)  # a newer writer took the dataset over
    with pytest.raises(FenceError):
        vacuum(DeltaTable(uri), token=4, keep_versions=[], retention_hours=0)
    assert _read(uri, 0) == [1.0, 2.0] and _read(uri, 1) == [5.0]


def test_a_vacuum_never_goes_below_the_tables_own_file_retention(tmp_path: Path) -> None:
    uri = str(tmp_path / "base")
    write_deltalake(uri, _rows(1.0), configuration=retention_configuration(60))
    write_deltalake(uri, _rows(2.0), mode="overwrite")
    with pytest.raises(Exception, match="(?i)retention"):  # noqa: B017 — delta-rs's own guard
        vacuum(DeltaTable(uri), token=1, keep_versions=[], retention_hours=0)
    assert _read(uri, 0) == [1.0]


def _many_versions(uri: str, configuration: dict[str, str] | None) -> None:
    write_deltalake(uri, _rows(0.0), configuration=configuration)
    for n in range(1, 12):
        write_deltalake(uri, _rows(float(n)), mode="overwrite")
    table = DeltaTable(uri)
    table.create_checkpoint()
    table.cleanup_metadata()  # log cleanup at a checkpoint (what delta-rs does on its own)


def test_c16_an_old_version_reads_after_log_cleanup_within_the_retention_window(tmp_path: Path) -> None:
    uri = str(tmp_path / "base")
    _many_versions(uri, retention_configuration(60))
    assert _read(uri, 1) == [1.0] and _read(uri, 11) == [11.0]


def test_c16_beyond_the_log_retention_an_old_version_is_gone_whatever_files_remain(tmp_path: Path) -> None:
    """Why every pin window must fit inside the log retention (Settings refuses the opposite)."""
    uri = str(tmp_path / "base")
    _many_versions(uri, {"delta.logRetentionDuration": "interval 0 seconds"})
    with pytest.raises(Exception):  # noqa: B017 — delta-rs: no log segment for that version
        _read(uri, 1)


def test_tables_are_configured_to_keep_old_versions_for_the_retention_window() -> None:
    assert retention_configuration(60) == {
        "delta.logRetentionDuration": "interval 60 days",
        "delta.deletedFileRetentionDuration": "interval 60 days",
    }
    with pytest.raises(ValueError):
        retention_configuration(0)
