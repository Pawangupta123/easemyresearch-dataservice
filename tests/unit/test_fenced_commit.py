from pathlib import Path

import pyarrow as pa
import pytest
from deltalake import DeltaTable, write_deltalake
from deltalake.exceptions import CommitFailedError

from dataservice.common.errors import FenceError
from dataservice.storage.fenced_commit import (
    WRITER_APP_ID,
    CommitContext,
    OpRef,
    check_fence,
    max_token_seen,
    parse_ops,
)


def _rows(n: int) -> pa.Table:
    return pa.table({"_row_id": list(range(n))})


def _ctx(token: int, attempt: int = 1) -> CommitContext:
    return CommitContext(token=token, ops=(OpRef("op-1", attempt),), op_tables=("base",))


def test_every_commit_carries_the_txn_and_the_operation_metadata(tmp_path: Path) -> None:
    write_deltalake(str(tmp_path), _rows(2), commit_properties=_ctx(5).commit_properties())
    table = DeltaTable(str(tmp_path))
    assert table.transaction_version(WRITER_APP_ID) == 5
    last = table.history(1)[0]
    assert (last["ops"], last["opTables"], last["part"]) == ("op-1:1", "base", "1")


def test_a_group_commit_lists_every_operation_and_reads_back(tmp_path: Path) -> None:
    group = (OpRef("op-a", 1), OpRef("op-b", 3), OpRef("op:with:colons", 2))
    ctx = CommitContext(token=4, ops=group, op_tables=("base", "patches"))
    write_deltalake(str(tmp_path), _rows(1), commit_properties=ctx.commit_properties())
    assert parse_ops(DeltaTable(str(tmp_path)).history(1)[0]["ops"]) == group


def test_a_commit_without_operations_is_refused() -> None:
    with pytest.raises(ValueError, match="at least one operation"):
        CommitContext(token=1, ops=(), op_tables=("base",))


def test_a_stale_writer_is_stopped_by_the_fence(tmp_path: Path) -> None:
    write_deltalake(str(tmp_path), _rows(1), commit_properties=_ctx(7).commit_properties())
    with pytest.raises(FenceError):
        check_fence(DeltaTable(str(tmp_path)), token=6)
    check_fence(DeltaTable(str(tmp_path)), token=7)


def test_max_token_seen_is_the_highest_across_tables(tmp_path: Path) -> None:
    first, second, untouched = tmp_path / "a", tmp_path / "b", tmp_path / "c"
    write_deltalake(str(first), _rows(1), commit_properties=_ctx(3).commit_properties())
    write_deltalake(str(second), _rows(1), commit_properties=_ctx(9).commit_properties())
    write_deltalake(str(untouched), _rows(1))
    tables = [DeltaTable(str(path)) for path in (first, second, untouched)]
    assert max_token_seen(tables) == 9
    assert max_token_seen([DeltaTable(str(untouched))]) == 0
    assert max_token_seen([]) == 0


def test_two_writers_with_the_same_app_id_race_and_one_loses(tmp_path: Path) -> None:
    write_deltalake(str(tmp_path), _rows(1), commit_properties=_ctx(1).commit_properties())
    first, second = DeltaTable(str(tmp_path)), DeltaTable(str(tmp_path))
    write_deltalake(first, _rows(1), mode="append", commit_properties=_ctx(2).commit_properties())
    with pytest.raises(CommitFailedError):
        write_deltalake(second, _rows(1), mode="append", commit_properties=_ctx(2, attempt=2).commit_properties())


def test_restore_writes_a_new_commit_not_the_old_version_number(tmp_path: Path) -> None:
    # Why consistency §5.3 counts attempts instead of "commits after the manifest version".
    write_deltalake(str(tmp_path), _rows(2), commit_properties=_ctx(1).commit_properties())
    write_deltalake(str(tmp_path), _rows(1), mode="append", commit_properties=_ctx(1).commit_properties())
    table = DeltaTable(str(tmp_path))
    table.restore(0, commit_properties=_ctx(2).commit_properties({"kind": "reconcile"}))
    restored = DeltaTable(str(tmp_path))
    assert restored.version() == 2
    assert restored.history(1)[0]["kind"] == "reconcile"
