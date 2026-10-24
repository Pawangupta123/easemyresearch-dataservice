"""Fence every table, then reconcile (consistency.md §4.3) — scenarios C2, C2b, C3, C3b."""

from pathlib import Path

import pyarrow as pa
import pytest
from deltalake import DeltaTable, write_deltalake
from deltalake.exceptions import CommitFailedError

from dataservice.common.errors import FenceError
from dataservice.storage.fenced_commit import WRITER_APP_ID, CommitContext, OpRef, append
from dataservice.storage.reconcile import ReconcileError, reconcile

TABLES = ("base", "patches", "followups")
OLD, NEW = 1, 2  # the crashed writer's token and the new lease holder's
X1 = OpRef("op-x", 1)


@pytest.fixture
def lake(tmp_path: Path) -> dict[str, str]:
    """Three tables at manifest version 0, each holding one row."""
    paths = {name: str(tmp_path / name) for name in TABLES}
    for path in paths.values():
        write_deltalake(path, pa.table({"v": [0]}), commit_properties=_ctx(OLD, OpRef("ingest", 1)).commit_properties())
    return paths


MANIFEST = dict.fromkeys(TABLES, 0)


def _ctx(token: int, *ops: OpRef) -> CommitContext:
    return CommitContext(token=token, ops=ops, op_tables=TABLES)


def _open(lake: dict[str, str]) -> dict[str, DeltaTable]:
    return {name: DeltaTable(path) for name, path in lake.items()}


def _write(lake: dict[str, str], table: str, token: int, *ops: OpRef, rows: int = 1) -> None:
    append(DeltaTable(lake[table]), pa.table({"v": list(range(1, rows + 1))}), _ctx(token, *ops))


def _rows(path: str) -> int:
    actions = pa.table(DeltaTable(path).get_add_actions(flatten=True))
    return int(sum(actions.column("num_records").to_pylist()))


def test_c2_a_stale_writer_with_a_fresh_view_is_fenced(lake: dict[str, str]) -> None:
    reconcile(_open(lake), MANIFEST, NEW, committed={})
    with pytest.raises(FenceError):
        _write(lake, "base", OLD, X1)


def test_c2_a_stale_writer_holding_a_pre_fence_snapshot_loses_the_race(lake: dict[str, str]) -> None:
    stale = DeltaTable(lake["base"])  # loaded before the new writer fenced
    reconcile(_open(lake), MANIFEST, NEW, committed={})
    with pytest.raises(CommitFailedError):
        append(stale, pa.table({"v": [9]}), _ctx(OLD, X1))
    assert _rows(lake["base"]) == 1


def test_c2b_the_fence_covers_tables_the_new_writer_never_writes(lake: dict[str, str]) -> None:
    reconcile(_open(lake), MANIFEST, NEW, committed={})
    with pytest.raises(FenceError):
        _write(lake, "followups", OLD, X1)


def test_c3_an_incomplete_operation_is_rolled_back_everywhere(lake: dict[str, str]) -> None:
    _write(lake, "base", OLD, X1)  # crashed before writing patches, never `committed`
    result = reconcile(_open(lake), MANIFEST, NEW, committed={})
    assert result.rolled_back_ops == {X1} and not result.roll_forward_ops
    assert {name: _rows(path) for name, path in lake.items()} == dict.fromkeys(TABLES, 1)


def test_c3_a_committed_operation_rolls_forward(lake: dict[str, str]) -> None:
    _write(lake, "base", OLD, X1)
    _write(lake, "patches", OLD, X1)  # all its tables written, Node marked it committed
    result = reconcile(_open(lake), MANIFEST, NEW, committed={X1: OLD})
    assert result.roll_forward_ops == {X1} and not result.rolled_back_ops
    assert result.roll_forward_versions == {"base": 1, "patches": 1}
    assert (_rows(lake["base"]), _rows(lake["patches"])) == (2, 2)


def test_a_committed_group_survives_a_crash_in_the_next_group(lake: dict[str, str]) -> None:
    group_1, group_2 = (OpRef("op-a", 1), OpRef("op-b", 1)), (OpRef("op-c", 1),)
    _write(lake, "base", OLD, *group_1, rows=2)  # committed, not yet published
    _write(lake, "base", OLD, *group_2)  # crashed mid-way
    result = reconcile(_open(lake), MANIFEST, NEW, committed=dict.fromkeys(group_1, OLD))
    assert result.roll_forward_ops == set(group_1) and result.roll_forward_versions == {"base": 1}
    assert result.rolled_back_ops == set(group_2)
    assert _rows(lake["base"]) == 3


def test_a_committed_operation_written_under_another_token_is_rolled_back(lake: dict[str, str]) -> None:
    _write(lake, "base", OLD, X1)
    result = reconcile(_open(lake), MANIFEST, NEW, committed={X1: 7})  # Node recorded token 7
    assert result.rolled_back_ops == {X1}


def test_c3b_a_retry_after_rollback_counts_only_the_new_attempt(lake: dict[str, str]) -> None:
    _write(lake, "base", OLD, X1)
    reconcile(_open(lake), MANIFEST, NEW, committed={})  # attempt 1 rolled back
    x2 = OpRef("op-x", 2)
    _write(lake, "base", NEW, x2)
    _write(lake, "patches", NEW, x2)
    result = reconcile(_open(lake), MANIFEST, 3, committed={x2: NEW})
    assert result.roll_forward_ops == {x2}  # attempt 1's commit is void, never counted
    assert not result.rolled_back_ops
    assert (_rows(lake["base"]), _rows(lake["patches"])) == (2, 2)


def test_reconciling_twice_changes_nothing(lake: dict[str, str]) -> None:
    _write(lake, "base", OLD, X1)
    reconcile(_open(lake), MANIFEST, NEW, committed={})
    again = reconcile(_open(lake), MANIFEST, 3, committed={})
    assert not again.rolled_back_ops and not again.roll_forward_ops
    assert _rows(lake["base"]) == 1


def test_every_reconcile_commit_keeps_the_fence(lake: dict[str, str]) -> None:
    _write(lake, "base", OLD, X1)
    reconcile(_open(lake), MANIFEST, NEW, committed={})
    assert all(DeltaTable(path).transaction_version(WRITER_APP_ID) == NEW for path in lake.values())


def test_the_rollback_commit_is_fenced_and_says_what_it_removed(lake: dict[str, str]) -> None:
    _write(lake, "base", OLD, X1)
    reconcile(_open(lake), MANIFEST, NEW, committed={})
    last = DeltaTable(lake["base"]).history(1)[0]
    assert (last["kind"], last["token"], last["restoredTo"], last["rolledBack"]) == ("reconcile", "2", "0", "op-x:1")


def test_a_committed_commit_after_an_uncommitted_one_stops_reconciliation(lake: dict[str, str]) -> None:
    y1 = OpRef("op-y", 1)
    _write(lake, "base", OLD, y1)  # never committed
    _write(lake, "base", OLD, X1)  # yet a later one is — the protocol was broken
    with pytest.raises(ReconcileError):
        reconcile(_open(lake), MANIFEST, NEW, committed={X1: OLD})
