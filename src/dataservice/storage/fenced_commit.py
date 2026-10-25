"""The single gate every Delta write goes through (consistency.md §4.2).

Every commit carries `txn("emr-writer", token)` plus custom metadata: `token`, and either `ops`
(every operation it holds with its attempt — one, or a whole group commit, §4.7) or a `kind`
(`fence`, `reconcile`). A commit without the txn would be a blind write that never conflicts
with a racing writer of the same app id, so no other file may call delta-rs write APIs
(tests/unit/test_architecture.py enforces it). (A RESTORE keeps the table's latest txn, so it
never lowers the fence; it still carries the token for the race check and the audit trail.)
"""

from collections.abc import Iterable
from dataclasses import dataclass

import pyarrow as pa
from deltalake import CommitProperties, DeltaTable, Transaction, write_deltalake

from dataservice.common.errors import FenceError

WRITER_APP_ID = "emr-writer"
FENCE_PROPERTY = "emr.fence"


@dataclass(frozen=True)
class OpRef:
    """One operation inside a commit, at the attempt that wrote it (§5.3)."""

    op_id: str
    attempt: int


@dataclass(frozen=True)
class CommitContext:
    token: int
    ops: tuple[OpRef, ...]
    op_tables: tuple[str, ...]
    part: int = 1

    def __post_init__(self) -> None:
        if not self.ops:
            raise ValueError("a commit must carry at least one operation")

    def commit_properties(self, extra_metadata: dict[str, str] | None = None) -> CommitProperties:
        return _properties(
            self.token,
            {"ops": format_ops(self.ops), "opTables": ",".join(self.op_tables), "part": str(self.part)}
            | (extra_metadata or {}),
        )


def _properties(token: int, metadata: dict[str, str]) -> CommitProperties:
    return CommitProperties(
        app_transactions=[Transaction(WRITER_APP_ID, token)],
        custom_metadata={"token": str(token)} | metadata,
        max_commit_retries=0,
    )


def format_ops(ops: Iterable[OpRef]) -> str:
    return ",".join(f"{op.op_id}:{op.attempt}" for op in ops)


def parse_ops(value: str) -> tuple[OpRef, ...]:
    """Read a commit's `ops` metadata back (reconciliation reads the log with it)."""
    refs = []
    for item in filter(None, value.split(",")):
        op_id, _, attempt = item.rpartition(":")
        refs.append(OpRef(op_id=op_id, attempt=int(attempt)))
    return tuple(refs)


def check_fence(table: DeltaTable, token: int) -> None:
    """Abort if any writer has committed to this table with a higher token."""
    seen = table.transaction_version(WRITER_APP_ID)
    if seen is not None and seen > token:
        raise FenceError(f"table fenced by token {seen}; this writer holds {token}", user_safe=False)


def max_token_seen(tables: Iterable[DeltaTable]) -> int:
    """The highest writer token committed to any of the tables (0 when none) — a new lease
    token must exceed it even if Mongo lost the stored one (§4.1)."""
    return max((table.transaction_version(WRITER_APP_ID) or 0 for table in tables), default=0)


def append(table: DeltaTable, data: pa.Table, context: CommitContext) -> None:
    """Append rows for the operations in `context` — fenced, never rebased onto a newer version."""
    check_fence(table, context.token)
    write_deltalake(table, data, mode="append", commit_properties=context.commit_properties())


def overwrite(table: DeltaTable, data: pa.Table | pa.RecordBatchReader, context: CommitContext) -> None:
    """Replace the table's data (a base rewrite: compaction, transform) — fenced, never rebased.
    A stream is written batch by batch, so a GB-scale rewrite never sits in memory."""
    check_fence(table, context.token)
    write_deltalake(table, data, mode="overwrite", commit_properties=context.commit_properties())


def fence(table: DeltaTable, token: int) -> None:
    """A data-free commit carrying the new token (§4.3 step 1): from here on, any commit from an
    older writer either sees the higher token or races this commit and loses."""
    check_fence(table, token)
    table.alter.set_table_properties(
        {FENCE_PROPERTY: str(token)},
        raise_if_not_exists=False,
        commit_properties=_properties(token, {"kind": "fence"}),
    )


def restore(table: DeltaTable, version: int, token: int, rolled_back: Iterable[OpRef]) -> None:
    """Roll the table back to `version` with a fenced RESTORE (a NEW commit, §4.3 step 2) that
    records what it removed, so later reconciliation never counts those commits again."""
    check_fence(table, token)
    table.restore(
        version,
        commit_properties=_properties(
            token, {"kind": "reconcile", "restoredTo": str(version), "rolledBack": format_ops(rolled_back)}
        ),
    )
