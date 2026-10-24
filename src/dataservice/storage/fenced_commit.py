"""The single gate every Delta write goes through (consistency.md §4.2).

Every commit carries `txn("emr-writer", token)` plus `{ops, opTables, part}` as custom metadata,
where `ops` lists every operation the commit holds with its attempt — one, or a whole group
commit (§4.7). A commit without the txn would be a blind append that never conflicts, so no
other file may call delta-rs write APIs — tests/unit/test_architecture.py enforces it.
"""

from collections.abc import Iterable
from dataclasses import dataclass

from deltalake import CommitProperties, DeltaTable, Transaction

from dataservice.common.errors import FenceError

WRITER_APP_ID = "emr-writer"


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
        metadata = {
            "ops": format_ops(self.ops),
            "opTables": ",".join(self.op_tables),
            "part": str(self.part),
            **(extra_metadata or {}),
        }
        return CommitProperties(
            app_transactions=[Transaction(WRITER_APP_ID, self.token)],
            custom_metadata=metadata,
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
