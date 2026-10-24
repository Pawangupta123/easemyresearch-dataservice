"""The single gate every Delta write goes through (consistency.md §4.2).

Every commit carries `txn("emr-writer", token)` plus `{opId, attempt, opTables, part}` as
custom metadata. A commit without the txn would be a blind append that never conflicts, so no
other file may call delta-rs write APIs — tests/unit/test_architecture.py enforces it.
"""

from dataclasses import dataclass

from deltalake import CommitProperties, DeltaTable, Transaction

from dataservice.common.errors import FenceError

WRITER_APP_ID = "emr-writer"


@dataclass(frozen=True)
class CommitContext:
    token: int
    op_id: str
    attempt: int
    op_tables: tuple[str, ...]
    part: int = 1

    def commit_properties(self, extra_metadata: dict[str, str] | None = None) -> CommitProperties:
        metadata = {
            "opId": self.op_id,
            "attempt": str(self.attempt),
            "opTables": ",".join(self.op_tables),
            "part": str(self.part),
            **(extra_metadata or {}),
        }
        return CommitProperties(
            app_transactions=[Transaction(WRITER_APP_ID, self.token)],
            custom_metadata=metadata,
            max_commit_retries=0,
        )


def check_fence(table: DeltaTable, token: int) -> None:
    """Abort if any writer has committed to this table with a higher token."""
    seen = table.transaction_version(WRITER_APP_ID)
    if seen is not None and seen > token:
        raise FenceError(f"table fenced by token {seen}; this writer holds {token}")
