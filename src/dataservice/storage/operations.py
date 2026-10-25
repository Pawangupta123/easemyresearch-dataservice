"""The Delta-job operation contract as the Data Service's writer sees it (consistency.md §4–§6).

Node owns `data_operations` and the publish transaction and serves them on its internal
endpoints; the writer only calls them. Implemented by the Node client in production and by the
reference Node side (prototype/node_side) in tests — like `LeaseAuthority` for the lease.

Every call is checked against the dataset's live lease; a lost lease raises `FenceError`, and
the writer stops at once (the next holder's recovery takes over).
"""

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Protocol

from dataservice.storage.fenced_commit import OpRef
from dataservice.storage.lease import Lease


@dataclass(frozen=True)
class JobResult:
    rows_added: int = 0
    rows_changed: int = 0
    # Analyses using these fields go stale at publish (an append changes every field's data).
    changed_field_ids: tuple[str, ...] = ()
    # Fields whose EXISTING values the job rewrote — a small edit on one of them since the job's
    # base revision is a conflict (rebase rule); an append touches none.
    touched_field_ids: tuple[str, ...] = ()


@dataclass(frozen=True)
class Commit:
    """What `committed` records, so a later lease holder can publish it without re-running it."""

    base_revision: int
    table_versions: Mapping[str, int]
    result: JobResult = field(default_factory=JobResult)


@dataclass(frozen=True)
class CommittedOp:
    ref: OpRef
    token: int  # the token that committed it — a roll-forward is checked against this one
    commit: Commit


@dataclass(frozen=True)
class QueuedOp:
    op_id: str
    op_type: str


@dataclass(frozen=True)
class Manifest:
    revision: int
    table_versions: Mapping[str, int]


@dataclass(frozen=True)
class PublishRequest:
    op_id: str
    attempt: int
    commit: Commit
    # Set when a new lease holder rolls forward an operation an earlier holder committed:
    # the operation is checked against this token, the lease against the publisher's own.
    commit_token: int | None = None


class OperationAuthority(Protocol):
    def manifest(self, dataset_id: str) -> Manifest: ...

    def next_operation(self, dataset_id: str) -> QueuedOp | None:
        """The queue's head when it is ready to run (`dispatched`); None when the queue is empty or
        its head is not ready — nothing behind the head ever runs first."""
        ...

    def payload(self, op_id: str) -> Mapping[str, object]: ...

    def running(self, op_id: str, token: int) -> int:
        """→ running; returns the attempt this writer runs (the same one on a duplicate call)."""
        ...

    def committed(self, op_id: str, token: int, attempt: int, commit: Commit) -> None: ...

    def failed(self, op_id: str, token: int, attempt: int, error: Mapping[str, str], retryable: bool) -> bool:
        """The writer gave this attempt up and rolled its commits back. Node retries it while it is
        retryable and attempts are left; returns True when the operation is finished (failed)."""
        ...

    def committed_operations(self, dataset_id: str) -> list[CommittedOp]:
        """Operations committed but not yet published — reconciliation rolls these forward."""
        ...

    def rolled_back(self, op_id: str, attempt: int, token: int) -> None:
        """Reconciliation removed this attempt's commits: remember it, make the operation runnable
        again (the next `running` is attempt + 1)."""
        ...

    def publish(self, request: PublishRequest, lease: Lease) -> int:
        """Node's publish transaction; returns the new revision (the same one on a retry)."""
        ...
