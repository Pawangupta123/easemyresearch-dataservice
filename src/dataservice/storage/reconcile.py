"""Acquire = fence every table, then reconcile (consistency.md §4.3).

A new lease holder finds what the previous writer left between each table's manifest version
and now, and decides per commit:

- every operation in it is `committed` in Node at this attempt and with this commit's token →
  **roll forward** (Node publishes it before anything new);
- otherwise → **roll back**: a fenced RESTORE to just before it, and its operations retry.

Commits are processed in order, so the roll-forward commits must be a prefix: a committed commit
after one that is rolled back would mean the writer broke the protocol — reconciliation stops
instead of guessing.
"""

from collections.abc import Mapping
from dataclasses import dataclass

from deltalake import DeltaTable

from dataservice.common.errors import AppError
from dataservice.storage.fenced_commit import OpRef, fence, parse_ops, restore

NON_DATA_KINDS = frozenset({"fence", "reconcile"})


class ReconcileError(AppError):
    """The log contradicts the protocol; a person must look (never guessed around)."""


@dataclass(frozen=True)
class Commit:
    version: int
    token: int | None
    ops: tuple[OpRef, ...]


@dataclass(frozen=True)
class TablePlan:
    roll_forward: tuple[Commit, ...]
    rollback_to: int | None
    rolled_back: tuple[OpRef, ...]


@dataclass(frozen=True)
class ReconcileResult:
    """What Node must do next: publish `roll_forward_ops` at `roll_forward_versions`; mark
    `rolled_back_ops` rolled back and retry them with the next attempt."""

    roll_forward_versions: dict[str, int]
    roll_forward_ops: frozenset[OpRef]
    rolled_back_ops: frozenset[OpRef]


def data_commits_after(table: DeltaTable, manifest_version: int) -> list[Commit]:
    """The table's effective data commits after the manifest version, oldest first. Fence
    commits carry no data; a reconcile RESTORE voids every commit after its target."""
    effective: list[Commit] = []
    for entry in sorted(table.history(), key=lambda item: int(item["version"])):
        version = int(entry["version"])
        if version <= manifest_version:
            continue
        kind = entry.get("kind")
        if kind == "reconcile":
            restored_to = int(entry["restoredTo"])
            effective = [commit for commit in effective if commit.version <= restored_to]
            continue
        if kind in NON_DATA_KINDS:
            continue
        token = entry.get("token")
        effective.append(
            Commit(
                version=version, token=int(token) if token is not None else None, ops=parse_ops(entry.get("ops", ""))
            )
        )
    return effective


def plan_table(commits: list[Commit], manifest_version: int, committed: Mapping[OpRef, int]) -> TablePlan:
    """`committed` maps each operation Node has marked committed (at that attempt) to the token
    recorded on it."""

    def is_committed(commit: Commit) -> bool:
        return bool(commit.ops) and all(committed.get(op) == commit.token for op in commit.ops)

    prefix = 0
    while prefix < len(commits) and is_committed(commits[prefix]):
        prefix += 1
    roll_forward, rest = tuple(commits[:prefix]), commits[prefix:]
    if any(is_committed(commit) for commit in rest):
        raise ReconcileError("a committed commit follows an uncommitted one", user_safe=False)
    if not rest:
        return TablePlan(roll_forward=roll_forward, rollback_to=None, rolled_back=())
    rollback_to = roll_forward[-1].version if roll_forward else manifest_version
    rolled_back = tuple(dict.fromkeys(op for commit in rest for op in commit.ops))
    return TablePlan(roll_forward=roll_forward, rollback_to=rollback_to, rolled_back=rolled_back)


def reconcile(
    tables: Mapping[str, DeltaTable],
    manifest_versions: Mapping[str, int],
    token: int,
    committed: Mapping[OpRef, int],
) -> ReconcileResult:
    for table in tables.values():  # step 1: fence EVERY table first (C2b)
        fence(table, token)
    forward_versions: dict[str, int] = {}
    forward_ops: set[OpRef] = set()
    rolled_back: set[OpRef] = set()
    for name, table in tables.items():
        manifest_version = manifest_versions[name]
        plan = plan_table(data_commits_after(table, manifest_version), manifest_version, committed)
        if plan.roll_forward:
            forward_versions[name] = plan.roll_forward[-1].version
            forward_ops.update(op for commit in plan.roll_forward for op in commit.ops)
        if plan.rollback_to is not None:
            restore(table, plan.rollback_to, token, plan.rolled_back)
            rolled_back.update(plan.rolled_back)
    # An operation is either published everywhere or retried everywhere — never half of each.
    if forward_ops & rolled_back:
        raise ReconcileError("an operation is committed in one table and rolled back in another", user_safe=False)
    return ReconcileResult(forward_versions, frozenset(forward_ops), frozenset(rolled_back))
