"""The writer: runs Delta-job operations under the lease (consistency.md §4–§6).

    drain:   recover, then run the queue's head again and again until nothing is ready
    run:     running (attempt) → the job's fenced commit → committed (with what to publish) → publish
    recover: fence every table → reconcile → roll forward committed operations (publish them,
             never re-run) → roll back the rest (their next attempt re-runs from the payload)

A failing attempt rolls its own commits back and is reported to Node, which retries it until its
attempts run out (then it is failed for good, a poison operation) — so the queue always moves on.
A lost lease or a refused publish propagates out of the drain and stops it.
A wrong payload is not worth retrying (`AppError`); anything else may be transient. A lost lease
(`FenceError`) stops the writer at once without touching the operation: the next holder recovers.
An operation whose publish is refused after `committed` stays at the head and the drain stops
(publish-rejection handling, C4, is not built yet).

First job type: `append_rows` — rows of an import added to the base table.
"""

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import cast

import pyarrow as pa

from dataservice.common.errors import AppError, FenceError, OperationFailedError
from dataservice.storage.fenced_commit import CommitContext, OpRef, append
from dataservice.storage.lease import Lease
from dataservice.storage.operations import (
    Commit,
    Job,
    JobResult,
    OperationAuthority,
    PublishRequest,
    WriterTables,
)
from dataservice.storage.reconcile import data_commits_after, reconcile
from dataservice.storage.staging import read_staged_json

BASE = "base"


@dataclass(frozen=True)
class Recovery:
    rolled_forward: dict[str, int]  # opId → the revision it was published as
    rolled_back: tuple[OpRef, ...]


@dataclass(frozen=True)
class DrainResult:
    recovery: Recovery
    applied: dict[str, int] = field(default_factory=dict)  # opId → revision
    failed: list[str] = field(default_factory=list)  # operations finished as failed


class AttemptFailed(Exception):
    """This attempt failed, its commits are rolled back and Node knows; `final` = no retry left."""

    def __init__(self, op_id: str, final: bool, cause: Exception) -> None:
        super().__init__(f"operation {op_id} attempt failed ({'final' if final else 'will retry'}): {cause}")
        self.op_id, self.final = op_id, final


def _from_json(value: object, arrow_type: pa.DataType) -> object:
    """A JSON value as the column's Arrow type takes it: payloads arrive as JSON, where an instant
    is an ISO string and a day is `yyyy-mm-dd`."""
    if not isinstance(value, str):
        return value
    try:
        if pa.types.is_timestamp(arrow_type):
            return datetime.fromisoformat(value)
        if pa.types.is_date(arrow_type):
            return date.fromisoformat(value)
    except ValueError as error:
        raise AppError(f"not a valid {arrow_type} value: {value!r}", user_safe=False) from error
    return value


def _row_source(payload: Mapping[str, object]) -> Mapping[str, object]:
    """The rows inline, or — for an import, too big for an operation document — staged by Node and
    named by `source` (storage/staging.py). A read failure is transient (retried); a staged file
    that is not `{rows: […]}` is a wrong payload (not retried)."""
    source = payload.get("source")
    if source is None:
        return payload
    if not isinstance(source, str):
        raise AppError("append_rows source must be a URI", user_safe=False)
    staged = read_staged_json(source)
    if not isinstance(staged, Mapping):
        raise AppError(f"staged rows at {source} are not an object", user_safe=False)
    return cast(Mapping[str, object], staged)


def _rows(payload: Mapping[str, object], schema: pa.Schema) -> pa.Table:
    rows = _row_source(payload).get("rows")
    if not isinstance(rows, list) or not all(isinstance(row, Mapping) for row in rows):
        raise AppError("append_rows payload has no rows", user_safe=False)
    unknown = {key for row in rows for key in row} - set(schema.names)
    if unknown:
        raise AppError(f"rows carry columns the dataset does not have: {sorted(unknown)}", user_safe=False)
    types = {name: schema.field(name).type for name in schema.names}
    typed = [{key: _from_json(value, types[key]) for key, value in row.items()} for row in rows]
    try:
        return pa.Table.from_pylist(typed, schema=schema)
    except (pa.ArrowInvalid, pa.ArrowTypeError) as error:
        raise AppError(f"rows do not match the dataset's columns: {error}", user_safe=False) from error


def append_rows(payload: Mapping[str, object], tables: WriterTables, context: CommitContext) -> JobResult:
    table = tables.open(BASE)
    rows = _rows(payload, pa.schema(table.schema().to_arrow()))
    append(table, rows, context)
    fields = tuple(name.removeprefix("f_") for name in rows.schema.names if name.startswith("f_"))
    return JobResult(rows_added=rows.num_rows, changed_field_ids=fields)


_JOBS: dict[str, Job] = {"append_rows": append_rows}


def register_job(name: str, job: Job) -> None:
    """Add a job type (the app registers the ones other modules own, e.g. an upload's ingest)."""
    if name in _JOBS and _JOBS[name] is not job:
        raise ValueError(f"job type {name!r} is already registered")
    _JOBS[name] = job


def _roll_back_own_attempt(dataset_id: str, lease: Lease, tables: WriterTables, authority: OperationAuthority) -> None:
    """Remove whatever the failed attempt committed: reconcile against the manifest (it is the
    only uncommitted work, since operations run one at a time). Nothing committed → nothing to do
    (no needless fence commits)."""
    manifest = authority.manifest(dataset_id)
    versions = {name: manifest.table_versions.get(name, -1) for name in tables.uris}
    if not any(data_commits_after(tables.open(name), versions[name]) for name in tables.uris):
        return
    reconcile(
        {name: tables.open(name) for name in tables.uris},
        versions,
        lease.token,
        {op.ref: op.token for op in authority.committed_operations(dataset_id)},
    )


def run_operation(
    dataset_id: str, op_id: str, op_type: str, lease: Lease, tables: WriterTables, authority: OperationAuthority
) -> int:
    """Run one operation's attempt and publish it; returns the new revision."""
    base_revision = authority.manifest(dataset_id).revision
    attempt = authority.running(op_id, lease.token)
    context = CommitContext(lease.token, (OpRef(op_id, attempt),), tuple(tables.uris))
    try:
        job = _JOBS.get(op_type)
        if job is None:
            raise AppError(f"unknown job type {op_type!r}", user_safe=False)
        result = job(authority.payload(op_id), tables, context)
        versions = {name: tables.open(name).version() for name in tables.uris}
        commit = Commit(base_revision=base_revision, table_versions=versions, result=result)
        authority.committed(op_id, lease.token, attempt, commit)
    except FenceError:
        raise  # the lease is gone: touch nothing, the next holder recovers
    except Exception as error:
        _roll_back_own_attempt(dataset_id, lease, tables, authority)
        message = {"code": "OPERATION_FAILED", "message": str(error)}
        final = authority.failed(op_id, lease.token, attempt, message, retryable=not isinstance(error, AppError))
        raise AttemptFailed(op_id, final, error) from error
    return authority.publish(PublishRequest(op_id, attempt, commit), lease)


def run_append_rows(
    dataset_id: str, op_id: str, lease: Lease, tables: WriterTables, authority: OperationAuthority
) -> int:
    """Append an import's rows to the base table and publish them; returns the new revision."""
    return run_operation(dataset_id, op_id, "append_rows", lease, tables, authority)


def recover(dataset_id: str, lease: Lease, tables: WriterTables, authority: OperationAuthority) -> Recovery:
    """What a new lease holder does before anything else (consistency §4.3)."""
    manifest = authority.manifest(dataset_id)
    committed = authority.committed_operations(dataset_id)
    outcome = reconcile(
        {name: tables.open(name) for name in tables.uris},
        {name: manifest.table_versions.get(name, -1) for name in tables.uris},
        lease.token,
        {op.ref: op.token for op in committed},
    )
    for ref in sorted(outcome.rolled_back_ops, key=lambda ref: ref.op_id):
        authority.rolled_back(ref.op_id, ref.attempt, lease.token)
    published = {}
    for op in committed:
        if op.ref in outcome.roll_forward_ops:
            request = PublishRequest(op.ref.op_id, op.ref.attempt, op.commit, commit_token=op.token)
            published[op.ref.op_id] = authority.publish(request, lease)
    return Recovery(rolled_forward=published, rolled_back=tuple(sorted(outcome.rolled_back_ops, key=lambda r: r.op_id)))


def drain(dataset_id: str, lease: Lease, tables: WriterTables, authority: OperationAuthority) -> DrainResult:
    """Recover, then run the queue in `seq` order until its head is not ready (§4.4)."""
    result = DrainResult(recovery=recover(dataset_id, lease, tables, authority))
    while (queued := authority.next_operation(dataset_id, lease.token)) is not None:
        try:
            result.applied[queued.op_id] = run_operation(
                dataset_id, queued.op_id, queued.op_type, lease, tables, authority
            )
        except AttemptFailed as failed:
            if failed.final:
                result.failed.append(failed.op_id)
            # else Node put it back at the head: the loop runs its next attempt
        except OperationFailedError:
            result.failed.append(queued.op_id)  # its attempts were used up before this run
    return result
