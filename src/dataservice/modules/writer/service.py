"""The writer: runs Delta-job operations under the lease (consistency.md §4–§6).

    run:     running (attempt) → fenced commit → committed (with what to publish) → publish
    recover: fence every table → reconcile → roll forward committed operations (publish them,
             never re-run) → roll back the rest (their next attempt re-runs from the payload)

Node answers the caller only once the publish has applied an operation. A failure before
`committed` reports the attempt failed; once `committed`, the operation is either published or
left for the next lease holder's recovery to roll forward.

First job type: `append_rows` — rows of an import added to the base table.
"""

from collections.abc import Mapping
from dataclasses import dataclass, field

import pyarrow as pa
from deltalake import DeltaTable

from dataservice.common.errors import AppError
from dataservice.storage.fenced_commit import CommitContext, OpRef, append
from dataservice.storage.lease import Lease
from dataservice.storage.operations import Commit, JobResult, OperationAuthority, PublishRequest
from dataservice.storage.reconcile import reconcile

BASE = "base"


@dataclass(frozen=True)
class WriterTables:
    """Where the dataset's Delta tables live (one base table in the prototype)."""

    uris: Mapping[str, str]
    storage_options: Mapping[str, str] = field(default_factory=dict)

    def open(self, name: str) -> DeltaTable:
        return DeltaTable(self.uris[name], storage_options=dict(self.storage_options))


@dataclass(frozen=True)
class Recovery:
    rolled_forward: dict[str, int]  # opId → the revision it was published as
    rolled_back: tuple[OpRef, ...]


def _rows(payload: Mapping[str, object], schema: pa.Schema) -> pa.Table:
    rows = payload.get("rows")
    if not isinstance(rows, list) or not all(isinstance(row, Mapping) for row in rows):
        raise AppError("append_rows payload has no rows", user_safe=False)
    unknown = {key for row in rows for key in row} - set(schema.names)
    if unknown:
        raise AppError(f"rows carry columns the dataset does not have: {sorted(unknown)}", user_safe=False)
    return pa.Table.from_pylist(list(rows), schema=schema)


def run_append_rows(
    dataset_id: str, op_id: str, lease: Lease, tables: WriterTables, authority: OperationAuthority
) -> int:
    """Append an import's rows to the base table and publish them; returns the new revision."""
    base_revision = authority.manifest(dataset_id).revision
    attempt = authority.running(op_id, lease.token)
    try:
        table = tables.open(BASE)
        rows = _rows(authority.payload(op_id), pa.schema(table.schema().to_arrow()))
        append(table, rows, CommitContext(lease.token, (OpRef(op_id, attempt),), (BASE,)))
    except Exception as error:
        authority.failed(op_id, lease.token, attempt, {"code": "OPERATION_FAILED", "message": str(error)}, False)
        raise
    fields = tuple(name.removeprefix("f_") for name in rows.schema.names if name.startswith("f_"))
    commit = Commit(
        base_revision=base_revision,
        table_versions={BASE: tables.open(BASE).version()},
        result=JobResult(rows_added=rows.num_rows, changed_field_ids=fields),
    )
    authority.committed(op_id, lease.token, attempt, commit)
    return authority.publish(PublishRequest(op_id, attempt, commit), lease)


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
