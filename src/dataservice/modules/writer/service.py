"""The writer: runs one Delta-job operation under the lease (consistency.md §4–§6).

    running (attempt) → fenced commit → committed → publish

Node answers the caller only once the publish has applied it. If anything fails before
`committed`, the attempt is reported failed; once `committed`, the operation is either published
or left for the next lease holder's reconciliation to roll forward (it never re-runs).

First job type: `append_rows` — rows of an import added to the base table.
"""

from collections.abc import Mapping
from dataclasses import dataclass

import pyarrow as pa
from deltalake import DeltaTable

from dataservice.common.errors import AppError
from dataservice.storage.fenced_commit import CommitContext, OpRef, append
from dataservice.storage.lease import Lease
from dataservice.storage.operations import JobResult, OperationAuthority, PublishRequest

ROW_ID = "_rid"


@dataclass(frozen=True)
class WriterTables:
    base_uri: str
    storage_options: Mapping[str, str]


def _rows(payload: Mapping[str, object], schema: pa.Schema) -> pa.Table:
    rows = payload.get("rows")
    if not isinstance(rows, list) or not all(isinstance(row, Mapping) for row in rows):
        raise AppError("append_rows payload has no rows", user_safe=False)
    unknown = {key for row in rows for key in row} - set(schema.names)
    if unknown:
        raise AppError(f"rows carry columns the dataset does not have: {sorted(unknown)}", user_safe=False)
    return pa.Table.from_pylist(list(rows), schema=schema)


def run_append_rows(
    op_id: str,
    base_revision: int,
    lease: Lease,
    tables: WriterTables,
    authority: OperationAuthority,
) -> int:
    """Append an import's rows to the base table and publish them; returns the new revision."""
    attempt = authority.running(op_id, lease.token)
    try:
        table = DeltaTable(tables.base_uri, storage_options=dict(tables.storage_options))
        rows = _rows(authority.payload(op_id), pa.schema(table.schema().to_arrow()))
        append(table, rows, CommitContext(lease.token, (OpRef(op_id, attempt),), ("base",)))
    except Exception as error:
        authority.failed(op_id, lease.token, attempt, {"code": "OPERATION_FAILED", "message": str(error)}, False)
        raise
    version = DeltaTable(tables.base_uri, storage_options=dict(tables.storage_options)).version()
    authority.committed(op_id, lease.token, attempt, {"base": version})
    fields = tuple(name.removeprefix("f_") for name in rows.schema.names if name.startswith("f_"))
    result = JobResult(rows_added=rows.num_rows, changed_field_ids=fields, touched_field_ids=())
    return authority.publish(PublishRequest(op_id, attempt, base_revision, {"base": version}, result), lease)
