"""Compaction: fold a revision's patches into its Delta base (consistency.md §15.2).

Streams revision n through the one reader (base + patches up to W = n's `toSeq`) straight into
the new base through the fenced gate — batch by batch, so memory stays flat at any size — and
hands Node what to publish: the new base version and the watermark W. Values never change — the
published revision reads identically (C39) — so Node keeps `dataVersion`, marks nothing stale and
recomputes nothing.

Refuses rather than guesses: if the base got data commits since n (another base rewrite), or if
the merged rows would drop or retype a base column, nothing is written.
"""

from collections.abc import Iterator
from dataclasses import dataclass

import pyarrow as pa
import pyarrow.types as pa_types
from deltalake import DeltaTable
from deltalake.exceptions import CommitFailedError

from dataservice.common.errors import ConflictError
from dataservice.storage.fenced_commit import CommitContext, overwrite
from dataservice.storage.reader import scan
from dataservice.storage.reconcile import data_commits_after
from dataservice.storage.snapshot import PatchSource, Snapshot


@dataclass(frozen=True)
class CompactionResult:
    base_version: int  # the new base's Delta version
    folded_to: int  # W: patches with seq ≤ W are now in the base
    rows: int  # rows written, trashed ones included


def _same_type(actual: pa.DataType, expected: pa.DataType) -> bool:
    """Equal types; a list's child field name (`item` / `element`) does not matter."""
    if pa_types.is_list(actual) and pa_types.is_list(expected):
        return _same_type(actual.value_type, expected.value_type)
    return bool(actual == expected)


class _Fetched:
    """The revision's patches, fetched once for the check and the fold."""

    def __init__(self, table: pa.Table) -> None:
        self.table = table

    def patches(self, dataset_id: str, from_seq: int, to_seq: int) -> pa.Table:
        return self.table


def compact(snapshot: Snapshot, patch_source: PatchSource, context: CommitContext) -> CompactionResult:
    table = DeltaTable(snapshot.base_uri, storage_options=snapshot.storage_options)
    # Fence and reconcile commits carry no data; any other commit since the base means another
    # base rewrite landed — the rebase rule refuses (consistency §4.5).
    moved = data_commits_after(table, snapshot.base_version)
    if moved:
        raise ConflictError(f"base rewritten since version {snapshot.base_version}; re-prepare", user_safe=False)
    expected = pa.schema(table.schema().to_arrow())  # the Delta table's own schema
    patches = _Fetched(patch_source.patches(snapshot.dataset_id, snapshot.patch_from, snapshot.patch_to))
    if any(visit for visit in patches.table.column("follow_up").to_pylist() if visit):
        # Publishing W would drop them: only the base is folded here, not the followups table.
        raise ValueError("the revision has follow-up visit patches; folding the followups table is not built yet")
    written_rows = 0
    with scan(snapshot, patches, include_deleted=True) as merged:
        missing = set(expected.names) - set(merged.schema.names)
        if missing:
            raise ValueError(f"compaction would drop base columns {sorted(missing)}; the snapshot lists every field")
        unstored = set(merged.schema.names) - set(expected.names)
        if unstored:  # the field's schema change must add the column first, or its values would be lost
            raise ValueError(f"fields {sorted(unstored)} have no column in the base table yet")
        # Types must already match: a silent cast could change values (and pyarrow's cast needs
        # its compute library, which Windows Application Control blocks on the dev box).
        drifted = [
            f"{name}: {merged.schema.field(name).type} != {expected.field(name).type}"
            for name in expected.names
            if not _same_type(merged.schema.field(name).type, expected.field(name).type)
        ]
        if drifted:
            raise ValueError(f"merged rows changed column types: {drifted}")
        folded_schema = pa.schema([merged.schema.field(name) for name in expected.names])

        def batches() -> Iterator[pa.RecordBatch]:
            nonlocal written_rows
            for batch in merged:
                written_rows += batch.num_rows
                yield batch.select(expected.names)

        try:
            overwrite(table, pa.RecordBatchReader.from_batches(folded_schema, batches()), context)
        except CommitFailedError as error:  # C25: a base rewrite (a transform) committed meanwhile
            raise ConflictError("base rewritten during compaction; re-prepare", user_safe=False) from error
    written = DeltaTable(snapshot.base_uri, storage_options=snapshot.storage_options).version()
    return CompactionResult(base_version=written, folded_to=snapshot.patch_to, rows=written_rows)
