"""Compaction: fold a revision's patches into its Delta base (consistency.md §15.2).

Reads revision n through the one reader (base + patches up to W = n's `toSeq`), writes the
result as the new base through the fenced gate, and hands Node what to publish: the new base
version and the watermark W. Values never change — the published revision reads identically
(C39) — so Node keeps `dataVersion`, marks nothing stale and recomputes nothing.

Refuses rather than guesses: if the base got data commits since n (another base rewrite), or if the merged
rows would drop a base column, nothing is written.
"""

from dataclasses import dataclass

import pyarrow as pa
import pyarrow.types as pa_types
from deltalake import DeltaTable

from dataservice.common.errors import ConflictError
from dataservice.storage.fenced_commit import CommitContext, overwrite
from dataservice.storage.reader import merge, read_base
from dataservice.storage.reconcile import data_commits_after
from dataservice.storage.snapshot import PATCH_SCHEMA, PatchSource, Snapshot


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


def compact(snapshot: Snapshot, patch_source: PatchSource, context: CommitContext) -> CompactionResult:
    table = DeltaTable(snapshot.base_uri, storage_options=snapshot.storage_options)
    # Fence and reconcile commits carry no data; any other commit since the base means another
    # base rewrite landed — the rebase rule refuses (consistency §4.5).
    moved = data_commits_after(table, snapshot.base_version)
    if moved:
        raise ConflictError(f"base rewritten since version {snapshot.base_version}; re-prepare", user_safe=False)
    base = read_base(snapshot)
    patches = patch_source.patches(snapshot.dataset_id, snapshot.patch_from, snapshot.patch_to)
    if patches.schema != PATCH_SCHEMA:
        raise ValueError("patch source returned an unexpected schema")
    merged = merge(base, patches, snapshot, include_deleted=True)
    # Compare with the Delta table's own schema, not the in-memory read (DataFusion hands out
    # string_view where Delta stores string).
    expected = pa.schema(table.schema().to_arrow())
    missing = set(expected.names) - set(merged.schema.names)
    if missing:
        raise ValueError(f"compaction would drop base columns {sorted(missing)}; the snapshot lists every field")
    folded = merged.select(expected.names)
    # Types must already match: a silent cast could change values (and pyarrow's cast needs its
    # compute library, which Windows Application Control blocks on the dev box).
    drifted = [
        f"{name}: {folded.schema.field(name).type} != {expected.field(name).type}"
        for name in expected.names
        if not _same_type(folded.schema.field(name).type, expected.field(name).type)
    ]
    if drifted:
        raise ValueError(f"merged rows changed column types: {drifted}")
    overwrite(table, folded, context)
    written = DeltaTable(snapshot.base_uri, storage_options=snapshot.storage_options).version()
    return CompactionResult(base_version=written, folded_to=snapshot.patch_to, rows=folded.num_rows)
