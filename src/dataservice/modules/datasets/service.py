"""The query service (api.md §2.1): a page, a count and an edit's before-values of one published
revision, all through the one reader (consistency §15.1).

The caller never names a table: its URI comes from the lake root and the dataset id, so a
manifest can only point at this dataset's own tables at its own versions.
"""

import re
from collections.abc import Iterable
from datetime import date, datetime

from dataservice.common.errors import ForbiddenError
from dataservice.common.utils.dataset_token import ALL_ROWS, DatasetClaims
from dataservice.modules.datasets.schemas import (
    OBJECT_ID,
    Cell,
    CellsRead,
    CellsReadRequest,
    CellValue,
    FieldDto,
    ManifestDto,
    Row,
    RowsCount,
    RowsPage,
    RowsQueryRequest,
    RowState,
)
from dataservice.storage.reader import Query, count, query
from dataservice.storage.reader.model import CREATED_AT, CREATED_BY, DELETED_AT, ROW_ID, UPDATED_AT
from dataservice.storage.snapshot import FieldSpec, PatchSource, Snapshot, TableVersion

_DATASET_ID = re.compile(OBJECT_ID)


def table_uri(lake_root: str, dataset_id: str, table: str) -> str:
    """`<lake_root>/datasets/<datasetId>/<table>` — only for a well-formed dataset id."""
    if not _DATASET_ID.fullmatch(dataset_id):
        raise ValueError(f"not a dataset id: {dataset_id!r}")
    return f"{lake_root.rstrip('/')}/datasets/{dataset_id}/{table}"


def _specs(fields: Iterable[FieldDto]) -> tuple[FieldSpec, ...]:
    return tuple(FieldSpec(item.field_id, item.value_type) for item in fields)


def snapshot_from_manifest(lake_root: str, dataset_id: str, manifest: ManifestDto) -> Snapshot:
    """The revision a manifest names, as the reader reads it. Pure: no I/O."""
    followups = manifest.tables.followups
    return Snapshot(
        dataset_id=dataset_id,
        base_uri=table_uri(lake_root, dataset_id, "table"),
        base_version=manifest.tables.base.version,
        patch_from=manifest.patches.from_seq,
        patch_to=manifest.patches.to_seq,
        fields=_specs(manifest.fields),
        followups=None
        if followups is None
        else TableVersion(table_uri(lake_root, dataset_id, "followups"), followups.version),
        visit_fields=_specs(manifest.visit_fields),
    )


def _every_row(claims: DatasetClaims) -> None:
    # The grid shows a form's rows to every viewer (as Node does today); only an edit is scoped.
    if claims.row_scope != ALL_ROWS:
        raise ForbiddenError("A row-scoped token can't list or count rows")


def _json(value: object) -> CellValue:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, float | str) or value is None:
        return value
    if isinstance(value, list):
        return [str(item) for item in value]
    raise TypeError(f"unexpected cell value {value!r}")


def _instant(value: object) -> datetime | None:
    return value if isinstance(value, datetime) else None


def _text(value: object) -> str | None:
    return value if isinstance(value, str) else None


def query_rows(
    lake_root: str, dataset_id: str, request: RowsQueryRequest, claims: DatasetClaims, patches: PatchSource
) -> RowsPage:
    _every_row(claims)
    snapshot = snapshot_from_manifest(lake_root, dataset_id, request.manifest)
    columns = None if request.columns is None else tuple(request.columns)
    asked = Query(fields=columns, limit=request.limit, offset=request.offset, meta=True, row_order=request.row_order)
    table = query(snapshot, patches, asked)
    returned = snapshot.fields if columns is None else [spec for spec in snapshot.fields if spec.field_id in columns]
    rows = [
        Row(
            _rid=str(record[ROW_ID]),
            created_by=_text(record[CREATED_BY]),
            created_at=_instant(record[CREATED_AT]),
            updated_at=_instant(record[UPDATED_AT]),
            values={spec.field_id: _json(record[spec.column]) for spec in returned},
        )
        for record in table.to_pylist()
    ]
    return RowsPage(revision=request.manifest.revision, rows=rows)


def count_rows(
    lake_root: str, dataset_id: str, manifest: ManifestDto, claims: DatasetClaims, patches: PatchSource
) -> RowsCount:
    _every_row(claims)
    return RowsCount(
        revision=manifest.revision, count=count(snapshot_from_manifest(lake_root, dataset_id, manifest), patches)
    )


def read_cells(
    lake_root: str, dataset_id: str, request: CellsReadRequest, claims: DatasetClaims, patches: PatchSource
) -> CellsRead:
    """The before-values of an edit (consistency §14.2), trashed rows included (their state
    decides the edit). A row-scoped caller sees only the rows it created — as Node's records
    lookup for a collaborator does — so another user's row reads as absent."""
    snapshot = snapshot_from_manifest(lake_root, dataset_id, request.manifest)
    fields = tuple(dict.fromkeys(target.field for target in request.targets))
    rids = tuple(dict.fromkeys(target.row for target in request.targets))
    table = query(snapshot, patches, Query(fields=fields, rids=rids, include_deleted=True, meta=True))
    column = {spec.field_id: spec.column for spec in snapshot.fields}
    found = {
        str(record[ROW_ID]): record
        for record in table.to_pylist()
        if claims.row_scope == ALL_ROWS or record[CREATED_BY] == claims.row_scope
    }
    cells = [
        Cell(target=target, value=_json(found[target.row][column[target.field]]))
        for target in request.targets
        if target.row in found
    ]
    rows = [
        RowState(
            _rid=rid,
            state="live" if record[DELETED_AT] is None else "deleted",
            created_by=_text(record[CREATED_BY]),
        )
        for rid, record in found.items()
    ]
    return CellsRead(revision=request.manifest.revision, cells=cells, rows=rows)
