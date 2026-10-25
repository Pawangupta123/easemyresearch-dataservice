"""Follow-up visits through the one reader — the same behaviour as Node's records today.

A visit row `(_rid, _follow_up)` exists once the visit is submitted (Node marks every entry of a
submitted visit `completed`; "pending" is never stored — the schedule works it out at read):
    visits = the followups table's rows ∪ visits created by `insert_row` patches of that visit
    cell   = the latest `set` patch of (row, visit, field) by seq ("null" = cleared), else stored
A visit is shown only while its main row is live — trashing a row hides all its visits, as in
Node, so delete / restore patches never address a visit (the reader refuses them). Follow-up
entries share the main form's field ids; their patches carry the visit number, so a cell of
visit 1 and the same field in visit 2 or the main visit are three different targets (C12).
"""

from collections.abc import Sequence

import duckdb
import pyarrow as pa

from dataservice.common.errors import ValidationError
from dataservice.storage.reader.engine import (
    connection,
    register_base,
    register_base_rows,
    register_patches,
    register_visit_rows,
    register_visits,
)
from dataservice.storage.reader.model import ROW_ID, VISIT
from dataservice.storage.reader.sql import merged_sql, summarise_patches
from dataservice.storage.snapshot import MAIN_VISIT, VALUE_COLUMN, FieldSpec, PatchSource, Snapshot


def query_visits(
    snapshot: Snapshot,
    patch_source: PatchSource,
    follow_up: int | None = None,
    fields: tuple[str, ...] | None = None,
) -> pa.Table:
    """The follow-up visits of the revision's live rows — one visit number, or all — with the
    asked visit fields (None = every visit field), ordered by row id then visit."""
    chosen = _chosen(snapshot, follow_up, fields)
    with connection() as con:
        register_base(con, snapshot)
        register_visits(con, snapshot)
        register_patches(con, patch_source.patches(snapshot.dataset_id, snapshot.patch_from, snapshot.patch_to))
        return _select(con, snapshot, chosen, follow_up)


def merge_visits(
    base: pa.Table,
    visits: pa.Table,
    patches: pa.Table,
    snapshot: Snapshot,
    follow_up: int | None = None,
    fields: tuple[str, ...] | None = None,
) -> pa.Table:
    """`query_visits` over rows already in memory. Pure: no I/O."""
    chosen = _chosen(snapshot, follow_up, fields)
    with connection() as con:
        register_base_rows(con, base, snapshot.fields)
        register_visit_rows(con, visits, snapshot.visit_fields)
        register_patches(con, patches)
        return _select(con, snapshot, chosen, follow_up)


def _chosen(snapshot: Snapshot, follow_up: int | None, fields: tuple[str, ...] | None) -> tuple[FieldSpec, ...]:
    if follow_up is not None and (
        isinstance(follow_up, bool) or not isinstance(follow_up, int) or follow_up <= MAIN_VISIT
    ):
        raise ValidationError("A follow-up visit number is a whole number from 1")
    by_id = {spec.field_id: spec for spec in snapshot.visit_fields}
    if fields is None:
        return snapshot.visit_fields
    unknown = [field_id for field_id in fields if field_id not in by_id]
    if unknown:
        raise ValidationError(f"Unknown follow-up fields: {', '.join(unknown)}")
    return tuple(by_id[field_id] for field_id in dict.fromkeys(fields))


def _select(
    con: duckdb.DuckDBPyConnection, snapshot: Snapshot, chosen: Sequence[FieldSpec], follow_up: int | None
) -> pa.Table:
    live_rows = merged_sql(summarise_patches(con, snapshot), (), include_deleted=False)  # also builds `pv`
    only = "" if follow_up is None else f" AND follow_up = {int(follow_up)}"
    found = con.execute(f"SELECT list(DISTINCT field) FILTER (WHERE op = 'set') FROM pv WHERE true{only}").fetchall()
    touched = frozenset(found[0][0] or ())
    patched = [(index, spec) for index, spec in enumerate(chosen) if spec.field_id in touched]
    edits = [
        f"arg_max({{'t': t, 'v': {VALUE_COLUMN[spec.value_type]}}}, seq)"
        f" FILTER (WHERE op = 'set' AND field = '{spec.field_id}') AS c{index}"
        for index, spec in patched
    ]
    ctes = [f"vp AS (SELECT * FROM pv WHERE true{only})"]
    if edits:
        ctes.append(f"ve AS (SELECT row, follow_up, {', '.join(edits)} FROM vp GROUP BY row, follow_up)")
    ctes += [
        "vins AS (SELECT DISTINCT row, follow_up FROM vp WHERE op = 'insert_row')",
        f'vnew AS (SELECT row AS "{ROW_ID}", follow_up AS "{VISIT}" FROM vins WHERE NOT EXISTS'
        f' (SELECT 1 FROM visits v WHERE v."{ROW_ID}" = vins.row AND v."{VISIT}" = vins.follow_up))',
    ]
    stored = ", ".join([f'"{ROW_ID}"', f'CAST("{VISIT}" AS INTEGER) AS "{VISIT}"', *(f'"{s.column}"' for s in chosen)])
    created = ", ".join([f'"{ROW_ID}"', f'"{VISIT}"', *(f'NULL AS "{s.column}"' for s in chosen)])
    visit_filter = "" if follow_up is None else f' WHERE "{VISIT}" = {int(follow_up)}'
    ctes.append(f"vall AS (SELECT {stored} FROM visits{visit_filter} UNION ALL SELECT {created} FROM vnew)")
    columns = [f'a."{ROW_ID}"', f'a."{VISIT}"']
    for index, spec in enumerate(chosen):
        plain = f'a."{spec.column}"'
        if spec.field_id in touched:
            plain = (
                f"CASE WHEN ve.c{index} IS NULL THEN {plain} WHEN ve.c{index}.t = 'null' THEN NULL"
                f" ELSE ve.c{index}.v END"
            )
        columns.append(f'{plain} AS "{spec.column}"')
    join = f' LEFT JOIN ve ON ve.row = a."{ROW_ID}" AND ve.follow_up = a."{VISIT}"' if edits else ""
    sql = (
        f"WITH {', '.join(ctes)} SELECT {', '.join(columns)} FROM vall a{join}"
        f' WHERE a."{ROW_ID}" IN (SELECT "{ROW_ID}" FROM ({live_rows}) live)'
        f' ORDER BY a."{ROW_ID}", a."{VISIT}"'
    )
    return con.sql(sql).to_arrow_table()
