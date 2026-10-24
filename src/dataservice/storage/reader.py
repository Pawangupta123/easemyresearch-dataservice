"""The ONE reader (hybrid edits): grid, analysis, export and compaction all read a revision here.

    rows   = base rows ∪ rows created by insert_row patches
    state  = the latest delete_row / restore_row patch of a row, else the base `_deleted_at`
             (a delete patch's `at` becomes the row's `_deleted_at`, so the trash window survives
             compaction)
    cell   = the latest `set` patch of (row, field) by seq ("null" = cleared), else the base value

Only patches inside the revision's range count, so every reader of revision n sees exactly the
same rows — before and after a compaction folds patches into base. Nothing else may read Delta
data directly (tests/unit/test_architecture.py enforces it).
"""

import duckdb
import pyarrow as pa
from deltalake import DeltaTable, QueryBuilder

from dataservice.storage.snapshot import PATCH_SCHEMA, VALUE_COLUMN, PatchSource, Snapshot

ROW_ID = "_rid"
DELETED_AT = "_deleted_at"


def read_base(snapshot: Snapshot) -> pa.Table:
    table = DeltaTable(snapshot.base_uri, version=snapshot.base_version, storage_options=snapshot.storage_options)
    return pa.table(QueryBuilder().register("base", table).execute("select * from base").read_all())


def read_rows(snapshot: Snapshot, patch_source: PatchSource, *, include_deleted: bool = False) -> pa.Table:
    base = read_base(snapshot)
    patches = patch_source.patches(snapshot.dataset_id, snapshot.patch_from, snapshot.patch_to)
    if patches.schema != PATCH_SCHEMA:
        raise ValueError("patch source returned an unexpected schema")
    return merge(base, patches, snapshot, include_deleted=include_deleted)


def merge(base: pa.Table, patches: pa.Table, snapshot: Snapshot, *, include_deleted: bool = False) -> pa.Table:
    """Base + patches → the revision's rows, ordered by row id. Pure: no I/O.
    `include_deleted` also returns trashed rows and their `_deleted_at` (compaction, trash view)."""
    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'")  # instants stay UTC whatever the host's zone
    con.register("base", base)
    con.register("patches", patches)

    cells, selects = [], [f'r."{ROW_ID}"']
    for index, spec in enumerate(snapshot.fields):
        alias = f"c{index}"
        value = VALUE_COLUMN[spec.value_type]
        cells.append(
            f"{alias} AS (SELECT row, arg_max({{'t': t, 'v': {value}}}, seq) AS c FROM p"
            f" WHERE op = 'set' AND field = '{spec.field_id}' GROUP BY row)"
        )
        selects.append(
            f"CASE WHEN {alias}.c IS NULL THEN b.\"{spec.column}\" WHEN {alias}.c.t = 'null' THEN NULL"
            f' ELSE {alias}.c.v END AS "{spec.column}"'
        )
    joins = "".join(f' LEFT JOIN c{index} ON c{index}.row = r."{ROW_ID}"' for index in range(len(snapshot.fields)))
    if include_deleted:
        selects.append(f'r.deleted_at AS "{DELETED_AT}"')
    deleted_filter = "" if include_deleted else " WHERE NOT r.deleted"
    sql = f"""
        WITH p AS (SELECT * FROM patches WHERE seq > {int(snapshot.patch_from)} AND seq <= {int(snapshot.patch_to)}),
        state AS (
            SELECT row, arg_max({{'op': op, 'at': "at"}}, seq) AS last FROM p  -- AT is a DuckDB keyword
            WHERE op IN ('delete_row', 'restore_row') GROUP BY row
        ),
        inserted AS (
            SELECT DISTINCT row AS "{ROW_ID}" FROM p WHERE op = 'insert_row'
            AND row NOT IN (SELECT "{ROW_ID}" FROM base)
        ),
        all_rows AS (
            SELECT "{ROW_ID}", CAST("{DELETED_AT}" AS TIMESTAMPTZ) AS base_deleted_at FROM base
            UNION ALL SELECT "{ROW_ID}", CAST(NULL AS TIMESTAMPTZ) FROM inserted
        ),
        r AS (
            SELECT a."{ROW_ID}",
                CASE WHEN s.last IS NULL THEN a.base_deleted_at
                     WHEN s.last.op = 'delete_row' THEN s.last.at
                     ELSE CAST(NULL AS TIMESTAMPTZ) END AS deleted_at,
                COALESCE(s.last.op = 'delete_row', a.base_deleted_at IS NOT NULL) AS deleted
            FROM all_rows a LEFT JOIN state s ON s.row = a."{ROW_ID}"
        ){"".join("," + cell for cell in cells)}
        SELECT {", ".join(selects)}
        FROM r LEFT JOIN base b ON b."{ROW_ID}" = r."{ROW_ID}"{joins}{deleted_filter}
        ORDER BY r."{ROW_ID}"
    """
    # con.sql(...) streams straight to Arrow; execute() would copy every row once more (DuckDB 2.0).
    return con.sql(sql).to_arrow_table()
