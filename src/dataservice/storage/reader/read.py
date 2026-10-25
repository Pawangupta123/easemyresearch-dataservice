"""The one reader's entry points: a page, a count, a full stream, a plan.

A request is checked before any I/O. A page whose full-width rows would meet patches (or a deep
page) is read in two passes: pass 1 finds its row ids from the sort and filter columns alone,
pass 2 reads only those rows in full and lays the patches over them. Otherwise one pass, where
DuckDB itself reads the other columns for the page's rows only (late materialization).
"""

from collections.abc import Generator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass

import duckdb
import pyarrow as pa

from dataservice.common.errors import ValidationError
from dataservice.storage.reader.engine import connection, register_base, register_base_rows, register_patches
from dataservice.storage.reader.model import DELETED_AT, EVERY_ROW, ROW_ID, SCAN_BATCH_ROWS, Query
from dataservice.storage.reader.sql import (
    PAGE_IDS,
    Params,
    Touched,
    bounded,
    merged_sql,
    specs,
    summarise_patches,
    where,
)
from dataservice.storage.snapshot import FieldSpec, PatchSource, Snapshot

# Pages up to this many rows may be read in two passes; a bigger result is read in one.
PAGE_ROWS_MAX = 5_000
# From this offset on, a page is read in two passes even without patches: one pass would carry
# every skipped row at full width through the sort.
DEEP_OFFSET = 10_000


def query(snapshot: Snapshot, patch_source: PatchSource, request: Query = EVERY_ROW) -> pa.Table:
    """The revision's rows that match `request` — one page of them when it has a limit."""
    plan = _plan(snapshot, request)
    with _open(snapshot, patch_source) as con:
        return _select(con, plan, request)


def count(snapshot: Snapshot, patch_source: PatchSource, request: Query = EVERY_ROW) -> int:
    """How many rows match `request`'s filters (its fields, sort and paging do not matter)."""
    plan = _plan(snapshot, Query(filters=request.filters, include_deleted=request.include_deleted))
    with _open(snapshot, patch_source) as con:
        merged = merged_sql(summarise_patches(con, snapshot), plan.filtered, request.include_deleted)
        found = con.execute(f"SELECT count(*) FROM ({merged}) m{plan.where}", plan.params).fetchall()
        return int(found[0][0])


@contextmanager
def scan(
    snapshot: Snapshot, patch_source: PatchSource, *, include_deleted: bool = False, batch_rows: int = SCAN_BATCH_ROWS
) -> Generator[pa.RecordBatchReader, None, None]:
    """Every row of the revision with every field, as a stream of batches (compaction, export):
    base rows in the base's own order, then inserted rows — so a compacted base keeps its row
    groups clustered by row id. Memory stays within the engine's budget whatever the size.
    Consume it inside the block.

    Measured on 10M rows (2026-10-25): keeping the order was faster (23 s vs 25 s) and lighter
    (3.0 GB vs 4.0 GB peak) than DuckDB's unordered streaming, which scattered row ids across
    every row group."""
    with _open(snapshot, patch_source) as con:
        sql = merged_sql(summarise_patches(con, snapshot), snapshot.fields, include_deleted)
        yield con.sql(sql).to_arrow_reader(batch_rows)


def explain(snapshot: Snapshot, patch_source: PatchSource, request: Query = EVERY_ROW) -> list[str]:
    """DuckDB's physical plan of each statement `query(request)` runs — one for a one-pass read,
    two for a two-pass page. Benchmarks and tests check that filters and column choices reach
    the Parquet scan, and which pages take two passes."""
    plan = _plan(snapshot, request)
    with _open(snapshot, patch_source) as con:
        return [
            "\n".join(str(row[-1]) for row in con.execute(f"EXPLAIN {sql}", params).fetchall())
            for sql, params in _statements(con, plan, request)
        ]


def read_rows(snapshot: Snapshot, patch_source: PatchSource, *, include_deleted: bool = False) -> pa.Table:
    """Every row of the revision, by row id (small datasets and tests)."""
    return query(snapshot, patch_source, Query(include_deleted=include_deleted))


def merge(base: pa.Table, patches: pa.Table, snapshot: Snapshot, request: Query = EVERY_ROW) -> pa.Table:
    """Base rows in memory + patches → the revision's rows, as `query` returns them. Pure: no I/O."""
    plan = _plan(snapshot, request)
    with connection() as con:
        register_base_rows(con, base, snapshot.fields)
        register_patches(con, patches)
        return _select(con, plan, request)


@contextmanager
def _open(snapshot: Snapshot, patch_source: PatchSource) -> Generator[duckdb.DuckDBPyConnection, None, None]:
    with connection() as con:
        register_base(con, snapshot)
        register_patches(con, patch_source.patches(snapshot.dataset_id, snapshot.patch_from, snapshot.patch_to))
        yield con


# ── plan ─────────────────────────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class _Plan:
    snapshot: Snapshot
    returned: tuple[FieldSpec, ...]
    sorted_by: tuple[FieldSpec, ...]
    filtered: tuple[FieldSpec, ...]
    where: str
    params: dict[str, object]
    order: str


def _plan(snapshot: Snapshot, request: Query) -> _Plan:
    """Everything a request asks for, checked — no I/O, so a bad request costs nothing."""
    if request.limit is not None:
        bounded("limit", request.limit)
    bounded("offset", request.offset)
    returned = snapshot.fields if request.fields is None else specs(snapshot, request.fields)
    sorted_by = specs(snapshot, [sort.field_id for sort in request.sort])
    multi = [spec.field_id for spec in sorted_by if spec.value_type == "opts"]
    if multi:
        raise ValidationError(f"Can't sort by multi-select fields: {', '.join(multi)}")
    params = Params()
    condition = where(snapshot, request, params)
    column_of = {spec.field_id: spec.column for spec in sorted_by}
    order = [f'"{column_of[s.field_id]}" {"DESC" if s.descending else "ASC"} NULLS LAST' for s in request.sort]
    filtered = specs(snapshot, [f.field_id for f in request.filters])
    return _Plan(snapshot, returned, sorted_by, filtered, condition, params.values, ", ".join([*order, f'"{ROW_ID}"']))


def _columns(plan: _Plan, request: Query) -> str:
    columns = [f'"{ROW_ID}"', *(f'"{spec.column}"' for spec in plan.returned)]
    if request.include_deleted:
        columns.append(f'"{DELETED_AT}"')
    return ", ".join(columns)


def _paging(request: Query) -> str:
    return (f" LIMIT {request.limit}" if request.limit is not None else "") + (
        f" OFFSET {request.offset}" if request.offset else ""
    )


def _two_passes(touched: Touched, plan: _Plan, request: Query) -> bool:
    if request.limit is None or request.limit > PAGE_ROWS_MAX:
        return False
    if request.offset >= DEEP_OFFSET:
        return True
    wide = {spec.field_id for spec in plan.returned}
    return touched.row_states or touched.inserts or bool(wide & touched.fields)


def _statements(con: duckdb.DuckDBPyConnection, plan: _Plan, request: Query) -> list[tuple[str, dict[str, object]]]:
    """What `query(request)` runs, in order — a page's first pass runs here to plan its second."""
    touched = summarise_patches(con, plan.snapshot)
    if not _two_passes(touched, plan, request):
        merged = merged_sql(touched, (*plan.returned, *plan.sorted_by, *plan.filtered), request.include_deleted)
        one = f"SELECT {_columns(plan, request)} FROM ({merged}) m{plan.where} ORDER BY {plan.order}{_paging(request)}"
        return [(one, plan.params)]
    narrow = merged_sql(touched, (*plan.sorted_by, *plan.filtered), request.include_deleted)
    find = f'SELECT "{ROW_ID}" FROM ({narrow}) m{plan.where} ORDER BY {plan.order}{_paging(request)}'
    rows: Sequence[object] = [row[0] for row in con.execute(find, plan.params).fetchall()]
    con.register(PAGE_IDS, pa.table({ROW_ID: pa.array(rows, pa.string())}))
    wide = merged_sql(touched, (*plan.returned, *plan.sorted_by), request.include_deleted, page=True)
    return [(find, plan.params), (f"SELECT {_columns(plan, request)} FROM ({wide}) m ORDER BY {plan.order}", {})]


def _select(con: duckdb.DuckDBPyConnection, plan: _Plan, request: Query) -> pa.Table:
    sql, params = _statements(con, plan, request)[-1]
    # con.sql(...) streams straight to Arrow; execute() would copy every row once more (DuckDB 2.0).
    return con.sql(sql, params=params).to_arrow_table()
