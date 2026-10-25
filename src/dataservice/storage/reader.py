"""The ONE reader (hybrid edits): grid, analysis, export and compaction all read a revision here.

    rows   = base rows ∪ rows created by insert_row patches
    state  = the latest delete_row / restore_row patch of a row, else the base `_deleted_at`
             (a delete patch's `at` becomes the row's `_deleted_at`, so the trash window survives
             compaction)
    cell   = the latest `set` patch of (row, field) by seq ("null" = cleared), else the base value

Only patches inside the revision's range count, so every reader of revision n sees exactly the
same rows — before and after a compaction folds patches into base. Nothing else may read Delta
data directly (tests/unit/test_architecture.py enforces it).

Built for GB-scale bases — the base is never loaded whole:
  - DuckDB reads the Parquet files of the pinned Delta version (listed by the Delta log), and
    only the columns the query needs;
  - a field no patch touches is the plain base column, so a filter on it reaches the Parquet
    scan (row groups skipped) and `ORDER BY … LIMIT` reads the other columns only for the page;
  - patches join only for the fields and row states they touch;
  - a page is found first from the row id and its sort / filter columns alone, and only its rows
    are then read in full (`_rid IN (…)` skips the other row groups) and dressed with patches;
  - filter, sort, paging and count run inside DuckDB; a full read streams in batches.
"""

from collections.abc import Generator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import date
from typing import Literal

import duckdb
import pyarrow as pa
from deltalake import DeltaTable

from dataservice.common.errors import ValidationError
from dataservice.storage.snapshot import PATCH_SCHEMA, VALUE_COLUMN, FieldSpec, PatchSource, Snapshot

ROW_ID = "_rid"
DELETED_AT = "_deleted_at"
SCAN_BATCH_ROWS = 65_536
# Pages up to this many rows are found first and read in full afterwards (two passes); a
# bigger result is read in one pass.
PAGE_ROWS_MAX = 5_000

FilterOp = Literal["eq", "ne", "lt", "le", "gt", "ge", "in", "is_null", "not_null", "contains"]
_COMPARE = {"eq": "=", "lt": "<", "le": "<=", "gt": ">", "ge": ">="}
# Delta features the Parquet files alone do not describe — a table with any of them can't be
# read file by file. Our writer never turns them on; the reader refuses rather than misreads.
_FILE_BY_FILE_BLOCKERS = frozenset({"deletionVectors", "columnMapping"})


@dataclass(frozen=True)
class Filter:
    """One condition on a field. `ne` keeps empty cells (a blank is "not X"); `contains` is a
    case-insensitive substring for text and "has this option" for multi-select."""

    field_id: str
    op: FilterOp
    value: object = None


@dataclass(frozen=True)
class Sort:
    field_id: str
    descending: bool = False  # empty cells sort last either way


@dataclass(frozen=True)
class Query:
    fields: tuple[str, ...] | None = None  # field ids to return; None = every field of the snapshot
    filters: tuple[Filter, ...] = ()  # all must hold
    sort: tuple[Sort, ...] = ()  # the row id breaks ties, so a page is always the same rows
    limit: int | None = None
    offset: int = 0
    include_deleted: bool = False  # also trashed rows and `_deleted_at` (compaction, trash view)


EVERY_ROW = Query()  # every live row with every field, by row id


def query(snapshot: Snapshot, patch_source: PatchSource, request: Query = EVERY_ROW) -> pa.Table:
    """The revision's rows that match `request` — one page of them when it has a limit."""
    with _open(snapshot, patch_source) as con:
        return _select(con, snapshot, request)


def count(snapshot: Snapshot, patch_source: PatchSource, request: Query = EVERY_ROW) -> int:
    """How many rows match `request`'s filters (its fields, sort and paging do not matter)."""
    with _open(snapshot, patch_source) as con:
        return _count(con, snapshot, request)


@contextmanager
def scan(
    snapshot: Snapshot, patch_source: PatchSource, *, include_deleted: bool = False, batch_rows: int = SCAN_BATCH_ROWS
) -> Generator[pa.RecordBatchReader, None, None]:
    """Every row of the revision with every field, as a stream of batches in no particular order
    (compaction, export) — memory stays bounded whatever the size. Consume it inside the block."""
    with _open(snapshot, patch_source) as con:
        con.execute("SET preserve_insertion_order = false")  # lets DuckDB stream without buffering
        sql = _merged_sql(_touched(con, snapshot), snapshot.fields, include_deleted)
        yield con.sql(sql).to_arrow_reader(batch_rows)


def explain(snapshot: Snapshot, patch_source: PatchSource, request: Query = EVERY_ROW) -> str:
    """DuckDB's physical plan for `query(request)` — benchmarks and tests check that filters and
    column choices reach the Parquet scan."""
    with _open(snapshot, patch_source) as con:
        return "\n".join(
            str(row[-1])
            for sql, params in _statements(con, snapshot, request)
            for row in con.execute(f"EXPLAIN {sql}", params).fetchall()
        )


def read_rows(snapshot: Snapshot, patch_source: PatchSource, *, include_deleted: bool = False) -> pa.Table:
    """Every row of the revision, by row id (small datasets and tests)."""
    return query(snapshot, patch_source, Query(include_deleted=include_deleted))


def merge(base: pa.Table, patches: pa.Table, snapshot: Snapshot, request: Query = EVERY_ROW) -> pa.Table:
    """Base rows in memory + patches → the revision's rows, as `query` returns them. Pure: no I/O."""
    with _connection() as con:
        con.register("base", base)
        _register_patches(con, patches)
        return _select(con, snapshot, request)


# ── connection ───────────────────────────────────────────────────────────────────────────────


@contextmanager
def _connection() -> Generator[duckdb.DuckDBPyConnection, None, None]:
    con = duckdb.connect()
    try:
        con.execute("SET TimeZone = 'UTC'")  # instants stay UTC whatever the host's zone
        yield con
    finally:
        con.close()


@contextmanager
def _open(snapshot: Snapshot, patch_source: PatchSource) -> Generator[duckdb.DuckDBPyConnection, None, None]:
    with _connection() as con:
        _register_base(con, snapshot)
        _register_patches(con, patch_source.patches(snapshot.dataset_id, snapshot.patch_from, snapshot.patch_to))
        yield con


def _register_base(con: duckdb.DuckDBPyConnection, snapshot: Snapshot) -> None:
    """`base` = the Parquet files of the pinned version, read lazily (nothing is loaded here)."""
    table = DeltaTable(snapshot.base_uri, version=snapshot.base_version, storage_options=snapshot.storage_options)
    blockers = _FILE_BY_FILE_BLOCKERS & set(table.protocol().reader_features or ())
    if blockers or table.metadata().partition_columns:
        raise ValueError(f"base table can't be read file by file (features {sorted(blockers)} or partitions)")
    files = table.file_uris()
    if not files:  # a table without rows has no files, and DuckDB can't read an empty file list
        con.register("base", pa.schema(table.schema().to_arrow()).empty_table())
        return
    if any(uri.startswith("s3://") for uri in files):
        _use_s3(con, snapshot.storage_options)
    con.read_parquet(files).create_view("base")


def _use_s3(con: duckdb.DuckDBPyConnection, options: Mapping[str, str]) -> None:
    """S3 access for DuckDB from the storage options delta-rs uses (never logged)."""
    con.execute("INSTALL httpfs")  # a no-op once installed (the image installs it at build time)
    con.execute("LOAD httpfs")
    secret = {
        "KEY_ID": options.get("AWS_ACCESS_KEY_ID"),
        "SECRET": options.get("AWS_SECRET_ACCESS_KEY"),
        "SESSION_TOKEN": options.get("AWS_SESSION_TOKEN"),
        "REGION": options.get("AWS_REGION"),
    }
    endpoint = options.get("AWS_ENDPOINT_URL")
    if endpoint:  # a local S3 (tests): host only, path-style, plain http when the URL says so
        scheme, _, host = endpoint.partition("://")
        secret |= {"ENDPOINT": host, "URL_STYLE": "path", "USE_SSL": "false" if scheme == "http" else "true"}
    pairs = "".join(f", {key} '{value.replace(chr(39), chr(39) * 2)}'" for key, value in secret.items() if value)
    con.execute(f"CREATE OR REPLACE SECRET dataservice_s3 (TYPE s3{pairs})")


def _register_patches(con: duckdb.DuckDBPyConnection, patches: pa.Table) -> None:
    if patches.schema != PATCH_SCHEMA:
        raise ValueError("patch source returned an unexpected schema")
    con.register("patches", patches)


# ── SQL ──────────────────────────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class _Touched:
    """What the revision's patches touch — everything else is read from base as it is."""

    fields: frozenset[str]
    row_states: bool
    inserts: bool


@dataclass
class _Params:
    """Filter values, bound as query parameters — never pasted into the SQL."""

    values: dict[str, object] = field(default_factory=dict)

    def add(self, value: object) -> str:
        name = f"p{len(self.values)}"
        self.values[name] = value
        return f"${name}"


def _specs(snapshot: Snapshot, field_ids: Sequence[str]) -> tuple[FieldSpec, ...]:
    by_id = {spec.field_id: spec for spec in snapshot.fields}
    unknown = [field_id for field_id in field_ids if field_id not in by_id]
    if unknown:
        raise ValidationError(f"Unknown fields: {', '.join(unknown)}")
    return tuple(by_id[field_id] for field_id in dict.fromkeys(field_ids))


def _touched(con: duckdb.DuckDBPyConnection, snapshot: Snapshot) -> _Touched:
    con.execute(
        "CREATE OR REPLACE TEMP VIEW p AS SELECT * FROM patches"
        f" WHERE seq > {int(snapshot.patch_from)} AND seq <= {int(snapshot.patch_to)}"
    )
    found = con.execute(
        "SELECT list(DISTINCT field) FILTER (WHERE op = 'set'),"
        " count(*) FILTER (WHERE op IN ('delete_row', 'restore_row')),"
        " count(*) FILTER (WHERE op = 'insert_row') FROM p"
    ).fetchall()  # DuckDB 2.0: a result must be read to the end before the next statement
    fields, states, inserts = found[0]
    return _Touched(frozenset(fields or ()), bool(states), bool(inserts))


def _literals(values: Sequence[str]) -> str:
    """Row ids as SQL string literals — they come from the dataset itself, quoted all the same."""
    return ", ".join("'" + value.replace("'", "''") + "'" for value in values)


def _merged_sql(
    touched: _Touched,
    specs: Sequence[FieldSpec],
    include_deleted: bool,
    rows: Sequence[str] | None = None,
) -> str:
    """The revision's rows with only `specs`' columns: base rows (+ inserted rows) with the
    patched cells and row states laid over them; trashed rows only with `include_deleted`;
    only the row ids in `rows` when given (a page being read in full)."""
    base, only_inserted = "base", ""
    if rows is not None:
        base = f'(SELECT * FROM base WHERE "{ROW_ID}" IN ({_literals(rows)}))' if rows else "(FROM base LIMIT 0)"
        only_inserted = f" AND row IN ({_literals(rows)})" if rows else " AND false"
    patched = [(index, spec) for index, spec in enumerate(specs) if spec.field_id in touched.fields]
    # One row of edits per edited row — every patched cell and the row state side by side — so
    # the base meets the patches in ONE join however many fields they touch.
    edits = [
        f"arg_max({{'t': t, 'v': {VALUE_COLUMN[spec.value_type]}}}, seq)"
        f" FILTER (WHERE op = 'set' AND field = '{spec.field_id}') AS c{index}"
        for index, spec in patched
    ]
    if touched.row_states:  # AT is a DuckDB keyword
        edits.append(
            "arg_max({'op': op, 'at': \"at\"}, seq) FILTER (WHERE op IN ('delete_row', 'restore_row')) AS last"
        )
    ctes = [f"e AS (SELECT row, {', '.join(edits)} FROM p GROUP BY row)"] if edits else []
    if touched.inserts:
        # Inserted ids that are not base rows: the base is probed against the few inserted ids
        # (a semi join that hashes only them), never hashed itself.
        ctes += [
            f"ins AS (SELECT DISTINCT row FROM p WHERE op = 'insert_row'{only_inserted})",
            f'inserted AS (SELECT row AS "{ROW_ID}" FROM ins WHERE row NOT IN'
            f' (SELECT b."{ROW_ID}" FROM base b SEMI JOIN ins ON b."{ROW_ID}" = ins.row))',
        ]

    def rows_from(source: str, alias: str, in_base: bool) -> str:
        columns = [f'{alias}."{ROW_ID}"']
        for index, spec in enumerate(specs):
            plain = f'{alias}."{spec.column}"' if in_base else "NULL"
            if spec.field_id in touched.fields:
                plain = (
                    f"CASE WHEN e.c{index} IS NULL THEN {plain} WHEN e.c{index}.t = 'null' THEN NULL"
                    f" ELSE e.c{index}.v END"
                )
            columns.append(f'{plain} AS "{spec.column}"')
        joins = f' LEFT JOIN e ON e.row = {alias}."{ROW_ID}"' if edits else ""
        base_deleted_at = f'CAST({alias}."{DELETED_AT}" AS TIMESTAMPTZ)' if in_base else "CAST(NULL AS TIMESTAMPTZ)"
        deleted_at, live = base_deleted_at, f'{alias}."{DELETED_AT}" IS NULL' if in_base else "true"
        if touched.row_states:
            deleted_at = (
                f"CASE WHEN e.last IS NULL THEN {base_deleted_at} WHEN e.last.op = 'delete_row' THEN e.last.at"
                " ELSE CAST(NULL AS TIMESTAMPTZ) END"
            )
            # Live by the latest row patch's op, not by its time (a delete is a delete even untimed).
            live = f"CASE WHEN e.last IS NULL THEN {base_deleted_at} IS NULL ELSE e.last.op = 'restore_row' END"
        if include_deleted:
            columns.append(f'{deleted_at} AS "{DELETED_AT}"')
        # Without row-state patches the live test is `_deleted_at IS NULL` on the Parquet scan itself.
        where = "" if include_deleted else f" WHERE {live}"
        return f"SELECT {', '.join(columns)} FROM {source} {alias}{joins}{where}"

    sql = rows_from(base, "b", True)
    if touched.inserts:
        sql += " UNION ALL " + rows_from("inserted", "i", False)
    return (f"WITH {', '.join(ctes)} " if ctes else "") + sql


def _value(spec: FieldSpec, value_type: str, value: object) -> object:
    valid = {
        "num": isinstance(value, int | float) and not isinstance(value, bool),
        "str": isinstance(value, str),
        "date": isinstance(value, date),
    }.get(value_type, False)
    if not valid:
        raise ValidationError(f"Filter value for field {spec.field_id} must be a {value_type}")
    return value


def _condition(spec: FieldSpec, condition: Filter, params: _Params) -> str:
    column, op, value = f'"{spec.column}"', condition.op, condition.value
    if op == "is_null":
        return f"{column} IS NULL"
    if op == "not_null":
        return f"{column} IS NOT NULL"
    if op == "contains":
        if spec.value_type == "opts":
            return f"list_contains({column}, {params.add(_value(spec, 'str', value))})"
        if spec.value_type == "str":
            return f"contains(lower({column}), lower({params.add(_value(spec, 'str', value))}))"
        raise ValidationError(f"'contains' does not apply to field {spec.field_id}")
    if spec.value_type == "opts":
        raise ValidationError(f"Field {spec.field_id} takes 'contains', 'is_null' or 'not_null'")
    if op == "in":
        if not isinstance(value, list | tuple) or not value:
            raise ValidationError(f"'in' on field {spec.field_id} needs a non-empty list")
        return f"{column} IN ({', '.join(params.add(_value(spec, spec.value_type, item)) for item in value)})"
    bound = params.add(_value(spec, spec.value_type, value))
    if op == "ne":
        return f"{column} IS DISTINCT FROM {bound}"
    return f"{column} {_COMPARE[op]} {bound}"


def _where(snapshot: Snapshot, request: Query, params: _Params) -> str:
    by_id = {spec.field_id: spec for spec in _specs(snapshot, [f.field_id for f in request.filters])}
    conditions = [_condition(by_id[condition.field_id], condition, params) for condition in request.filters]
    return f" WHERE {' AND '.join(conditions)}" if conditions else ""


@dataclass(frozen=True)
class _Plan:
    returned: tuple[FieldSpec, ...]
    sorted_by: tuple[FieldSpec, ...]
    filtered: tuple[FieldSpec, ...]
    where: str
    params: dict[str, object]
    order: str


def _plan(snapshot: Snapshot, request: Query) -> _Plan:
    if (request.limit is not None and request.limit < 0) or request.offset < 0:
        raise ValidationError("limit and offset can't be negative")
    returned = snapshot.fields if request.fields is None else _specs(snapshot, request.fields)
    sorted_by = _specs(snapshot, [sort.field_id for sort in request.sort])
    multi = [spec.field_id for spec in sorted_by if spec.value_type == "opts"]
    if multi:
        raise ValidationError(f"Can't sort by multi-select fields: {', '.join(multi)}")
    params = _Params()
    where = _where(snapshot, request, params)
    column_of = {spec.field_id: spec.column for spec in sorted_by}
    order = [f'"{column_of[s.field_id]}" {"DESC" if s.descending else "ASC"} NULLS LAST' for s in request.sort]
    filtered = _specs(snapshot, [condition.field_id for condition in request.filters])
    return _Plan(returned, sorted_by, filtered, where, params.values, ", ".join([*order, f'"{ROW_ID}"']))


def _columns(plan: _Plan, request: Query) -> str:
    columns = [f'"{ROW_ID}"', *(f'"{spec.column}"' for spec in plan.returned)]
    if request.include_deleted:
        columns.append(f'"{DELETED_AT}"')
    return ", ".join(columns)


def _paging(request: Query) -> str:
    return (f" LIMIT {int(request.limit)}" if request.limit is not None else "") + (
        f" OFFSET {int(request.offset)}" if request.offset else ""
    )


def _two_passes(touched: _Touched, plan: _Plan, request: Query) -> bool:
    """A page read in two passes — only when patches would join the full-width rows. Untouched,
    DuckDB already reads the other columns for the page's rows alone (late materialization)."""
    if request.limit is None or request.limit > PAGE_ROWS_MAX:
        return False
    wide = {spec.field_id for spec in plan.returned}
    return touched.row_states or touched.inserts or bool(wide & touched.fields)


def _find_page_sql(touched: _Touched, plan: _Plan, request: Query) -> str:
    """Pass 1 of a page: its row ids, from the sort and filter columns alone."""
    merged = _merged_sql(touched, (*plan.sorted_by, *plan.filtered), request.include_deleted)
    return f'SELECT "{ROW_ID}" FROM ({merged}) m{plan.where} ORDER BY {plan.order}{_paging(request)}'


def _read_page_sql(touched: _Touched, plan: _Plan, request: Query, rows: Sequence[str]) -> str:
    """Pass 2 of a page: only its rows, in full, in the same order (same revision, same sort)."""
    merged = _merged_sql(touched, (*plan.returned, *plan.sorted_by), request.include_deleted, rows)
    return f"SELECT {_columns(plan, request)} FROM ({merged}) m ORDER BY {plan.order}"


def _one_pass_sql(touched: _Touched, plan: _Plan, request: Query) -> str:
    merged = _merged_sql(touched, (*plan.returned, *plan.sorted_by, *plan.filtered), request.include_deleted)
    return f"SELECT {_columns(plan, request)} FROM ({merged}) m{plan.where} ORDER BY {plan.order}{_paging(request)}"


def _page_rows(con: duckdb.DuckDBPyConnection, sql: str, params: dict[str, object]) -> list[str]:
    return [str(row[0]) for row in con.execute(sql, params).fetchall()]


def _statements(
    con: duckdb.DuckDBPyConnection, snapshot: Snapshot, request: Query
) -> list[tuple[str, dict[str, object]]]:
    """What `query(request)` runs, in order — a page's first pass runs here to plan its second."""
    plan, touched = _plan(snapshot, request), _touched(con, snapshot)
    if not _two_passes(touched, plan, request):
        return [(_one_pass_sql(touched, plan, request), plan.params)]
    find = _find_page_sql(touched, plan, request)
    rows = _page_rows(con, find, plan.params)
    return [(find, plan.params), (_read_page_sql(touched, plan, request, rows), {})]


def _select(con: duckdb.DuckDBPyConnection, snapshot: Snapshot, request: Query) -> pa.Table:
    sql, params = _statements(con, snapshot, request)[-1]
    # con.sql(...) streams straight to Arrow; execute() would copy every row once more (DuckDB 2.0).
    return con.sql(sql, params=params).to_arrow_table()


def _count(con: duckdb.DuckDBPyConnection, snapshot: Snapshot, request: Query) -> int:
    params = _Params()
    where = _where(snapshot, request, params)
    needed = _specs(snapshot, [condition.field_id for condition in request.filters])
    merged = _merged_sql(_touched(con, snapshot), needed, request.include_deleted)
    found = con.execute(f"SELECT count(*) FROM ({merged}) m{where}", params.values).fetchall()
    return int(found[0][0])
