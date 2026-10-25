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
        sql = _merged_sql(con, snapshot, snapshot.fields, include_deleted)
        yield con.sql(sql).to_arrow_reader(batch_rows)


def explain(snapshot: Snapshot, patch_source: PatchSource, request: Query = EVERY_ROW) -> str:
    """DuckDB's physical plan for `query(request)` — benchmarks and tests check that filters and
    column choices reach the Parquet scan."""
    with _open(snapshot, patch_source) as con:
        sql, params = _select_sql(con, snapshot, request)
        return "\n".join(str(row[-1]) for row in con.execute(f"EXPLAIN {sql}", params).fetchall())


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


def _merged_sql(
    con: duckdb.DuckDBPyConnection, snapshot: Snapshot, specs: Sequence[FieldSpec], include_deleted: bool
) -> str:
    """The revision's rows with only `specs`' columns: base rows (+ inserted rows) with the
    patched cells and row states laid over them; trashed rows only with `include_deleted`."""
    touched = _touched(con, snapshot)
    patched = [(index, spec) for index, spec in enumerate(specs) if spec.field_id in touched.fields]
    ctes = [
        f"c{index} AS (SELECT row, arg_max({{'t': t, 'v': {VALUE_COLUMN[spec.value_type]}}}, seq) AS c FROM p"
        f" WHERE op = 'set' AND field = '{spec.field_id}' GROUP BY row)"
        for index, spec in patched
    ]
    if touched.row_states:
        ctes.append(
            "state AS (SELECT row, arg_max({'op': op, 'at': \"at\"}, seq) AS last FROM p"  # AT is a DuckDB keyword
            " WHERE op IN ('delete_row', 'restore_row') GROUP BY row)"
        )
    if touched.inserts:
        ctes.append(
            f"inserted AS (SELECT DISTINCT row AS \"{ROW_ID}\" FROM p WHERE op = 'insert_row'"
            f' AND NOT EXISTS (SELECT 1 FROM base WHERE base."{ROW_ID}" = p.row))'
        )

    def rows_from(source: str, alias: str, in_base: bool) -> str:
        columns = [f'{alias}."{ROW_ID}"']
        for index, spec in enumerate(specs):
            plain = f'{alias}."{spec.column}"' if in_base else "NULL"
            if spec.field_id in touched.fields:
                plain = (
                    f"CASE WHEN c{index}.c IS NULL THEN {plain} WHEN c{index}.c.t = 'null' THEN NULL"
                    f" ELSE c{index}.c.v END"
                )
            columns.append(f'{plain} AS "{spec.column}"')
        joins = "".join(f' LEFT JOIN c{index} ON c{index}.row = {alias}."{ROW_ID}"' for index, _ in patched)
        base_deleted_at = f'CAST({alias}."{DELETED_AT}" AS TIMESTAMPTZ)' if in_base else "CAST(NULL AS TIMESTAMPTZ)"
        deleted_at, live = base_deleted_at, f'{alias}."{DELETED_AT}" IS NULL' if in_base else "true"
        if touched.row_states:
            joins += f' LEFT JOIN state s ON s.row = {alias}."{ROW_ID}"'
            deleted_at = (
                f"CASE WHEN s.last IS NULL THEN {base_deleted_at} WHEN s.last.op = 'delete_row' THEN s.last.at"
                " ELSE CAST(NULL AS TIMESTAMPTZ) END"
            )
            # Live by the latest row patch's op, not by its time (a delete is a delete even untimed).
            live = f"CASE WHEN s.last IS NULL THEN {base_deleted_at} IS NULL ELSE s.last.op = 'restore_row' END"
        if include_deleted:
            columns.append(f'{deleted_at} AS "{DELETED_AT}"')
        # Without row-state patches the live test is `_deleted_at IS NULL` on the Parquet scan itself.
        where = "" if include_deleted else f" WHERE {live}"
        return f"SELECT {', '.join(columns)} FROM {source} {alias}{joins}{where}"

    sql = rows_from("base", "b", True)
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


def _select(con: duckdb.DuckDBPyConnection, snapshot: Snapshot, request: Query) -> pa.Table:
    sql, params = _select_sql(con, snapshot, request)
    # con.sql(...) streams straight to Arrow; execute() would copy every row once more (DuckDB 2.0).
    return con.sql(sql, params=params).to_arrow_table()


def _select_sql(con: duckdb.DuckDBPyConnection, snapshot: Snapshot, request: Query) -> tuple[str, dict[str, object]]:
    if (request.limit is not None and request.limit < 0) or request.offset < 0:
        raise ValidationError("limit and offset can't be negative")
    returned = snapshot.fields if request.fields is None else _specs(snapshot, request.fields)
    sorted_by = _specs(snapshot, [sort.field_id for sort in request.sort])
    multi = [spec.field_id for spec in sorted_by if spec.value_type == "opts"]
    if multi:
        raise ValidationError(f"Can't sort by multi-select fields: {', '.join(multi)}")
    needed = _specs(
        snapshot,
        [spec.field_id for spec in (*returned, *sorted_by)] + [condition.field_id for condition in request.filters],
    )
    params = _Params()
    where = _where(snapshot, request, params)
    columns = [f'"{ROW_ID}"', *(f'"{spec.column}"' for spec in returned)]
    if request.include_deleted:
        columns.append(f'"{DELETED_AT}"')
    column_of = {spec.field_id: spec.column for spec in sorted_by}
    order = [f'"{column_of[s.field_id]}" {"DESC" if s.descending else "ASC"} NULLS LAST' for s in request.sort]
    page = (f" LIMIT {int(request.limit)}" if request.limit is not None else "") + (
        f" OFFSET {int(request.offset)}" if request.offset else ""
    )
    merged = _merged_sql(con, snapshot, needed, request.include_deleted)
    sql = f"SELECT {', '.join(columns)} FROM ({merged}) m{where} ORDER BY {', '.join([*order, f'"{ROW_ID}"'])}{page}"
    return sql, params.values


def _count(con: duckdb.DuckDBPyConnection, snapshot: Snapshot, request: Query) -> int:
    params = _Params()
    where = _where(snapshot, request, params)
    needed = _specs(snapshot, [condition.field_id for condition in request.filters])
    merged = _merged_sql(con, snapshot, needed, request.include_deleted)
    found = con.execute(f"SELECT count(*) FROM ({merged}) m{where}", params.values).fetchall()
    return int(found[0][0])
