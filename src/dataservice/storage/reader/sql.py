"""The SQL of the one reader: base rows with the revision's patches laid over them, and filters.

    rows   = base rows ∪ rows created by insert_row patches
    state  = the latest delete_row / restore_row patch of a row, else the base `_deleted_at`
             (a delete patch's `at` becomes the row's `_deleted_at`, so the trash window survives
             compaction — a delete without `at` is refused)
    cell   = the latest `set` patch of (row, field) by seq ("null" = cleared), else the base value

A field no patch touches stays the plain base column (pushdown, late materialization), and all
the patched cells and the row state meet the base in ONE join.
"""

import math
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import date, datetime

import duckdb

from dataservice.common.errors import ValidationError
from dataservice.storage.reader.model import DELETED_AT, ROW_ID, Filter, Query
from dataservice.storage.snapshot import VALUE_COLUMN, FieldSpec, Snapshot

_COMPARE = {"eq": "=", "lt": "<", "le": "<=", "gt": ">", "ge": ">="}
_INT64 = 2**63
PAGE_IDS = "page_ids"  # the registered row ids of a page read in full (pass 2)


@dataclass(frozen=True)
class Touched:
    """What the revision's patches touch — everything else is read from base as it is."""

    fields: frozenset[str]
    row_states: bool
    inserts: bool


@dataclass
class Params:
    """Filter values, bound as query parameters — never pasted into the SQL."""

    values: dict[str, object] = field(default_factory=dict)

    def add(self, value: object) -> str:
        name = f"p{len(self.values)}"
        self.values[name] = value
        return f"${name}"


def specs(snapshot: Snapshot, field_ids: Sequence[str]) -> tuple[FieldSpec, ...]:
    by_id = {spec.field_id: spec for spec in snapshot.fields}
    unknown = [field_id for field_id in field_ids if field_id not in by_id]
    if unknown:
        raise ValidationError(f"Unknown fields: {', '.join(unknown)}")
    return tuple(by_id[field_id] for field_id in dict.fromkeys(field_ids))


def summarise_patches(con: duckdb.DuckDBPyConnection, snapshot: Snapshot) -> Touched:
    """`p` = the revision's patches; and what they touch."""
    con.execute(
        "CREATE OR REPLACE TEMP VIEW p AS SELECT * FROM patches"
        f" WHERE seq > {int(snapshot.patch_from)} AND seq <= {int(snapshot.patch_to)}"
    )
    found = con.execute(
        "SELECT list(DISTINCT field) FILTER (WHERE op = 'set'),"
        " count(*) FILTER (WHERE op IN ('delete_row', 'restore_row')),"
        " count(*) FILTER (WHERE op = 'insert_row'),"
        " count(*) FILTER (WHERE op = 'delete_row' AND \"at\" IS NULL),"  # AT is a DuckDB keyword
        " count(*) FILTER (WHERE row IS NULL) FROM p"
    ).fetchall()  # DuckDB 2.0: a result must be read to the end before the next statement
    fields, states, inserts, untimed, unaddressed = found[0]
    if unaddressed:  # an insert of no row would add a row without an id, for good after compaction
        raise ValueError(f"{unaddressed} patch(es) without a row id")
    if untimed:
        # Its trash time would be lost when compaction folds it into `_deleted_at` (C39).
        raise ValueError(f"{untimed} delete_row patch(es) without `at` — Node writes every patch with its time")
    return Touched(frozenset(fields or ()), bool(states), bool(inserts))


def merged_sql(touched: Touched, fields: Sequence[FieldSpec], include_deleted: bool, page: bool = False) -> str:
    """The revision's rows with only `fields`' columns: base rows (+ inserted rows) with the
    patched cells and row states laid over them; trashed rows only with `include_deleted`;
    with `page`, only the rows whose ids are in the registered `page_ids` table."""
    in_page = f' WHERE "{ROW_ID}" IN (SELECT "{ROW_ID}" FROM {PAGE_IDS})'
    base = f"(SELECT * FROM base{in_page})" if page else "base"
    patched = [(index, spec) for index, spec in enumerate(fields) if spec.field_id in touched.fields]
    edits = [
        f"arg_max({{'t': t, 'v': {VALUE_COLUMN[spec.value_type]}}}, seq)"
        f" FILTER (WHERE op = 'set' AND field = '{spec.field_id}') AS c{index}"
        for index, spec in patched
    ]
    if touched.row_states:
        edits.append(
            "arg_max({'op': op, 'at': \"at\"}, seq) FILTER (WHERE op IN ('delete_row', 'restore_row')) AS last"
        )
    ctes = [f"e AS (SELECT row, {', '.join(edits)} FROM p GROUP BY row)"] if edits else []
    if touched.inserts:
        # Inserted ids that are not base rows: the base is probed against the few inserted ids
        # (a semi join that hashes only them), never hashed itself.
        only = f' AND row IN (SELECT "{ROW_ID}" FROM {PAGE_IDS})' if page else ""
        ctes += [
            f"ins AS (SELECT DISTINCT row FROM p WHERE op = 'insert_row'{only})",
            f'inserted AS (SELECT row AS "{ROW_ID}" FROM ins WHERE row NOT IN'
            f' (SELECT b."{ROW_ID}" FROM base b SEMI JOIN ins ON b."{ROW_ID}" = ins.row))',
        ]

    def rows_from(source: str, alias: str, in_base: bool) -> str:
        columns = [f'{alias}."{ROW_ID}"']
        for index, spec in enumerate(fields):
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


def bounded(name: str, value: object) -> int:
    """A limit or an offset: a whole number DuckDB can hold."""
    if isinstance(value, bool) or not isinstance(value, int) or not 0 <= value < _INT64:
        raise ValidationError(f"{name} must be a whole number from 0 to {_INT64 - 1}")
    return value


def _value(spec: FieldSpec, value_type: str, value: object) -> object:
    """A filter value of the field's type."""
    if value_type == "num" and isinstance(value, int | float) and not isinstance(value, bool):
        if isinstance(value, int) and -_INT64 <= value < _INT64:
            return value
        try:
            number = float(value)
        except OverflowError:
            number = math.inf
        if math.isfinite(number):
            return number
    elif value_type == "str" and isinstance(value, str):
        return value
    elif value_type == "date" and isinstance(value, date) and not isinstance(value, datetime):
        return value
    raise ValidationError(f"Filter value for field {spec.field_id} must be a {value_type}")


def _condition(spec: FieldSpec, condition: Filter, params: Params) -> str:
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


def where(snapshot: Snapshot, request: Query, params: Params) -> str:
    by_id = {spec.field_id: spec for spec in specs(snapshot, [f.field_id for f in request.filters])}
    conditions = [_condition(by_id[condition.field_id], condition, params) for condition in request.filters]
    return f" WHERE {' AND '.join(conditions)}" if conditions else ""
