"""The DuckDB engine behind the one reader.

One DuckDB database per process, so every read shares ONE memory limit, thread pool and spill
folder (`configure`, from Settings at startup); each read gets its own cursor, whose registered
tables and temp views no other read sees. Extensions are installed when the image is built
(`install_extensions`), never during a request.

`base` (a temp view) is the pinned Delta version as Delta defines it: its Parquet files read by
name (`union_by_name`, so files written before a column was added still line up), plus a typed
NULL column for every column no file holds yet (added in metadata only) and for every snapshot
field the base does not have.
"""

import hashlib
import os
import re
import threading
from collections.abc import Generator, Iterable, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from urllib.parse import unquote, urlparse

import duckdb
import pyarrow as pa
import pyarrow.types as pa_types
from deltalake import DeltaTable

from dataservice.storage.reader.model import ROW_ID, VISIT
from dataservice.storage.snapshot import PATCH_SCHEMA, FieldSpec, Snapshot, TableVersion, ValueType

EXTENSIONS = ("httpfs", "aws")
_GLOB_CHARACTERS = re.compile(r"[*?\[]")
# Delta features the Parquet files alone do not describe — a table with any of them can't be
# read file by file. Our writer never turns them on; the reader refuses rather than misreads.
_FILE_BY_FILE_BLOCKERS = frozenset({"deletionVectors", "columnMapping"})
_FIELD_TYPE: dict[ValueType, str] = {"num": "DOUBLE", "str": "VARCHAR", "date": "DATE", "opts": "VARCHAR[]"}
# The column types each field type may be stored as.
_COLUMN_TYPES: dict[ValueType, frozenset[str]] = {
    "num": frozenset({"DOUBLE", "FLOAT", "BIGINT", "INTEGER", "SMALLINT", "TINYINT"}),
    "str": frozenset({"VARCHAR"}),
    "date": frozenset({"DATE"}),
    "opts": frozenset({"VARCHAR[]"}),
}
# delta-rs accepts these spellings of the AWS options; DuckDB needs one.
_AWS_ALIASES = {
    "ACCESS_KEY_ID": "AWS_ACCESS_KEY_ID",
    "SECRET_ACCESS_KEY": "AWS_SECRET_ACCESS_KEY",
    "SESSION_TOKEN": "AWS_SESSION_TOKEN",
    "TOKEN": "AWS_SESSION_TOKEN",
    "AWS_TOKEN": "AWS_SESSION_TOKEN",
    "REGION": "AWS_REGION",
    "AWS_DEFAULT_REGION": "AWS_REGION",
    "ENDPOINT": "AWS_ENDPOINT_URL",
    "ENDPOINT_URL": "AWS_ENDPOINT_URL",
    "AWS_ENDPOINT": "AWS_ENDPOINT_URL",
}


@dataclass(frozen=True)
class ReaderLimits:
    """The budget every read shares. None = DuckDB's default (all cores, 80% of RAM, `.tmp`)."""

    threads: int | None = None
    memory_limit: str | None = None  # e.g. "4GB" — beyond it DuckDB spills to `temp_directory`
    temp_directory: str | None = None


class _Engine:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.limits = ReaderLimits()
        self.database: duckdb.DuckDBPyConnection | None = None
        self.s3_loaded = False


_ENGINE = _Engine()


def configure(limits: ReaderLimits) -> None:
    """Set the shared budget — at startup, before any read."""
    with _ENGINE.lock:
        if _ENGINE.database is not None:
            _ENGINE.database.close()
        _ENGINE.limits, _ENGINE.database, _ENGINE.s3_loaded = limits, None, False


def install_extensions() -> None:
    """Download DuckDB's S3 extensions — run when the image is built, so no request ever does."""
    con = duckdb.connect()
    try:
        for name in EXTENSIONS:
            con.install_extension(name)
    finally:
        con.close()


def _database() -> duckdb.DuckDBPyConnection:
    with _ENGINE.lock:
        if _ENGINE.database is not None:
            return _ENGINE.database
        limits = _ENGINE.limits
        config: dict[str, str | bool | int | float | list[str]] = {"autoinstall_known_extensions": False}
        if limits.threads is not None:
            config["threads"] = str(limits.threads)
        if limits.memory_limit is not None:
            config["memory_limit"] = limits.memory_limit
        if limits.temp_directory is not None:
            config["temp_directory"] = limits.temp_directory
        _ENGINE.database = duckdb.connect(config=config)
        return _ENGINE.database


@contextmanager
def connection() -> Generator[duckdb.DuckDBPyConnection, None, None]:
    """One read's own cursor on the shared database, closed when the read ends."""
    con = _database().cursor()
    try:
        con.execute("SET TimeZone = 'UTC'")  # per cursor: instants stay UTC whatever the host's zone
        yield con
    finally:
        con.close()


def _load_s3_extensions(con: duckdb.DuckDBPyConnection) -> None:
    with _ENGINE.lock:
        if _ENGINE.s3_loaded:
            return
        for name in EXTENSIONS:
            try:
                con.load_extension(name)  # loads into the shared database, once
            except duckdb.Error as error:
                raise RuntimeError(
                    f"DuckDB extension {name!r} is not installed — run `python -m scripts.install_duckdb_extensions`"
                ) from error
        _ENGINE.s3_loaded = True


# ── S3 ───────────────────────────────────────────────────────────────────────────────────────


def s3_options(options: Mapping[str, str], environ: Mapping[str, str] = os.environ) -> dict[str, str]:
    """The AWS settings delta-rs would use: AWS_* environment variables, overridden by the
    snapshot's storage options, every spelling folded to one."""
    merged: dict[str, str] = {}
    for source in (environ, options):
        for key, value in source.items():
            name = _AWS_ALIASES.get(key.upper(), key.upper())
            if name.startswith("AWS_") and value:
                merged[name] = value
    return merged


def _quote(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def s3_secret_sql(bucket: str, options: Mapping[str, str]) -> str:
    """The DuckDB secret for one bucket: static keys when given, else the AWS credential chain
    (environment, ECS task role, instance role) — the same chain delta-rs resolves."""
    aws = s3_options(options)
    parts = ["TYPE s3", f"SCOPE {_quote('s3://' + bucket)}"]
    if aws.get("AWS_ACCESS_KEY_ID") and aws.get("AWS_SECRET_ACCESS_KEY"):
        parts += [f"KEY_ID {_quote(aws['AWS_ACCESS_KEY_ID'])}", f"SECRET {_quote(aws['AWS_SECRET_ACCESS_KEY'])}"]
        if aws.get("AWS_SESSION_TOKEN"):
            parts.append(f"SESSION_TOKEN {_quote(aws['AWS_SESSION_TOKEN'])}")
    else:
        parts += ["PROVIDER credential_chain", "VALIDATION 'none'"]
    if aws.get("AWS_REGION"):
        parts.append(f"REGION {_quote(aws['AWS_REGION'])}")
    endpoint = aws.get("AWS_ENDPOINT_URL")
    if endpoint:  # an S3-compatible endpoint (tests): host only, path-style, plain http when the URL says so
        scheme, _, host = endpoint.partition("://")
        use_ssl = "false" if scheme == "http" else "true"
        parts += [f"ENDPOINT {_quote(host.rstrip('/'))}", "URL_STYLE 'path'", f"USE_SSL {use_ssl}"]
    name = "s3_" + hashlib.sha256(bucket.encode()).hexdigest()[:16]
    return f"CREATE OR REPLACE SECRET {name} ({', '.join(parts)})"


# ── base and patches ─────────────────────────────────────────────────────────────────────────


def duckdb_type(arrow: pa.DataType) -> str:
    """DuckDB's name for a Delta column type (for a column no Parquet file holds yet)."""
    if pa_types.is_string(arrow) or pa_types.is_large_string(arrow) or pa_types.is_string_view(arrow):
        return "VARCHAR"
    if pa_types.is_list(arrow) or pa_types.is_large_list(arrow):
        return duckdb_type(arrow.value_type) + "[]"
    if pa_types.is_timestamp(arrow):
        if arrow.unit != "us":
            raise ValueError(f"timestamps are stored in microseconds, not {arrow.unit}")
        return "TIMESTAMPTZ" if arrow.tz else "TIMESTAMP"
    simple = {
        pa.float64(): "DOUBLE",
        pa.float32(): "FLOAT",
        pa.int64(): "BIGINT",
        pa.int32(): "INTEGER",
        pa.int16(): "SMALLINT",
        pa.int8(): "TINYINT",
        pa.bool_(): "BOOLEAN",
        pa.date32(): "DATE",
    }
    if arrow in simple:
        return simple[arrow]
    raise ValueError(f"no DuckDB type for Delta column type {arrow}")


def _file_path(uri: str) -> str:
    """A file URI from the Delta log as DuckDB opens it: percent-decoded (delta-rs encodes
    spaces and non-ASCII), `file://` turned into a path, never a glob."""
    if uri.startswith("file://"):
        path = unquote(urlparse(uri).path)
        decoded = path[1:] if re.match(r"^/[A-Za-z]:", path) else path
    else:
        decoded = unquote(uri)
    if _GLOB_CHARACTERS.search(decoded):
        raise ValueError(f"base file path has glob characters DuckDB would expand: {decoded!r}")
    return decoded


def _refuse_unreadable(table: DeltaTable) -> None:
    blockers = _FILE_BY_FILE_BLOCKERS & set(table.protocol().reader_features or ())
    if table.metadata().configuration.get("delta.columnMapping.mode", "none") != "none":
        blockers |= {"columnMapping"}  # legacy reader-v2 tables list no reader features
    if blockers or table.metadata().partition_columns:
        raise ValueError(f"base table can't be read file by file (features {sorted(blockers)} or partitions)")


def _table_view(
    con: duckdb.DuckDBPyConnection,
    view: str,
    source: str,
    present: Iterable[str],
    schema: pa.Schema,
    fields: Iterable[FieldSpec],
) -> None:
    """`view` over `source` with every column of `schema` and every snapshot field. A field whose
    type in the revision's schema (`schemaRef`) is not its column's type is refused, never cast:
    reading a revision with another revision's schema would change its values."""
    have = set(present)
    wanted = [(item.name, duckdb_type(item.type)) for item in schema]
    known = dict(wanted)
    clashes = [
        f"{spec.field_id} is {spec.value_type} but its column is {known[spec.column]}"
        for spec in fields
        if spec.column in known and known[spec.column] not in _COLUMN_TYPES[spec.value_type]
    ]
    if clashes:
        raise ValueError(f"the revision's schema does not match its base: {'; '.join(clashes)}")
    wanted += [(spec.column, _FIELD_TYPE[spec.value_type]) for spec in fields if spec.column not in known]
    added = "".join(f', CAST(NULL AS {kind}) AS "{name}"' for name, kind in wanted if name not in have)
    con.execute(f"CREATE OR REPLACE TEMP VIEW {view} AS SELECT *{added} FROM {source}")


def _register_delta(
    con: duckdb.DuckDBPyConnection,
    view: str,
    table_version: TableVersion,
    options: Mapping[str, str],
    fields: Iterable[FieldSpec],
) -> None:
    """`view` = the Parquet files of one Delta table at its pinned version, read lazily."""
    table = DeltaTable(table_version.uri, version=table_version.version, storage_options=dict(options))
    _refuse_unreadable(table)
    schema = pa.schema(table.schema().to_arrow())
    files = [_file_path(uri) for uri in table.file_uris()]
    if not files:  # a table without rows has no files, and DuckDB can't read an empty file list
        con.register(f"{view}_rows", schema.empty_table())
        _table_view(con, view, f"{view}_rows", schema.names, schema, fields)
        return
    if any(path.startswith("s3://") for path in files):
        _load_s3_extensions(con)
        con.execute(s3_secret_sql(urlparse(table_version.uri).netloc, options))
    source = f"read_parquet([{', '.join(_quote(path) for path in files)}], union_by_name = true)"
    present = [str(row[0]) for row in con.execute(f"DESCRIBE SELECT * FROM {source}").fetchall()]
    _table_view(con, view, source, present, schema, fields)


def register_base(con: duckdb.DuckDBPyConnection, snapshot: Snapshot) -> None:
    """`base` = the main visit's rows at the pinned version."""
    base = TableVersion(snapshot.base_uri, snapshot.base_version)
    _register_delta(con, "base", base, snapshot.storage_options, snapshot.fields)


# The key of a follow-up visit row; a dataset without visits yet reads an empty table of it.
VISIT_KEY = pa.schema([(ROW_ID, pa.string()), (VISIT, pa.int32())])


def register_visits(con: duckdb.DuckDBPyConnection, snapshot: Snapshot) -> None:
    """`visits` = the follow-up visit rows at the pinned version (empty when there are none)."""
    if snapshot.followups is None:
        con.register("visits_rows", VISIT_KEY.empty_table())
        _table_view(con, "visits", "visits_rows", VISIT_KEY.names, VISIT_KEY, snapshot.visit_fields)
        return
    _register_delta(con, "visits", snapshot.followups, snapshot.storage_options, snapshot.visit_fields)


def register_base_rows(con: duckdb.DuckDBPyConnection, rows: pa.Table, fields: Iterable[FieldSpec]) -> None:
    """`base` over rows already in memory (the pure merge)."""
    con.register("base_rows", rows)
    _table_view(con, "base", "base_rows", rows.schema.names, rows.schema, fields)


def register_visit_rows(con: duckdb.DuckDBPyConnection, rows: pa.Table, fields: Iterable[FieldSpec]) -> None:
    """`visits` over visit rows already in memory (the pure merge)."""
    con.register("visits_rows", rows)
    _table_view(con, "visits", "visits_rows", rows.schema.names, rows.schema, fields)


def register_patches(con: duckdb.DuckDBPyConnection, patches: pa.Table) -> None:
    if patches.schema != PATCH_SCHEMA:
        raise ValueError("patch source returned an unexpected schema")
    con.register("patches", patches)
