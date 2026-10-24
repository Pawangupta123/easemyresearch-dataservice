"""Live probe: does the installed delta-rs / DuckDB give us what consistency.md relies on?

Run: `uv run python probes/delta_api_probe.py`. Prints one line per fact; re-run after any
deltalake / duckdb upgrade before raising the minimum versions in pyproject.toml.
DuckDB's delta extension is blocked by Windows Application Control on the dev box — run the
last check on Linux (CI / Docker).
"""

import inspect
import tempfile

import duckdb
import pyarrow as pa
from deltalake import CommitProperties, DeltaTable, QueryBuilder, Transaction, write_deltalake
from deltalake import __version__ as deltalake_version

APP = "emr-writer"


def props(token: int, **metadata: str) -> CommitProperties:
    return CommitProperties(
        app_transactions=[Transaction(APP, token)], custom_metadata=metadata or None, max_commit_retries=0
    )


def rows(table: DeltaTable) -> int:
    result = QueryBuilder().register("t", table).execute("select count(*) as n from t").read_all()
    return int(result.column(0)[0].as_py())


def main() -> None:
    print("deltalake", deltalake_version, "| duckdb", duckdb.__version__)
    path = tempfile.mkdtemp()

    write_deltalake(path, pa.table({"_row_id": [1, 2]}), commit_properties=props(5, opId="op1", attempt="1"))
    table = DeltaTable(path)
    meta = {k: v for k, v in table.history(1)[0].items() if k in ("operation", "opId", "attempt")}
    print("txn after commit:", table.transaction_version(APP), "| commit metadata:", meta)

    write_deltalake(path, pa.table({"_row_id": [3]}), mode="append", commit_properties=props(6))
    DeltaTable(path).restore(0, commit_properties=props(7, kind="reconcile"))
    restored = DeltaTable(path)
    print(
        "restore -> version", restored.version(), "| rows", rows(restored), "| txn", restored.transaction_version(APP)
    )
    print("time travel v1 rows:", rows(DeltaTable(path, version=1)))
    print("vacuum has keep_versions:", "keep_versions" in inspect.signature(restored.vacuum).parameters)
    print("alter.set_table_properties:", hasattr(restored.alter, "set_table_properties"))

    first, second = DeltaTable(path), DeltaTable(path)
    write_deltalake(first, pa.table({"_row_id": [9]}), mode="append", commit_properties=props(8))
    try:
        write_deltalake(second, pa.table({"_row_id": [10]}), mode="append", commit_properties=props(8))
        print("same-app-id race: second commit SUCCEEDED (fence broken)")
    except Exception as error:
        print("same-app-id race: second commit rejected ->", type(error).__name__)

    try:
        con = duckdb.connect()
        con.execute("INSTALL delta; LOAD delta;")
        # DuckDB 2.0: relations keep no result, so fetch through execute(), not sql().fetchone().
        scan = con.execute(f"select count(*) from delta_scan('{path.replace(chr(92), '/')}', version => 1)").fetchone()
        print("duckdb delta_scan(version => 1):", scan)
    except duckdb.Error as error:
        print("duckdb delta extension unavailable here:", str(error)[:120])


if __name__ == "__main__":
    main()
