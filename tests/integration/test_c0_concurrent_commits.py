"""C0 (consistency.md §13): concurrent commits to one table on S3 never overwrite each other.

delta-rs ≥ 1.6 writes each log entry with an S3 conditional write; S3 refuses the second
writer of a version (412) and delta-rs retries on the next one. Verified on real AWS S3 on
2026-10-24 (20 writers: every successful commit kept, the rest failed loudly). This test keeps
the guarantee in CI: if an upgrade ever silently overwrites a commit, its file leaves the log.

Checked on the Delta log (add actions + their stats), not by reading Parquet: moto's server
cannot serve delta-rs data reads (requests stall) — see tests/integration/conftest.py.
"""

import threading

import pyarrow as pa
from deltalake import DeltaTable, write_deltalake

from tests.integration.conftest import S3

WRITERS = 10
FIRST_ROW = -1


def _writers_in_log(table: DeltaTable) -> list[int]:
    """One single-row file per commit: its min stat is the writer that committed it."""
    actions = pa.table(table.get_add_actions(flatten=True))  # arro3 table -> pyarrow via the Arrow C interface
    return sorted(int(value) for value in actions.column("min.writer").to_pylist())


def test_concurrent_commits_keep_every_success_and_fail_the_rest_loudly(s3: S3) -> None:
    uri = s3.table_uri()
    write_deltalake(uri, pa.table({"writer": [FIRST_ROW]}), storage_options=s3.storage_options)
    succeeded: list[int] = []
    failures: list[Exception] = []

    def append(writer: int) -> None:
        try:
            write_deltalake(uri, pa.table({"writer": [writer]}), mode="append", storage_options=s3.storage_options)
            succeeded.append(writer)
        except Exception as error:  # a loud failure is allowed; a silent loss is not
            failures.append(error)

    threads = [threading.Thread(target=append, args=(writer,)) for writer in range(WRITERS)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    table = DeltaTable(uri, storage_options=s3.storage_options)
    assert succeeded, f"no writer succeeded: {failures}"
    assert table.version() == len(succeeded)
    assert _writers_in_log(table) == sorted([FIRST_ROW, *succeeded])
