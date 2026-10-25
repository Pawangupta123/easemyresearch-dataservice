"""The ONE reader (hybrid edits): grid, analysis, export and compaction all read a revision here.

A revision = the Delta base at its pinned version + the Mongo patches with
`patch_from < seq ≤ patch_to` (rules in `sql.py`). Only patches inside the revision's range
count, so every reader of revision n sees exactly the same rows — before and after a compaction
folds patches into base. Nothing outside this package may read Delta data
(tests/unit/test_architecture.py enforces it).

Built for GB-scale bases — the base is never loaded whole: DuckDB reads only the needed columns
of the pinned version's Parquet files, filters reach the Parquet scan, patches join once and
only for what they touch, and filter / sort / paging / count run inside DuckDB on one shared,
budgeted engine (`engine.py`).
"""

from dataservice.storage.reader.engine import ReaderLimits, configure, install_extensions
from dataservice.storage.reader.model import (
    DELETED_AT,
    EVERY_ROW,
    ROW_ID,
    SCAN_BATCH_ROWS,
    VISIT,
    Filter,
    FilterOp,
    Query,
    Sort,
)
from dataservice.storage.reader.read import DEEP_OFFSET, PAGE_ROWS_MAX, count, explain, merge, query, read_rows, scan
from dataservice.storage.reader.visits import merge_visits, query_visits

__all__ = [
    "DEEP_OFFSET",
    "DELETED_AT",
    "EVERY_ROW",
    "PAGE_ROWS_MAX",
    "ROW_ID",
    "SCAN_BATCH_ROWS",
    "VISIT",
    "Filter",
    "FilterOp",
    "Query",
    "ReaderLimits",
    "Sort",
    "configure",
    "count",
    "explain",
    "install_extensions",
    "merge",
    "merge_visits",
    "query",
    "query_visits",
    "read_rows",
    "scan",
]
