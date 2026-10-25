"""Benchmark of the one reader on synthetic data — time and peak memory of every read path.

    uv run python probes/bench_reader.py <rows> [--dir DIR] [--patches 0,1000,100000]

Builds (once per DIR) a Delta base of <rows> rows × 50 fields (20 numbers, 15 texts, 10 dates,
5 multi-selects; 1% of rows in the trash), streamed so building never holds the table. Then
each case runs in its own process — so its peak memory is its own — for each patch count:
grid pages (sorted / filtered, on fields with and without patches), a deep page, counts, a
full streamed read (export) and, last, a compaction of the largest patch set.

Prints a Markdown table for docs/STATUS.md. Local disk only; nothing is uploaded anywhere.
"""

import argparse
import json
import os
import random
import subprocess
import sys
import time
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import duckdb
import pyarrow as pa
from deltalake import DeltaTable, write_deltalake

from dataservice.storage.compaction import compact
from dataservice.storage.fenced_commit import CommitContext, OpRef, max_token_seen
from dataservice.storage.reader import Filter, Query, Sort, count, query, scan
from dataservice.storage.snapshot import PATCH_SCHEMA, FieldSpec, Snapshot

NUMS, STRS, DATES, OPTS = 20, 15, 10, 5
FIELDS = (
    tuple(FieldSpec(f"n{i:02d}", "num") for i in range(NUMS))
    + tuple(FieldSpec(f"s{i:02d}", "str") for i in range(STRS))
    + tuple(FieldSpec(f"d{i:02d}", "date") for i in range(DATES))
    + tuple(FieldSpec(f"o{i:02d}", "opts") for i in range(OPTS))
)
# Patches touch these fields only; n10 / n11 / s05 stay plain base columns (pushdown paths).
PATCHED = ("n00", "n01", "n02", "s00", "s01", "d00", "o00")
GRID = tuple(spec.field_id for spec in FIELDS[:20])


def build(uri: str, rows: int) -> None:
    """The base, generated inside DuckDB and streamed into Delta in batches."""
    columns = [
        "printf('r%010d', i) AS _rid",
        "CASE WHEN i % 100 = 0 THEN TIMESTAMPTZ '2026-10-01 00:00:00+00' END AS _deleted_at",
    ]
    for k in range(NUMS):  # half lab-style values (1 decimal), half free floats
        value = f"round((hash(i, {k}) % 10000) / 100.0, 1)" if k % 2 == 0 else f"(hash(i, {k}) % 1000000007) / 7.0"
        columns.append(f"CAST({value} AS DOUBLE) AS f_n{k:02d}")
    for k in range(STRS):
        value = "'patient ' || i" if k == 14 else f"'s' || (hash(i, {100 + k}) % 50)"
        columns.append(f"{value} AS f_s{k:02d}")
    for k in range(DATES):
        columns.append(f"DATE '2020-01-01' + CAST(hash(i, {200 + k}) % 2000 AS INTEGER) AS f_d{k:02d}")
    for k in range(OPTS):
        columns.append(
            f"CASE hash(i, {300 + k}) % 3 WHEN 0 THEN []::VARCHAR[] WHEN 1 THEN ['o_a']"
            f" ELSE ['o_a', 'o_b'] END AS f_o{k:02d}"
        )
    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'")
    reader = con.sql(f"SELECT {', '.join(columns)} FROM range({rows}) t(i)").to_arrow_reader(200_000)
    write_deltalake(uri, reader, target_file_size=128 * 1024 * 1024)


def patches(rows: int, n: int) -> pa.Table:
    """n patches Node could have written: 90% cell sets, 5% deletes, 4% restores, 1% inserts."""
    rng, at = random.Random(n), datetime(2026, 10, 25, tzinfo=UTC)
    out: list[dict[str, object]] = []
    for seq in range(1, n + 1):
        roll, row = rng.random(), f"r{rng.randrange(rows):010d}"
        base: dict[str, object] = {"seq": seq, "row": row, "field": None, "t": None, "at": at}
        values: dict[str, object] = dict.fromkeys(("v_str", "v_num", "v_date", "v_opts"))
        if roll < 0.90:
            field = rng.choice(PATCHED)
            kind = {"n": "num", "s": "str", "d": "date", "o": "opts"}[field[0]]
            value = {
                "num": rng.uniform(0, 100),
                "str": f"edited {seq}",
                "date": date(2026, 1, 1) + timedelta(days=rng.randrange(300)),
                "opts": ["o_b"],
            }[kind]
            values[f"v_{kind}"] = value
            out.append(base | values | {"op": "set", "field": field, "t": kind})
        elif roll < 0.95:
            out.append(base | values | {"op": "delete_row"})
        elif roll < 0.99:
            out.append(base | values | {"op": "restore_row"})
        else:
            out.append(base | values | {"op": "insert_row", "row": f"new{seq:010d}"})
    return pa.Table.from_pylist(out, schema=PATCH_SCHEMA)


class Source:
    def __init__(self, table: pa.Table) -> None:
        self.table = table

    def patches(self, dataset_id: str, from_seq: int, to_seq: int) -> pa.Table:
        return self.table


def peak_mb() -> float:
    if sys.platform == "win32":
        import ctypes
        from ctypes import wintypes

        class Counters(ctypes.Structure):
            _fields_ = [
                ("cb", wintypes.DWORD),
                ("PageFaultCount", wintypes.DWORD),
                ("PeakWorkingSetSize", ctypes.c_size_t),
                ("WorkingSetSize", ctypes.c_size_t),
                ("QuotaPeakPagedPoolUsage", ctypes.c_size_t),
                ("QuotaPagedPoolUsage", ctypes.c_size_t),
                ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t),
                ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
                ("PagefileUsage", ctypes.c_size_t),
                ("PeakPagefileUsage", ctypes.c_size_t),
            ]

        counters = Counters()
        counters.cb = ctypes.sizeof(Counters)
        kernel32 = ctypes.WinDLL("kernel32")
        kernel32.GetCurrentProcess.restype = wintypes.HANDLE
        kernel32.K32GetProcessMemoryInfo.argtypes = [wintypes.HANDLE, ctypes.POINTER(Counters), wintypes.DWORD]
        if not kernel32.K32GetProcessMemoryInfo(kernel32.GetCurrentProcess(), ctypes.byref(counters), counters.cb):
            raise OSError("GetProcessMemoryInfo failed")
        return float(counters.PeakWorkingSetSize) / 2**20
    import resource

    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024


CASES: dict[str, Query | str] = {
    "page sorted (plain field)": Query(fields=GRID, sort=(Sort("n10", descending=True),), limit=50),
    "page sorted (patched field)": Query(fields=GRID, sort=(Sort("n00", descending=True),), limit=50),
    "page filtered (plain, ~2%)": Query(fields=GRID, filters=(Filter("n11", "gt", 1.4e8),), limit=50),
    "page filtered (patched)": Query(fields=GRID, filters=(Filter("n00", "gt", 99.0),), limit=50),
    "page text search": Query(fields=GRID, filters=(Filter("s14", "contains", "4242"),), limit=50),
    "deep page (offset 500k)": Query(fields=GRID, sort=(Sort("n10"),), limit=50, offset=500_000),
    "count all": "count",
    "count filtered": "count-filtered",
    "full read, streamed": "scan",
    "compaction": "compact",
}


REPEATS = 3  # best of 3 — the first run of a process also pays DuckDB's warm-up


def run_once(snapshot: Snapshot, source: Source, case: Query | str, uri: str) -> object:
    if isinstance(case, Query):
        return query(snapshot, source, case).num_rows
    if case == "count":
        return count(snapshot, source)
    if case == "count-filtered":
        return count(snapshot, source, Query(filters=(Filter("n11", "gt", 1.4e8),)))
    if case == "scan":
        rows = 0
        with scan(snapshot, source, include_deleted=True) as reader:
            for batch in reader:
                rows += batch.num_rows
        return rows
    token = max_token_seen([DeltaTable(uri)]) + 1
    return compact(snapshot, source, CommitContext(token, (OpRef("bench-compact", 1),), ("base",))).rows


def run_case(directory: Path, name: str, n_patches: int) -> dict[str, object]:
    meta = json.loads((directory / "meta.json").read_text())
    snapshot = Snapshot("bench", meta["uri"], meta["version"], 0, n_patches, FIELDS)
    source = Source(patches(meta["rows"], n_patches))
    case, started_mb = CASES[name], peak_mb()
    repeats = 1 if case == "compact" else REPEATS
    timings: list[float] = []
    for _ in range(repeats):
        started = time.perf_counter()
        result = run_once(snapshot, source, case, meta["uri"])
        timings.append(time.perf_counter() - started)
    seconds = min(timings)
    return {"seconds": seconds, "peak_mb": peak_mb(), "before_mb": started_mb, "result": result}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("rows", type=int)
    parser.add_argument("--dir", type=Path)
    parser.add_argument("--patches", default="0,1000,100000")
    parser.add_argument("--case")  # internal: run one case in this process
    parser.add_argument("--n", type=int, default=0)
    args = parser.parse_args()
    directory: Path = args.dir or Path.cwd() / ".bench" / f"rows-{args.rows}"
    if args.case:
        print(json.dumps(run_case(directory, args.case, args.n)))
        return 0
    uri = str(directory / "base")
    if not (directory / "meta.json").exists():
        directory.mkdir(parents=True, exist_ok=True)
        started = time.perf_counter()
        build(uri, args.rows)
        version = DeltaTable(uri).version()
        (directory / "meta.json").write_text(json.dumps({"uri": uri, "version": version, "rows": args.rows}))
        print(f"built {args.rows:,} rows in {time.perf_counter() - started:.1f}s", file=sys.stderr)
    size = sum(path.stat().st_size for path in Path(uri).glob("*.parquet")) / 2**20
    counts = [int(value) for value in args.patches.split(",")]
    print(f"\n**{args.rows:,} rows × {len(FIELDS)} fields** — {size:,.0f} MB of Parquet, {os.cpu_count()} cores\n")
    print("| case | patches | time (best of 3) | peak memory | result |\n|---|---|---|---|---|")
    for name in CASES:
        for n in counts if name != "compaction" else counts[-1:]:
            command = [sys.executable, __file__, str(args.rows), "--dir", str(directory), "--case", name, "--n", str(n)]
            done = subprocess.run(command, capture_output=True, text=True, check=False)
            if done.returncode:
                print(f"| {name} | {n:,} | FAILED | | {done.stderr.strip().splitlines()[-1][:120]} |")
                continue
            out = json.loads(done.stdout.strip().splitlines()[-1])
            print(
                f"| {name} | {n:,} | {out['seconds'] * 1000:,.0f} ms | {out['peak_mb']:,.0f} MB"
                f" (start {out['before_mb']:,.0f}) | {out['result']:,} |",
                flush=True,
            )
    return 0


if __name__ == "__main__":
    sys.exit(main())
