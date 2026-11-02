"""Seed one Data Service dataset for the walking skeleton — the Delta side only.

    uv run python probes/seed_skeleton_dataset.py <rows> [--lake-root .lake] [--owner <userId>] [--bench]

Writes the base table of a small CRF (numbers, texts, a date, single and multi choices) under
<lake-root>/datasets/<datasetId>/table through the fenced create, streamed from DuckDB so 1M
rows never sit in memory. Ids look like Node's: 24-hex ObjectIds, row ids time-ordered.

Prints, and saves next to the table as seed.json, what Node's seed script needs to write the
Mongo side (the form, datasets, dataset_revisions, dataset_schemas — Node owns those
collections) and the revision-1 manifest the query endpoints take. With --bench, also times a
grid page and a count through the query service (best of 3).
"""

import argparse
import json
import time
from collections.abc import Callable
from pathlib import Path

import duckdb
import pyarrow as pa

from dataservice.common.utils.dataset_token import DatasetClaims
from dataservice.modules.datasets.schemas import ManifestDto, RowsQueryRequest
from dataservice.modules.datasets.service import count_rows, query_rows
from dataservice.storage.fenced_commit import CommitContext, OpRef, create
from dataservice.storage.layout import table_uri
from dataservice.storage.snapshot import PATCH_SCHEMA

EPOCH = 1_767_225_600  # 2026-01-01T00:00:00Z: row i was created i seconds later
RETENTION_DAYS = 60  # Settings.delta_retention_days' default


def _id(prefix: str, number: int) -> str:
    return f"{prefix}{number:0{24 - len(prefix)}x}"


# (label, Node field type, value type, options) — the shapes a real CRF has.
FORM = (
    ("Age", "number", "num", ()),
    ("Sex", "radio", "opts", ("Male", "Female")),
    ("Patient name", "text", "str", ()),
    ("Visit date", "date", "date", ()),
    ("BMI", "number", "num", ()),
    ("Symptoms", "checkbox", "opts", ("Fever", "Cough", "Headache")),
    ("Smoker", "radio", "opts", ("Yes", "No")),
    ("Notes", "textarea", "str", ()),
)


def fields() -> list[dict[str, object]]:
    found: list[dict[str, object]] = []
    for index, (label, node_type, value_type, options) in enumerate(FORM):
        field_id = _id("5eedf", index)
        found.append(
            {
                "fieldId": field_id,
                "valueType": value_type,
                "label": label,
                "type": node_type,
                "options": [{"optionId": _id(f"5eed0{index:x}", k), "label": text} for k, text in enumerate(options)],
            }
        )
    return found


def _column(field: dict[str, object], index: int) -> str:
    column, kind = f'"f_{field["fieldId"]}"', field["valueType"]
    options = field["options"]
    assert isinstance(options, list)
    ids = [str(option["optionId"]) for option in options]
    if kind == "num":
        return f"CAST(round(18 + (hash(i * 1000 + {index}) % 6000) / 100.0, 1) AS DOUBLE) AS {column}"
    if kind == "str":
        return f"CASE WHEN i % 10 = 0 THEN NULL ELSE 'value ' || (hash(i * 1000 + {index}) % 500) END AS {column}"
    if kind == "date":
        return f"DATE '2026-01-01' + CAST(hash(i * 1000 + {index}) % 300 AS INTEGER) AS {column}"
    if field["type"] == "radio":
        choices = " ".join(f"WHEN {k} THEN ['{option}']" for k, option in enumerate(ids))
        return f"CASE hash(i * 1000 + {index}) % {len(ids) + 1} {choices} ELSE []::VARCHAR[] END AS {column}"
    picked = ", ".join(
        f"CASE WHEN hash(i * 1000 + {index * 10 + k}) % 2 = 0 THEN '{option}' END" for k, option in enumerate(ids)
    )
    return f"list_filter([{picked}], lambda o: o IS NOT NULL) AS {column}"


def seed(rows: int, lake_root: str, owner: str) -> dict[str, object]:
    dataset_id, form = _id("5eed", rows), fields()
    columns = [
        f"printf('%08x%016x', {EPOCH} + i, i) AS _rid",
        f"'{owner}' AS _created_by",
        f"to_timestamp({EPOCH} + i) AS _created_at",
        f"to_timestamp({EPOCH} + i) AS _updated_at",
        "CAST(NULL AS TIMESTAMPTZ) AS _deleted_at",
        *(_column(field, index) for index, field in enumerate(form)),
    ]
    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'")
    reader = con.sql(f"SELECT {', '.join(columns)} FROM range({rows}) t(i)").to_arrow_reader(200_000)
    uri = table_uri(lake_root, dataset_id, "table")
    context = CommitContext(1, (OpRef(f"seed:{dataset_id}", 1),), ("base",))
    started = time.perf_counter()
    table = create(uri, reader, context, retention_days=RETENTION_DAYS)
    print(f"wrote {rows:,} rows to {uri} in {time.perf_counter() - started:.1f} s")
    manifest = {
        "revision": 1,
        "tables": {"base": {"version": table.version()}, "followups": None},
        "patches": {"fromSeq": 0, "toSeq": 0},
        "fields": [{"fieldId": field["fieldId"], "valueType": field["valueType"]} for field in form],
        "rowCount": rows,
        "dataVersion": 1,
    }
    return {"datasetId": dataset_id, "owner": owner, "lakeRoot": lake_root, "form": form, "manifest": manifest}


class _NoPatches:
    def patches(self, dataset_id: str, from_seq: int, to_seq: int) -> pa.Table:
        return PATCH_SCHEMA.empty_table()


def bench(seeded: dict[str, object]) -> None:
    dataset_id, lake_root = str(seeded["datasetId"]), str(seeded["lakeRoot"])
    manifest = ManifestDto.model_validate(seeded["manifest"])
    claims = DatasetClaims(user_id=str(seeded["owner"]), dataset_id=dataset_id, scope="read", row_scope="all")
    cases: dict[str, Callable[[], object]] = {
        "grid page (50, newest first)": lambda: query_rows(
            lake_root, dataset_id, RowsQueryRequest(manifest=manifest, limit=50), claims, _NoPatches()
        ),
        "count": lambda: count_rows(lake_root, dataset_id, manifest, claims, _NoPatches()),
    }
    for name, run in cases.items():
        timings = []
        for _ in range(3):
            started = time.perf_counter()
            run()
            timings.append(time.perf_counter() - started)
        print(f"{name}: best {min(timings) * 1000:.0f} ms of 3")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("rows", type=int)
    parser.add_argument("--lake-root", default=".lake")
    parser.add_argument("--owner", default=_id("5eedaa", 1), help="the user id that creates every row")
    parser.add_argument("--bench", action="store_true")
    args = parser.parse_args()
    seeded = seed(args.rows, str(Path(args.lake_root).resolve()), args.owner)
    out = Path(table_uri(str(seeded["lakeRoot"]), str(seeded["datasetId"]), "seed.json"))
    out.write_text(json.dumps(seeded, indent=2), encoding="utf-8")
    print(f"seed description: {out}")
    if args.bench:
        bench(seeded)


if __name__ == "__main__":
    main()
