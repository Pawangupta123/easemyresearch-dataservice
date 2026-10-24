"""Reads the Mongo `dataset_patches` of a revision (hybrid edits) — the ONLY Mongo collection
the Data Service touches, and only to read. Node writes patches in its edit transaction.

Patch document (written by Node):
    { datasetId, seq, op, target: { row, field?, scope: { kind: "main", followUp: 0 } },
      value: { t, v } | null, opId, batchId, actor, at }
"""

from collections.abc import Mapping
from datetime import UTC, datetime

import pyarrow as pa
from pymongo.collection import Collection
from pymongo.database import Database

from dataservice.storage.snapshot import PATCH_SCHEMA, VALUE_COLUMN

Document = dict[str, object]
COLUMN_FOR_TYPE: dict[str, str] = {str(value_type): column for value_type, column in VALUE_COLUMN.items()}


def _as_mapping(value: object) -> Mapping[str, object]:
    return value if isinstance(value, Mapping) else {}


def _cell(t: object, v: object) -> dict[str, object]:
    columns: dict[str, object] = dict.fromkeys(COLUMN_FOR_TYPE.values())
    column = COLUMN_FOR_TYPE.get(t) if isinstance(t, str) else None
    if column is None:
        return columns  # a cleared cell ("null") or a row op
    if column == "v_date" and isinstance(v, datetime):
        v = v.date()  # BSON has no date-only type; Node stores midnight UTC
    elif column == "v_num" and isinstance(v, int) and not isinstance(v, bool):
        v = float(v)
    columns[column] = v
    return columns


def _utc(value: object) -> datetime | None:
    if not isinstance(value, datetime):
        return None
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)  # pymongo: naive UTC


def to_patch_row(document: Mapping[str, object]) -> dict[str, object]:
    target, value = _as_mapping(document.get("target")), _as_mapping(document.get("value"))
    t = value.get("t")
    return (
        {"seq": document["seq"], "op": document["op"], "row": target.get("row"), "field": target.get("field"), "t": t}
        | _cell(t, value.get("v"))
        | {"at": _utc(document.get("at"))}
    )


class MongoPatchSource:
    def __init__(self, database: Database[Document]) -> None:
        self.patches_collection: Collection[Document] = database["dataset_patches"]

    def patches(self, dataset_id: str, from_seq: int, to_seq: int) -> pa.Table:
        cursor = self.patches_collection.find(
            {"datasetId": dataset_id, "seq": {"$gt": from_seq, "$lte": to_seq}},
            {"_id": 0, "seq": 1, "op": 1, "target": 1, "value": 1, "at": 1},
        ).sort("seq", 1)
        return pa.Table.from_pylist([to_patch_row(document) for document in cursor], schema=PATCH_SCHEMA)
