"""A dataset's first Delta table, from the rows of its first import (walking skeleton).

Node parses and type-checks the upload with its existing import rules and sends the rows here;
the table is written through the fenced create, so a retry can never overwrite what a first
attempt wrote. The full design moves detection and validation here too (PRD D12, plan step 7).
"""

from datetime import date

import pyarrow as pa
from deltalake import DeltaTable

from dataservice.common.errors import ConflictError, ValidationError
from dataservice.modules.datasets.schemas import CellValue, TableCreated, TableCreateRequest
from dataservice.modules.datasets.service import table_uri
from dataservice.storage.fenced_commit import CommitContext, OpRef, create
from dataservice.storage.snapshot import UTC_TIMESTAMP, ValueType

# The rows one request may carry (a 10 MB xlsx is far below it).
MAX_ROWS = 2_000_000
# A first table is written before any lease exists: token 1, the lowest a writer can hold.
CREATE_TOKEN = 1


def _num(value: CellValue) -> float | None:
    if value is None or (isinstance(value, float) and not isinstance(value, bool)):
        return value
    raise ValueError("a number")


def _text(value: CellValue) -> str | None:
    if value is None or isinstance(value, str):
        return value
    raise ValueError("a text")


def _day(value: CellValue) -> date | None:
    if value is None:
        return None
    if isinstance(value, str):
        return date.fromisoformat(value)  # yyyy-mm-dd; anything else raises ValueError
    raise ValueError("a yyyy-mm-dd date")


def _options(value: CellValue) -> list[str] | None:
    if value is None or (isinstance(value, list) and all(isinstance(item, str) for item in value)):
        return value
    raise ValueError("a list of option ids")


_COLUMN = {
    "num": (_num, pa.float64()),
    "str": (_text, pa.string()),
    "date": (_day, pa.date32()),
    "opts": (_options, pa.list_(pa.string())),
}


def _column(field_id: str, value_type: ValueType, values: list[CellValue]) -> pa.Array:
    cast, arrow = _COLUMN[value_type]
    try:
        return pa.array([cast(value) for value in values], arrow)
    except ValueError as error:
        raise ValidationError(f"Field {field_id}: every value must be {error} or null") from None


def create_table(lake_root: str, dataset_id: str, request: TableCreateRequest, retention_days: int) -> TableCreated:
    rows = request.rows
    size = len(rows.rid)
    if size > MAX_ROWS:
        raise ValidationError(f"At most {MAX_ROWS} rows in one table create")
    if len(set(rows.rid)) != size:
        raise ValidationError("Row ids must be unique")
    uri = table_uri(lake_root, dataset_id, "table")
    if DeltaTable.is_deltatable(uri):
        raise ConflictError("This dataset already has its table")
    columns: dict[str, pa.Array] = {
        "_rid": pa.array(rows.rid, pa.string()),
        "_created_by": pa.array(rows.created_by, pa.string()),
        "_created_at": pa.array(rows.created_at, UTC_TIMESTAMP),
        "_updated_at": pa.array(rows.created_at, UTC_TIMESTAMP),
        "_deleted_at": pa.nulls(size, UTC_TIMESTAMP),
    }
    for item in request.fields:
        columns[f"f_{item.field_id}"] = _column(item.field_id, item.value_type, rows.values[item.field_id])
    context = CommitContext(CREATE_TOKEN, (OpRef(f"import:{dataset_id}", 1),), ("base",))
    table = create(uri, pa.table(columns), context, retention_days=retention_days)
    return TableCreated(version=table.version(), row_count=size)
