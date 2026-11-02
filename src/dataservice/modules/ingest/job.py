"""The `ingest_upload` job (skeleton step 7c): an import's rows straight from its upload — the
writer runs it under the lease like any Delta job. Node sends, per column, what its own import
rules read (the form's field and option ids, the accepted corrections, the proven date order);
the cells are cast with those rules (detect/cast.py, golden-tested) and appended to the dataset's
first table, which Node had the Data Service create empty at confirm.

A wrong payload or an upload that is not a workbook is not retried; a failed read of the upload is.
"""

from collections.abc import Mapping
from datetime import UTC, date, datetime
from typing import Literal

import pyarrow as pa
from pydantic import BaseModel, ConfigDict, Field
from pydantic import ValidationError as PydanticValidationError
from pydantic.alias_generators import to_camel

from dataservice.common.errors import AppError
from dataservice.config import get_settings
from dataservice.modules.ingest.detect.cast import CellValue, FieldSpec, cast_rows
from dataservice.modules.ingest.detect.cells import read_first_sheet
from dataservice.modules.ingest.service import check_source
from dataservice.storage.fenced_commit import CommitContext, append
from dataservice.storage.layout import BASE
from dataservice.storage.operations import JobResult, WriterTables
from dataservice.storage.reader.model import CREATED_AT, CREATED_BY, DELETED_AT, ROW_ID, UPDATED_AT
from dataservice.storage.staging import read_staged_bytes

JOB_TYPE = "ingest_upload"


class _Body(BaseModel):
    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True, extra="forbid", frozen=True)


class IngestField(_Body):
    field_id: str = Field(pattern=r"^[0-9a-f]{24}$")
    column_index: int = Field(ge=1)
    type: str
    value_type: Literal["str", "num", "date", "opts"]
    date_format: Literal["DMY", "MDY"] | None
    corrections: dict[str, str]
    options: dict[str, str]


class IngestUploadPayload(_Body):
    source: str
    import_id: str
    started_at: datetime
    user_id: str
    fields: list[IngestField] = Field(min_length=1)


def _specs(payload: IngestUploadPayload) -> list[FieldSpec]:
    return [
        FieldSpec(
            field.field_id,
            field.column_index,
            field.type,
            field.value_type,
            field.date_format,
            field.corrections,
            field.options,
        )
        for field in payload.fields
    ]


def _arrow_value(value: CellValue, arrow_type: pa.DataType) -> object:
    """A cast cell as its column takes it (a day comes as yyyy-mm-dd)."""
    if isinstance(value, str) and pa.types.is_date(arrow_type):
        return date.fromisoformat(value)
    return value


def ingest_upload(payload: Mapping[str, object], tables: WriterTables, context: CommitContext) -> JobResult:
    try:
        job = IngestUploadPayload.model_validate(payload)
    except PydanticValidationError as error:
        raise AppError(f"ingest_upload payload is not valid: {error}", user_safe=False) from error
    check_source(job.source, get_settings().import_bucket or "")
    sheet = read_first_sheet(read_staged_bytes(job.source))  # a failed read is retried
    if sheet is None:
        raise AppError("the upload has no sheet", user_safe=False)

    table = tables.open(BASE)
    schema = pa.schema(table.schema().to_arrow())
    missing = [field.field_id for field in job.fields if f"f_{field.field_id}" not in schema.names]
    if missing:
        raise AppError(f"the dataset's table has no column for fields {missing}", user_safe=False)

    rows = list(cast_rows(sheet, _specs(job), job.import_id, job.started_at))
    now = datetime.now(UTC)
    meta: dict[str, object] = {CREATED_BY: job.user_id, CREATED_AT: now, UPDATED_AT: now, DELETED_AT: None}
    columns: dict[str, pa.Array] = {}
    for name in schema.names:
        arrow_type = schema.field(name).type
        if name == ROW_ID:
            values: list[object] = [row[ROW_ID] for row in rows]
        elif name in meta:
            values = [meta[name]] * len(rows)
        else:
            values = [_arrow_value(row.get(name), arrow_type) for row in rows]
        columns[name] = pa.array(values, arrow_type)
    append(table, pa.table(columns), context)
    return JobResult(rows_added=len(rows), changed_field_ids=tuple(field.field_id for field in job.fields))
