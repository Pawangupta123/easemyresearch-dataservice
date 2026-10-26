"""The query service's bodies (api.md §2.1) — Node sends camelCase JSON, the same names back.

Every request names a published revision by its manifest, which Node resolves (the current
revision, or the one a grid cursor pinned): nothing is read "at latest". Unknown keys are
refused, so a field Node sends that this service does not yet honour (a filter, a sort) fails
loudly instead of being ignored.
"""

from datetime import datetime
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator
from pydantic.alias_generators import to_camel

from dataservice.storage.reader.read import PAGE_ROWS_MAX
from dataservice.storage.snapshot import FIELD_ID, ValueType

# A dataset, row or user id: a Mongo ObjectId as 24 lowercase hex characters.
OBJECT_ID = r"^[0-9a-f]{24}$"
FieldId = Annotated[str, Field(pattern=f"^{FIELD_ID.pattern}$")]
RowId = Annotated[str, Field(min_length=1, max_length=64)]
# A cell as JSON: num → number, str → string, date → "yyyy-mm-dd", opts → option ids, empty → null.
CellValue = float | str | list[str] | None


class _Body(BaseModel):
    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True, extra="forbid", frozen=True)


class FieldDto(_Body):
    field_id: FieldId
    value_type: ValueType


class TableDto(_Body):
    version: int = Field(ge=0)


class TablesDto(_Body):
    # Required for now: a Data Service dataset gets its base from its first import.
    base: TableDto
    followups: TableDto | None = None  # none until a follow-up visit is submitted


class PatchRangeDto(_Body):
    from_seq: int = Field(ge=0)  # exclusive: already folded into the Delta tables
    to_seq: int = Field(ge=0)  # inclusive

    @model_validator(mode="after")
    def _ordered(self) -> "PatchRangeDto":
        if self.to_seq < self.from_seq:
            raise ValueError("toSeq must not be below fromSeq")
        return self


def _unique(fields: list[FieldDto], name: str) -> list[FieldDto]:
    seen = [item.field_id for item in fields]
    if len(set(seen)) != len(seen):
        raise ValueError(f"{name} lists a field twice")
    return fields


class ManifestDto(_Body):
    """A published revision as the reader needs it (consistency §2), with the field types of its
    `schemaRef` resolved by Node (dataset_schemas)."""

    revision: int = Field(ge=1)
    tables: TablesDto
    patches: PatchRangeDto
    fields: list[FieldDto]
    visit_fields: list[FieldDto] = []  # the fields its follow-up visits capture
    row_count: int = Field(ge=0)
    data_version: int = Field(ge=0)

    @model_validator(mode="after")
    def _distinct_fields(self) -> "ManifestDto":
        _unique(self.fields, "fields")
        _unique(self.visit_fields, "visitFields")
        return self


class RowsQueryRequest(_Body):
    manifest: ManifestDto
    columns: list[FieldId] | None = None  # field ids to return; None = every field
    offset: int = Field(default=0, ge=0)
    limit: int = Field(ge=1, le=PAGE_ROWS_MAX)
    row_order: Literal["asc", "desc"] = "desc"  # newest first, as Node's grid


class RowsCountRequest(_Body):
    manifest: ManifestDto


class CellTarget(_Body):
    """A main-visit cell (consistency §9). Follow-up visit cells are not read here yet."""

    row: RowId
    field: FieldId


class CellsReadRequest(_Body):
    manifest: ManifestDto
    targets: list[CellTarget] = Field(min_length=1, max_length=PAGE_ROWS_MAX)


class _Reply(BaseModel):
    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)


class Row(_Reply):
    rid: str = Field(alias="_rid")
    created_by: str | None
    created_at: datetime | None
    updated_at: datetime | None
    values: dict[str, CellValue]  # by field id


class RowsPage(_Reply):
    revision: int
    rows: list[Row]


class RowsCount(_Reply):
    revision: int
    count: int


class Cell(_Reply):
    target: CellTarget
    value: CellValue


class RowState(_Reply):
    rid: str = Field(alias="_rid")
    state: Literal["live", "deleted"]
    created_by: str | None


class CellsRead(_Reply):
    """The before-values of an edit: a cell for every target whose row exists for the caller,
    and the state of those rows. A target row the caller may not see is absent from both."""

    revision: int
    cells: list[Cell]
    rows: list[RowState]


class TableRowsDto(_Body):
    """A new table's rows, column by column (compact, and each column becomes one Arrow array)."""

    rid: list[RowId]
    created_by: list[str | None]
    created_at: list[datetime]
    values: dict[str, list[CellValue]]  # by field id: one value per row


class TableCreateRequest(_Body):
    """A dataset's first table — the rows of its first import."""

    fields: list[FieldDto] = Field(min_length=1)
    rows: TableRowsDto

    @model_validator(mode="after")
    def _consistent(self) -> "TableCreateRequest":
        _unique(self.fields, "fields")
        size = len(self.rows.rid)
        if len(self.rows.created_by) != size or len(self.rows.created_at) != size:
            raise ValueError("rows: every column must have one value per row")
        if set(self.rows.values) != {item.field_id for item in self.fields}:
            raise ValueError("rows.values must hold exactly the listed fields")
        if any(len(column) != size for column in self.rows.values.values()):
            raise ValueError("rows: every column must have one value per row")
        return self


class TableCreated(_Reply):
    version: int
    row_count: int
