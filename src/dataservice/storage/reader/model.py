"""What a reader of a revision can ask for — plain values, no I/O."""

from dataclasses import dataclass
from typing import Literal

ROW_ID = "_rid"
DELETED_AT = "_deleted_at"
VISIT = "_follow_up"  # the visit number of a follow-up visit row (1, 2, …)
# Who created a row and when, and when it last changed — Node's userId / createdAt / updatedAt.
CREATED_BY = "_created_by"
CREATED_AT = "_created_at"
UPDATED_AT = "_updated_at"
META = (CREATED_BY, CREATED_AT, UPDATED_AT)
SCAN_BATCH_ROWS = 65_536

RowOrder = Literal["asc", "desc"]
FilterOp = Literal["eq", "ne", "lt", "le", "gt", "ge", "in", "is_null", "not_null", "contains"]


@dataclass(frozen=True)
class Filter:
    """One condition on a field. `ne` keeps empty cells (a blank is "not X"); `contains` is a
    case-insensitive substring for text and "has this option" for multi-select."""

    field_id: str
    op: FilterOp
    value: object = None


@dataclass(frozen=True)
class Sort:
    field_id: str
    descending: bool = False  # empty cells sort last either way


@dataclass(frozen=True)
class Query:
    fields: tuple[str, ...] | None = None  # field ids to return; None = every field of the snapshot
    filters: tuple[Filter, ...] = ()  # all must hold
    sort: tuple[Sort, ...] = ()  # the row id breaks ties, so a page is always the same rows
    limit: int | None = None
    offset: int = 0
    include_deleted: bool = False  # also trashed rows and `_deleted_at` (compaction, trash view)
    meta: bool = False  # also `_created_by`, `_created_at` and `_updated_at` (the grid's row shape)
    # The row-id order: ids are ObjectIds, so "desc" is newest first — Node's createdAt desc, _id desc.
    row_order: RowOrder = "asc"
    rids: tuple[str, ...] | None = None  # only these rows (an edit's before-values); None = all


EVERY_ROW = Query()  # every live row with every field, by row id
