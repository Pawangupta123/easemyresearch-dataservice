"""What one published revision is, as a reader needs it (consistency.md §2, hybrid edits).

A revision = the Delta tables at their manifest versions + the Mongo patches with
`from_seq < seq ≤ to_seq`. Every reader reads both, together, through storage/reader/.

Two tables can hold a row's cells: `base` (the main visit) and `followups` (one row per
follow-up visit, keyed `(_rid, _follow_up)`, created by the visit's submit — as Node's records
do today). Table fields (mini tables) and screening are legacy-only (PRD D14): new forms have
neither, so there is no table-field scope.
"""

import re
from dataclasses import dataclass, field
from typing import Literal, Protocol

import pyarrow as pa

ValueType = Literal["str", "num", "date", "opts"]
PatchOp = Literal["set", "insert_row", "delete_row", "restore_row"]

# Field ids become column names in generated SQL — only plain identifiers are accepted.
FIELD_ID = re.compile(r"[A-Za-z0-9_]{1,64}")  # whole id: always fullmatch

VALUE_COLUMN: dict[ValueType, str] = {"str": "v_str", "num": "v_num", "date": "v_date", "opts": "v_opts"}

# Every instant is UTC: `_deleted_at` in the Delta tables and a patch's `at`.
UTC_TIMESTAMP = pa.timestamp("us", tz="UTC")

MAIN_VISIT = 0  # `follow_up` of a main-visit cell; follow-up visits are 1, 2, …

# The shape a PatchSource returns.
PATCH_SCHEMA = pa.schema(
    [
        ("seq", pa.int64()),
        ("op", pa.string()),
        ("row", pa.string()),
        ("follow_up", pa.int32()),  # 0 = the main visit (base), N = follow-up visit N (followups)
        ("field", pa.string()),  # null for row ops
        ("t", pa.string()),  # value type, or "null" for a cleared cell; null for row ops
        ("v_str", pa.string()),
        ("v_num", pa.float64()),
        ("v_date", pa.date32()),
        ("v_opts", pa.list_(pa.string())),
        ("at", UTC_TIMESTAMP),  # when Node wrote it — a delete's time feeds the trash window
    ]
)


@dataclass(frozen=True)
class FieldSpec:
    field_id: str
    value_type: ValueType

    def __post_init__(self) -> None:
        if not FIELD_ID.fullmatch(self.field_id):
            raise ValueError(f"invalid field id: {self.field_id!r}")

    @property
    def column(self) -> str:
        return f"f_{self.field_id}"


@dataclass(frozen=True)
class TableVersion:
    """One Delta table of a revision at its manifest version."""

    uri: str
    version: int


@dataclass(frozen=True)
class Snapshot:
    dataset_id: str
    base_uri: str
    base_version: int
    patch_from: int  # exclusive — patches up to here are already folded into base
    patch_to: int  # inclusive
    fields: tuple[FieldSpec, ...]
    storage_options: dict[str, str] = field(default_factory=dict)
    followups: TableVersion | None = None  # None: the dataset has no follow-up visit yet
    visit_fields: tuple[FieldSpec, ...] = ()  # the fields its follow-up visits capture


class PatchSource(Protocol):
    def patches(self, dataset_id: str, from_seq: int, to_seq: int) -> pa.Table:
        """Patches with from_seq < seq ≤ to_seq, in PATCH_SCHEMA."""
        ...
