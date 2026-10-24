"""What one published revision is, as a reader needs it (consistency.md §2, hybrid edits).

A revision = the Delta tables at their manifest versions + the Mongo patches with
`from_seq < seq ≤ to_seq`. Every reader reads both, together, through storage/reader.py.
"""

import re
from dataclasses import dataclass, field
from typing import Literal, Protocol

import pyarrow as pa

ValueType = Literal["str", "num", "date", "opts"]
PatchOp = Literal["set", "insert_row", "delete_row", "restore_row"]

# Field ids become column names in generated SQL — only plain identifiers are accepted.
FIELD_ID = re.compile(r"^[A-Za-z0-9_]{1,64}$")

VALUE_COLUMN: dict[ValueType, str] = {"str": "v_str", "num": "v_num", "date": "v_date", "opts": "v_opts"}

# Every instant is UTC: `_deleted_at` in the Delta tables and a patch's `at`.
UTC_TIMESTAMP = pa.timestamp("us", tz="UTC")

# The shape a PatchSource returns (main-scope patches; table / follow-up scopes come later).
PATCH_SCHEMA = pa.schema(
    [
        ("seq", pa.int64()),
        ("op", pa.string()),
        ("row", pa.string()),
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
        if not FIELD_ID.match(self.field_id):
            raise ValueError(f"invalid field id: {self.field_id!r}")

    @property
    def column(self) -> str:
        return f"f_{self.field_id}"


@dataclass(frozen=True)
class Snapshot:
    dataset_id: str
    base_uri: str
    base_version: int
    patch_from: int  # exclusive — patches up to here are already folded into base
    patch_to: int  # inclusive
    fields: tuple[FieldSpec, ...]
    storage_options: dict[str, str] = field(default_factory=dict)


class PatchSource(Protocol):
    def patches(self, dataset_id: str, from_seq: int, to_seq: int) -> pa.Table:
        """Patches with from_seq < seq ≤ to_seq, in PATCH_SCHEMA."""
        ...
