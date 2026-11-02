"""The upload routes' bodies — the preview in exactly the shape Node stores and its frontend shows
(Node's ImportPreviewField, camelCase), and the samples its AI type refinement classifies."""

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field
from pydantic.alias_generators import to_camel


class _Body(BaseModel):
    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True, extra="forbid", frozen=True)


class UploadDetectRequest(_Body):
    # The upload Node's presigned URL wrote: s3://<import bucket>/imports/<userId>/… (or a local
    # path in tests).
    source: str = Field(min_length=1, max_length=1024)


class Occurrence(_Body):
    row: int
    column_index: int
    cell: str


class Correction(_Body):
    value: str
    suggested: list[str]
    confidence: Literal["HIGH", "MEDIUM", "LOW"]
    occurrences: list[Occurrence]


class Notice(_Body):
    kind: Literal["long_options", "multi_value_numbers"]
    message: str
    samples: list[str]


class TypeMetrics(_Body):
    total_count: int
    non_empty_count: int
    unique_count: int | None
    unique_capped: bool
    convertible_count: int
    ambiguous_count: int
    invalid_count: int
    decimal_count: int
    date_count: int
    date_parseable_count: int
    date_ambiguous_count: int
    date_format: Literal["DMY", "MDY"] | None
    date_format_conflict: bool
    candidate_options: list[str]


class PreviewField(_Body):
    temp_field_id: str
    field_name: str
    column_index: int
    detected_type: Literal["text", "number", "date", "radio"]
    options: list[str]
    corrections: list[Correction]
    notices: list[Notice] = Field(default_factory=list)  # Node stores the key only when non-empty
    type_metrics: TypeMetrics


class AiSample(_Body):
    temp_field_id: str
    header: str
    rule_type: str
    samples: list[str]


class HeaderCheck(_Body):
    ok: bool
    headers: list[str] | None = None
    message: str | None = None


class UploadDetected(_Body):
    """A refused header row comes back with `headers.ok` false and no fields (Node fails the job
    with its message, as it does today)."""

    headers: HeaderCheck
    fields: list[PreviewField]
    samples: list[AiSample]
