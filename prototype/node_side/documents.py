"""Small helpers for reading Mongo documents in the reference Node side (no I/O)."""

from collections.abc import Mapping


def int_at(document: Mapping[str, object], *path: str) -> int:
    """The integer at a dotted path, or a clear error naming the malformed document."""
    value: object = document
    for key in path:
        if not isinstance(value, Mapping):
            raise ValueError(f"malformed document at {'.'.join(path)}: {document}")
        value = value.get(key)
    if not isinstance(value, int):
        raise ValueError(f"malformed number at {'.'.join(path)}: {document}")
    return value
