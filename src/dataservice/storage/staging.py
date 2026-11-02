"""A job's staged input: what Node wrote somewhere the writer can read, named in the operation's
payload instead of carried in it (a Mongo document holds at most 16 MB; an import's rows don't fit).

`s3://bucket/key` in every deployed environment (Node's import bucket); a local path in tests.
The content is JSON, gzip-compressed or not.
"""

import gzip
import json
from collections.abc import Mapping
from pathlib import Path
from urllib.parse import urlparse

from dataservice.storage.reader.engine import read_s3_object

_GZIP_MAGIC = b"\x1f\x8b"


def read_staged_bytes(uri: str, storage_options: Mapping[str, str] | None = None) -> bytes:
    parsed = urlparse(uri)
    if parsed.scheme == "s3":
        return read_s3_object(uri, storage_options)
    if parsed.scheme in ("", "file") or len(parsed.scheme) == 1:  # a Windows drive letter is a scheme
        return Path(parsed.path if parsed.scheme == "file" else uri).read_bytes()
    raise ValueError(f"unsupported staged input: {uri!r}")


def read_staged_json(uri: str, storage_options: Mapping[str, str] | None = None) -> object:
    raw = read_staged_bytes(uri, storage_options)
    return json.loads(gzip.decompress(raw) if raw.startswith(_GZIP_MAGIC) else raw)
