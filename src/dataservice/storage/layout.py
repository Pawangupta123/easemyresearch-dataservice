"""Where a dataset's Delta tables live (data-model §1): `<lake_root>/datasets/<datasetId>/<folder>`.

The caller never names a path: a table's URI comes from the lake root, the dataset id and the
manifest's table name, so a request can only reach this dataset's own tables.
"""

import re

_DATASET_ID = re.compile(r"[0-9a-f]{24}")

# The manifest's table names → their folders under the dataset.
BASE = "base"
FOLLOWUPS = "followups"
TABLE_FOLDERS = {BASE: "table", FOLLOWUPS: "followups"}


def table_uri(lake_root: str, dataset_id: str, table: str) -> str:
    """`<lake_root>/datasets/<datasetId>/<table>` — only for a well-formed dataset id."""
    if not _DATASET_ID.fullmatch(dataset_id):
        raise ValueError(f"not a dataset id: {dataset_id!r}")
    return f"{lake_root.rstrip('/')}/datasets/{dataset_id}/{table}"


def dataset_table_uris(lake_root: str, dataset_id: str, names: tuple[str, ...] = (BASE,)) -> dict[str, str]:
    """The URIs of a dataset's tables, by manifest name."""
    return {name: table_uri(lake_root, dataset_id, TABLE_FOLDERS[name]) for name in names}
