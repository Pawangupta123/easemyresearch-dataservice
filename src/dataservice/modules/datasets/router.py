"""`/v1/datasets/{datasetId}/…` — the query service (api.md §2.1). Each route takes Node's scoped
token for this dataset (common/auth.py). The handlers are plain `def`: DuckDB work blocks, so
FastAPI runs them on its thread pool and the event loop stays free."""

from functools import lru_cache
from typing import Annotated

from fastapi import APIRouter, Depends, Path
from pymongo import MongoClient

from dataservice.common.auth import require_dataset_token
from dataservice.common.responses import ApiResponse, ok
from dataservice.common.utils.dataset_token import DatasetClaims
from dataservice.config import Settings, get_settings
from dataservice.modules.datasets import service
from dataservice.modules.datasets.schemas import (
    OBJECT_ID,
    CellsRead,
    CellsReadRequest,
    RowsCount,
    RowsCountRequest,
    RowsPage,
    RowsQueryRequest,
    TableCreated,
    TableCreateRequest,
)
from dataservice.modules.datasets.table_create import create_table
from dataservice.storage.mongo_patches import Document, MongoPatchSource
from dataservice.storage.snapshot import PatchSource

router = APIRouter(prefix="/v1/datasets", tags=["datasets"])

DatasetId = Annotated[str, Path(pattern=OBJECT_ID)]
ReadClaims = Annotated[DatasetClaims, Depends(require_dataset_token("read"))]
JobClaims = Annotated[DatasetClaims, Depends(require_dataset_token("job"))]


@lru_cache
def _mongo(url: str) -> MongoClient[Document]:
    """One client per process (it pools its own connections); dates come back as UTC instants."""
    return MongoClient(url, tz_aware=True)


def lake_root(settings: Annotated[Settings, Depends(get_settings)]) -> str:
    if not settings.lake_root:
        raise RuntimeError("LAKE_ROOT is not set: the dataset routes can't read any table")
    return settings.lake_root


def patch_source(settings: Annotated[Settings, Depends(get_settings)]) -> PatchSource:
    if not settings.mongodb_url or not settings.mongodb_db:
        raise RuntimeError("MONGODB_URL and MONGODB_DB are not set: the dataset routes can't read patches")
    return MongoPatchSource(_mongo(settings.mongodb_url)[settings.mongodb_db])


LakeRoot = Annotated[str, Depends(lake_root)]
Patches = Annotated[PatchSource, Depends(patch_source)]


@router.post("/{dataset_id}/rows:query")
def query_rows(
    dataset_id: DatasetId, body: RowsQueryRequest, claims: ReadClaims, root: LakeRoot, patches: Patches
) -> ApiResponse[RowsPage]:
    return ok(service.query_rows(root, dataset_id, body, claims, patches))


@router.post("/{dataset_id}/rows:count")
def count_rows(
    dataset_id: DatasetId, body: RowsCountRequest, claims: ReadClaims, root: LakeRoot, patches: Patches
) -> ApiResponse[RowsCount]:
    return ok(service.count_rows(root, dataset_id, body.manifest, claims, patches))


@router.post("/{dataset_id}/cells:read")
def read_cells(
    dataset_id: DatasetId, body: CellsReadRequest, claims: ReadClaims, root: LakeRoot, patches: Patches
) -> ApiResponse[CellsRead]:
    return ok(service.read_cells(root, dataset_id, body, claims, patches))


@router.post("/{dataset_id}/table:create")
def create_dataset_table(
    dataset_id: DatasetId,
    body: TableCreateRequest,
    claims: JobClaims,
    root: LakeRoot,
    settings: Annotated[Settings, Depends(get_settings)],
) -> ApiResponse[TableCreated]:
    return ok(create_table(root, dataset_id, body, settings.delta_retention_days))
