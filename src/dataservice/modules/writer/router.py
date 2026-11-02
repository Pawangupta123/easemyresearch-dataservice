"""`POST /v1/datasets/{datasetId}/writer:drain` — Node wakes the writer of one dataset (its
dataset-drain queue). Answers at once; the drain runs in the background under the lease."""

from functools import lru_cache
from typing import Annotated

from fastapi import APIRouter, Depends, Path, status
from pydantic import BaseModel

from dataservice.common.auth import require_dataset_token
from dataservice.common.responses import ApiResponse, ok
from dataservice.common.utils.dataset_token import DatasetClaims
from dataservice.config import Settings, get_settings
from dataservice.modules.writer.node_authority import NodeAuthority
from dataservice.modules.writer.runner import DrainRunner

router = APIRouter(prefix="/v1/datasets", tags=["writer"])

DatasetId = Annotated[str, Path(pattern=r"^[0-9a-f]{24}$")]
JobClaims = Annotated[DatasetClaims, Depends(require_dataset_token("job"))]


class DrainAccepted(BaseModel):
    # False: a drain of this dataset is running already; it drains once more when it ends.
    started: bool


@lru_cache
def _runner(url: str, secret: str, timeout: float, lake_root: str, renew_seconds: float) -> DrainRunner:
    """One runner per process: it is what keeps a dataset to one drain at a time."""
    return DrainRunner(NodeAuthority(url, secret, timeout), lake_root, renew_seconds)


def drain_runner(settings: Annotated[Settings, Depends(get_settings)]) -> DrainRunner:
    url, secret, lake_root = settings.node_internal_url, settings.node_callback_secret, settings.lake_root
    if not url or not secret or not lake_root:
        raise RuntimeError("NODE_INTERNAL_URL, NODE_CALLBACK_SECRET and LAKE_ROOT must be set to run the writer")
    return _runner(url, secret, settings.node_timeout_seconds, lake_root, settings.writer_lease_renew_seconds)


@router.post("/{dataset_id}/writer:drain", status_code=status.HTTP_202_ACCEPTED)
def drain_dataset(
    dataset_id: DatasetId, _claims: JobClaims, runner: Annotated[DrainRunner, Depends(drain_runner)]
) -> ApiResponse[DrainAccepted]:
    return ok(DrainAccepted(started=runner.request(dataset_id)), "Drain requested")
