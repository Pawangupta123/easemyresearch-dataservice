"""`POST /v1/datasets/{datasetId}/upload:detect` — the preview of an upload for the dataset Node
reserved for it (job token). Plain `def`: reading and parsing a workbook blocks, so FastAPI runs
it on its thread pool."""

from typing import Annotated

from fastapi import APIRouter, Depends, Path

from dataservice.common.auth import require_dataset_token
from dataservice.common.responses import ApiResponse, ok
from dataservice.common.utils.dataset_token import DatasetClaims
from dataservice.config import Settings, get_settings
from dataservice.modules.ingest import service
from dataservice.modules.ingest.schemas import UploadDetected, UploadDetectRequest

router = APIRouter(prefix="/v1/datasets", tags=["ingest"])

DatasetId = Annotated[str, Path(pattern=r"^[0-9a-f]{24}$")]
JobClaims = Annotated[DatasetClaims, Depends(require_dataset_token("job"))]


def import_bucket(settings: Annotated[Settings, Depends(get_settings)]) -> str:
    if not settings.import_bucket:
        raise RuntimeError("IMPORT_BUCKET is not set: uploads can't be read")
    return settings.import_bucket


@router.post("/{dataset_id}/upload:detect")
def detect_upload(
    dataset_id: DatasetId,
    body: UploadDetectRequest,
    _claims: JobClaims,
    bucket: Annotated[str, Depends(import_bucket)],
    settings: Annotated[Settings, Depends(get_settings)],
) -> ApiResponse[UploadDetected]:
    del dataset_id  # the token is checked against it; the upload is named by `source`
    return ok(service.detect_from_source(body.source, bucket, settings.import_max_bytes))
