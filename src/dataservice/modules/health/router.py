from fastapi import APIRouter

from dataservice.common.responses import ApiResponse, ok
from dataservice.modules.health.schemas import HealthData

router = APIRouter(tags=["health"])


@router.get("/health")
def health() -> ApiResponse[HealthData]:
    return ok(HealthData(status="ok"), "Service is healthy")
