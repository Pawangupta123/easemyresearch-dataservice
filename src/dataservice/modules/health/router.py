from fastapi import APIRouter

from dataservice.modules.health.schemas import HealthResponse

router = APIRouter(tags=["health"])


@router.get("/health")
def health() -> HealthResponse:
    return HealthResponse(status="ok")
