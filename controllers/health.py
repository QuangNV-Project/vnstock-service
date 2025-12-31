# controllers/health.py
"""
Health check endpoints for service monitoring and Kubernetes probes.
"""

from fastapi import APIRouter
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from datetime import datetime

from vnstock import Listing

router = APIRouter(prefix="", tags=["Health"])


class HealthResponse(BaseModel):
    status: str
    service: str
    timestamp: str


@router.get("/health", response_model=HealthResponse)
async def health_check():
    """Health check endpoint cho service discovery."""
    return HealthResponse(
        status="healthy",
        service="vnstock-api",
        timestamp=datetime.now().isoformat()
    )


@router.get("/ready")
async def readiness_check():
    """Readiness probe cho Kubernetes."""
    try:
        # Try a simple operation to verify service is ready
        listing = Listing(source="vci")
        _ = listing.all_symbols()
        return {"ready": True, "timestamp": datetime.now().isoformat()}
    except Exception as e:
        return JSONResponse(
            status_code=503,
            content={"ready": False, "error": str(e)}
        )

