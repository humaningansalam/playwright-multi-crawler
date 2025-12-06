from fastapi import APIRouter, Response
from prometheus_client import generate_latest

router = APIRouter(
    prefix="/metrics",
    tags=["Monitoring"]
)

@router.get("")
async def metrics_endpoint():
    """
    Exposes Prometheus metrics.
    Scraped by Prometheus server.
    """
    return Response(content=generate_latest(), media_type="text/plain")