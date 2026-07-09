from fastapi import APIRouter, Response
from fastapi.responses import PlainTextResponse
from prometheus_client import generate_latest

router = APIRouter(
    prefix="/metrics",
    tags=["Monitoring"]
)

@router.get(
    "",
    response_class=PlainTextResponse,
    responses={
        200: {
            "description": "Prometheus metrics payload",
            "content": {
                "text/plain": {
                    "schema": {"type": "string"}
                }
            },
        }
    },
)
async def metrics_endpoint():
    """
    Exposes Prometheus metrics.
    Scraped by Prometheus server.
    """
    return Response(content=generate_latest(), media_type="text/plain")
