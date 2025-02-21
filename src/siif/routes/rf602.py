from fastapi import APIRouter

rf602_router = APIRouter(prefix="/rf602", tags=["SIIF - rf602"])


@rf602_router.get("/")
async def download_report():
    return "rf602"
