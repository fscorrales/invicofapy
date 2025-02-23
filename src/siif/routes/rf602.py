from fastapi import APIRouter

from ..services import Rf602ServiceDependency

rf602_router = APIRouter(prefix="/rf602", tags=["SIIF - rf602"])


@rf602_router.post("/siif_download/")
async def siif_download(
    username: str,
    password: str,
    ejercicio: int,
    service: Rf602ServiceDependency,
):
    pass
