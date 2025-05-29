from fastapi import APIRouter, Depends
from typing import List, Annotated

from ..schemas import Rf602Filter, Rf602Document, EjercicioSIIF
from ..services import Rf602ServiceDependency
from ...utils import apply_auto_filter, RouteReturnSchema
from ...auth.services import OptionalAuthorizationDependency

rf602_router = APIRouter(prefix="/rf602", tags=["SIIF - rf602"])


@rf602_router.post("/sync_from_siif", response_model=RouteReturnSchema)
async def sync_rf602_from_siif(
    auth: OptionalAuthorizationDependency,
    service: Rf602ServiceDependency,
    ejercicio: Annotated[EjercicioSIIF, Depends()],
    username: str = None,
    password: str = None,
):
    if auth.is_admin:
        username = settings.SIIF_USERNAME
        password = settings.SIIF_PASSWORD

    return await service.sync_rf602_from_siif(username=username, password=password)


@rf602_router.get(
    "/get_from_db", response_model=List[Rf602Document]
)
async def get_rf602_from_db(
    service: Rf602ServiceDependency,
    params: Annotated[Rf602Filter, Depends()],
):
    apply_auto_filter(params=params)
    return await service.get_rf602_from_db(params=params)

# @rf602_router.post("/download_and_update/")
# async def siif_download(
#     ejercicio: str,
#     service: Rf602ServiceDependency,
# ) -> Rf602ValidationOutput:
#     return await service.download_and_update(ejercicio=ejercicio)