from typing import Annotated, List

from fastapi import APIRouter, Depends

from ...auth.services import OptionalAuthorizationDependency
from ...config import settings
from ...utils import RouteReturnSchema, apply_auto_filter
from ..schemas.icaro_vs_siif import ControlEjecucionAnualDocument, ControlEjecucionAnualFilter, ControlEjecucionAnualParams
from ..services import IcaroVsSIIFServiceDependency

icaro_vs_siif_router = APIRouter(prefix="/icaro_vs_siif")


# @rf602_router.post("/sync_from_siif", response_model=RouteReturnSchema)
# async def sync_rf602_from_siif(
#     auth: OptionalAuthorizationDependency,
#     service: Rf602ServiceDependency,
#     params: Annotated[Rf602Params, Depends()],
#     username: str = None,
#     password: str = None,
# ):
#     if auth.is_admin:
#         username = settings.SIIF_USERNAME
#         password = settings.SIIF_PASSWORD

#     return await service.sync_rf602_from_siif(
#         username=username, password=password, params=params
#     )


@icaro_vs_siif_router.get("/control_anual", response_model=List[ControlEjecucionAnualDocument])
async def control_ejecucion_anual(
    service: IcaroVsSIIFServiceDependency,
    params: Annotated[ControlEjecucionAnualFilter, Depends()],
):
    apply_auto_filter(params=params)
    return await service.control_ejecucion_anual()


# @rf602_router.post("/download_and_update/")
# async def siif_download(
#     ejercicio: str,
#     service: Rf602ServiceDependency,
# ) -> Rf602ValidationOutput:
#     return await service.download_and_update(ejercicio=ejercicio)
