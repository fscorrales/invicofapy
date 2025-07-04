from typing import Annotated, List

from fastapi import APIRouter, Depends

from ...auth.services import OptionalAuthorizationDependency
from ...config import settings
from ...utils import RouteReturnSchema, apply_auto_filter
from ..schemas.icaro_vs_siif import (
    ControlAnualDocument,
    ControlAnualFilter,
    ControlAnualParams,
)
from ..services import IcaroVsSIIFServiceDependency

icaro_vs_siif_router = APIRouter(prefix="/icaro_vs_siif")


@icaro_vs_siif_router.post("/compute", response_model=RouteReturnSchema)
async def compute_control_anual(
    service: IcaroVsSIIFServiceDependency,
    params: Annotated[ControlAnualParams, Depends()],
):
    return await service.compute_control_anual(
        params=params
    )


@icaro_vs_siif_router.get("/get_from_db", response_model=List[ControlAnualDocument])
async def get_control_anual_from_db(
    service: IcaroVsSIIFServiceDependency,
    params: Annotated[ControlAnualFilter, Depends()],
):
    apply_auto_filter(params=params)
    return await service.get_control_anual_from_db(params=params)


# @rf602_router.post("/download_and_update/")
# async def siif_download(
#     ejercicio: str,
#     service: Rf602ServiceDependency,
# ) -> Rf602ValidationOutput:
#     return await service.download_and_update(ejercicio=ejercicio)
