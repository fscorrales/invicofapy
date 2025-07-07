from typing import Annotated, List

from fastapi import APIRouter, Depends

from ...auth.services import OptionalAuthorizationDependency
from ...config import settings
from ...utils import RouteReturnSchema, apply_auto_filter
from ..schemas.icaro_vs_siif import (
    ControlAnualDocument,
    ControlAnualFilter,
    ControlCompletoParams,
    ControlComprobantesDocument,
    ControlComprobantesFilter,
)
from ..services import IcaroVsSIIFServiceDependency

icaro_vs_siif_router = APIRouter(prefix="/icaro_vs_siif")


# -------------------------------------------------
@icaro_vs_siif_router.post("/sync_from_source", response_model=List[RouteReturnSchema])
async def sync_icaro_vs_siif_from_source(
    auth: OptionalAuthorizationDependency,
    service: IcaroVsSIIFServiceDependency,
    params: Annotated[ControlCompletoParams, Depends()],
    username: str = None,
    password: str = None,
):
    if auth.is_admin:
        username = settings.SIIF_USERNAME
        password = settings.SIIF_PASSWORD

    return await service.sync_icaro_vs_siif_from_source(
        username=username, password=password, params=params
    )


# -------------------------------------------------
@icaro_vs_siif_router.post("/compute", response_model=RouteReturnSchema)
async def compute_all(
    service: IcaroVsSIIFServiceDependency,
    params: Annotated[ControlCompletoParams, Depends()],
):
    return await service.compute_all(params=params)


# -------------------------------------------------
@icaro_vs_siif_router.post("/control_anual/compute", response_model=RouteReturnSchema)
async def compute_control_anual(
    service: IcaroVsSIIFServiceDependency,
    params: Annotated[ControlCompletoParams, Depends()],
):
    return await service.compute_control_anual(params=params)


# -------------------------------------------------
@icaro_vs_siif_router.get(
    "/control_anual/get_from_db", response_model=List[ControlAnualDocument]
)
async def get_control_anual_from_db(
    service: IcaroVsSIIFServiceDependency,
    params: Annotated[ControlAnualFilter, Depends()],
):
    apply_auto_filter(params=params)
    return await service.get_control_anual_from_db(params=params)


# -------------------------------------------------
@icaro_vs_siif_router.post(
    "/control_comprobantes/compute", response_model=RouteReturnSchema
)
async def compute_control_comprobantes(
    service: IcaroVsSIIFServiceDependency,
    params: Annotated[ControlCompletoParams, Depends()],
):
    return await service.compute_control_comprobantes(params=params)


# -------------------------------------------------
@icaro_vs_siif_router.get(
    "/control_comprobantes/get_from_db",
    response_model=List[ControlComprobantesDocument],
)
async def get_control_comprobantes_from_db(
    service: IcaroVsSIIFServiceDependency,
    params: Annotated[ControlComprobantesFilter, Depends()],
):
    apply_auto_filter(params=params)
    return await service.get_control_comprobantes_from_db(params=params)
