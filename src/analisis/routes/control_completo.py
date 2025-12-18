from typing import Annotated, List

from fastapi import APIRouter, Depends, Query

from ...auth.services import OptionalAuthorizationDependency
from ...config import settings
from ...utils import GoogleExportResponse, RouteReturnSchema
from ..schemas.control_completo import (
    ControlCompletoParams,
    ControlCompletoSyncParams,
)
from ..services.control_completo import (
    ControlCompletoServiceDependency,
)

control_completo_router = APIRouter(prefix="/completo")


# -------------------------------------------------
@control_completo_router.post(
    "/sync_from_source", response_model=List[RouteReturnSchema]
)
async def sync_control_completo_from_source(
    auth: OptionalAuthorizationDependency,
    service: ControlCompletoServiceDependency,
    params: Annotated[ControlCompletoSyncParams, Depends()],
):
    if auth.is_admin:
        params.siif_username = settings.SIIF_USERNAME
        params.siif_password = settings.SIIF_PASSWORD
        params.sscc_username = settings.SSCC_USERNAME
        params.sscc_password = settings.SSCC_PASSWORD
        params.sgf_username = settings.SGF_USERNAME
        params.sgf_password = settings.SGF_PASSWORD

    return await service.sync_control_completo_from_source(params=params)


# -------------------------------------------------
@control_completo_router.post("/compute", response_model=List[RouteReturnSchema])
async def compute_all(
    service: ControlCompletoServiceDependency,
    params: Annotated[ControlCompletoParams, Depends()],
):
    return await service.compute_all(params=params)


# -------------------------------------------------
@control_completo_router.get(
    "/export",
    summary="Exporta todos los controles a Google Sheets",
    response_model=List[GoogleExportResponse],
)
async def export_all_from_db(
    service: ControlCompletoServiceDependency,
    params: Annotated[ControlCompletoParams, Depends()],
):
    return await service.export_all_from_db_to_google(params=params)
