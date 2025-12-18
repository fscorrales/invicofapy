from typing import Annotated, List

from fastapi import APIRouter, Depends, Query

from ...auth.services import OptionalAuthorizationDependency
from ...config import settings
from ...utils import RouteReturnSchema
from ..schemas.control_honorarios import (
    ControlHonorariosParams,
    ControlHonorariosSyncParams,
)
from ..services import ControlHonorariosServiceDependency

control_honorarios_router = APIRouter(prefix="/honorarios")


# -------------------------------------------------
@control_honorarios_router.post(
    "/sync_from_source", response_model=List[RouteReturnSchema]
)
async def sync_control_honorarios_from_source(
    auth: OptionalAuthorizationDependency,
    service: ControlHonorariosServiceDependency,
    params: Annotated[ControlHonorariosSyncParams, Depends()],
):
    if auth.is_admin:
        params.siif_username = settings.SIIF_USERNAME
        params.siif_password = settings.SIIF_PASSWORD
        params.sscc_username = settings.SSCC_USERNAME
        params.sscc_password = settings.SSCC_PASSWORD
        params.sgf_username = settings.SGF_USERNAME
        params.sgf_password = settings.SGF_PASSWORD

    return await service.sync_control_honorarios_from_source(params=params)


# -------------------------------------------------
@control_honorarios_router.post("/compute", response_model=List[RouteReturnSchema])
async def compute_all(
    service: ControlHonorariosServiceDependency,
    params: Annotated[ControlHonorariosParams, Depends()],
):
    return await service.compute_all(params=params)


# -------------------------------------------------
@control_honorarios_router.get(
    "/export",
    summary="Descarga todos los controles como archivo .xlsx y exporta a Google Sheets",
    response_description="Archivo Excel con los registros solicitados",
)
async def export_all_from_db(
    service: ControlHonorariosServiceDependency,
    params: Annotated[ControlHonorariosParams, Depends()],
    upload_to_google_sheets: bool = Query(True, alias="uploadToGoogleSheets"),
):
    return await service.export_all_from_db(
        upload_to_google_sheets=upload_to_google_sheets, params=params
    )
