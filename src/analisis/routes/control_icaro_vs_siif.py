from typing import Annotated, List

from fastapi import APIRouter, Depends, Query

from ...auth.services import OptionalAuthorizationDependency
from ...config import settings
from ...utils import RouteReturnSchema
from ..schemas.control_icaro_vs_siif import (
    ControlCompletoParams,
    ControlCompletoSyncParams,
)
from ..services import ControlIcaroVsSIIFServiceDependency

control_icaro_vs_siif_router = APIRouter(prefix="/icaro_vs_siif")


# -------------------------------------------------
@control_icaro_vs_siif_router.post(
    "/sync_from_source", response_model=List[RouteReturnSchema]
)
async def sync_icaro_vs_siif_from_source(
    auth: OptionalAuthorizationDependency,
    service: ControlIcaroVsSIIFServiceDependency,
    params: Annotated[ControlCompletoSyncParams, Depends()],
):
    if auth.is_admin:
        params.siif_username = settings.SIIF_USERNAME
        params.siif_password = settings.SIIF_PASSWORD

    return await service.sync_icaro_vs_siif_from_source(params=params)


# -------------------------------------------------
@control_icaro_vs_siif_router.post("/compute", response_model=List[RouteReturnSchema])
async def compute_all(
    service: ControlIcaroVsSIIFServiceDependency,
    params: Annotated[ControlCompletoParams, Depends()],
):
    return await service.compute_all(params=params)


# -------------------------------------------------
@control_icaro_vs_siif_router.get(
    "/export",
    summary="Descarga todos los controles como archivo .xlsx y exporta a Google Sheets",
    response_description="Archivo Excel con los registros solicitados",
)
async def export_all_from_db(
    service: ControlIcaroVsSIIFServiceDependency,
    params: Annotated[ControlCompletoParams, Depends()],
    upload_to_google_sheets: bool = Query(True, alias="uploadToGoogleSheets"),
):
    return await service.export_all_from_db(
        upload_to_google_sheets=upload_to_google_sheets, params=params
    )
