from typing import Annotated, List

from fastapi import APIRouter, Depends, Query

from ...auth.services import OptionalAuthorizationDependency
from ...config import settings
from ...utils import RouteReturnSchema
from ..schemas.control_recursos import (
    ControlRecursosParams,
)
from ..services import ControlRecursosServiceDependency

control_recursos_router = APIRouter(prefix="/recursos")


# -------------------------------------------------
@control_recursos_router.post(
    "/sync_from_source", response_model=List[RouteReturnSchema]
)
async def sync_icaro_vs_siif_from_source(
    auth: OptionalAuthorizationDependency,
    service: ControlRecursosServiceDependency,
    params: Annotated[ControlRecursosParams, Depends()],
    siif_username: str = Query(None, alias="SIIFUsername"),
    siif_password: str = Query(None, alias="SIIFPassword"),
    sscc_username: str = Query(None, alias="SSCCUsername"),
    sscc_password: str = Query(None, alias="SSCCPassword"),
):
    if auth.is_admin:
        siif_username = settings.SIIF_USERNAME
        siif_password = settings.SIIF_PASSWORD
        sscc_username = settings.SSCC_USERNAME
        sscc_password = settings.SSCC_PASSWORD

    return await service.sync_recursos_from_source(
        siif_username=siif_username, siif_password=siif_password, params=params
    )


# -------------------------------------------------
@control_recursos_router.post("/compute", response_model=List[RouteReturnSchema])
async def compute_all(
    service: ControlRecursosServiceDependency,
    params: Annotated[ControlRecursosParams, Depends()],
):
    return await service.compute_all(params=params)


# -------------------------------------------------
@control_recursos_router.get(
    "/export",
    summary="Descarga todos los controles como archivo .xlsx y exporta a Google Sheets",
    response_description="Archivo Excel con los registros solicitados",
)
async def export_all_from_db(
    service: ControlRecursosServiceDependency,
    upload_to_google_sheets: bool = Query(True, alias="uploadToGoogleSheets"),
):
    return await service.export_all_from_db(
        upload_to_google_sheets=upload_to_google_sheets
    )
