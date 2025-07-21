import os
from typing import Annotated, List

from fastapi import APIRouter, Depends, Query

from ...auth.services import OptionalAuthorizationDependency
from ...config import settings
from ...utils import RouteReturnSchema, get_sqlite_path
from ..schemas.control_recursos import ControlRecursosParams, ControlRecursosSyncParams
from ..services import ControlRecursosServiceDependency

control_recursos_router = APIRouter(prefix="/recursos")


# -------------------------------------------------
@control_recursos_router.post(
    "/sync_from_source", response_model=List[RouteReturnSchema]
)
async def sync_icaro_vs_siif_from_source(
    auth: OptionalAuthorizationDependency,
    service: ControlRecursosServiceDependency,
    params: Annotated[ControlRecursosSyncParams, Depends()],
):
    if auth.is_admin:
        params.siif_username = settings.SIIF_USERNAME
        params.siif_password = settings.SIIF_PASSWORD
        params.sscc_username = settings.SSCC_USERNAME
        params.sscc_password = settings.SSCC_PASSWORD

    return await service.sync_recursos_from_source(params=params)


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
