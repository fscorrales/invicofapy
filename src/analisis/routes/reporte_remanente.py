from typing import Annotated, List

from fastapi import APIRouter, Depends, Query

from ...auth.services import OptionalAuthorizationDependency
from ...config import settings
from ...utils import RouteReturnSchema
from ..schemas.reporte_remanente import (
    ReporteRemanenteParams,
    ReporteRemanenteSyncParams,
)
from ..services.reporte_remanente import (
    ReporteRemanenteServiceDependency,
)

reporte_remanente_router = APIRouter(prefix="/remanente")


# -------------------------------------------------
@reporte_remanente_router.post(
    "/sync_from_source", response_model=List[RouteReturnSchema]
)
async def sync_remanente_from_source(
    auth: OptionalAuthorizationDependency,
    service: ReporteRemanenteServiceDependency,
    params: Annotated[ReporteRemanenteSyncParams, Depends()],
):
    if auth.is_admin:
        params.siif_username = settings.SIIF_USERNAME
        params.siif_password = settings.SIIF_PASSWORD

    return await service.sync_remanente_from_source(params=params)


# -------------------------------------------------
@reporte_remanente_router.get(
    "/export",
    summary="Descarga todos los reportes como archivo .xlsx y exporta a Google Sheets",
    response_description="Archivo Excel con los registros solicitados",
)
async def export_all_from_db(
    service: ReporteRemanenteServiceDependency,
    params: Annotated[ReporteRemanenteParams, Depends()],
    upload_to_google_sheets: bool = Query(True, alias="uploadToGoogleSheets"),
):
    return await service.export_all_from_db(
        upload_to_google_sheets=upload_to_google_sheets, params=params
    )
