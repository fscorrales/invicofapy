from typing import Annotated, List

from fastapi import APIRouter, Depends, Query

from ...auth.services import OptionalAuthorizationDependency
from ...config import settings
from ...utils import RouteReturnSchema
from ..schemas.reporte_ejecucion_gastos import (
    ReporteEjecucionGastosParams,
    ReporteEjecucionGastosSyncParams,
)
from ..services.reporte_ejecucion_gastos import (
    ReporteEjecucionGastosServiceDependency,
)

reporte_ejecucion_gastos_router = APIRouter(prefix="/ejecucion_gastos")


# -------------------------------------------------
@reporte_ejecucion_gastos_router.post(
    "/sync_from_source", response_model=List[RouteReturnSchema]
)
async def sync_ejecucion_gastos_from_source(
    auth: OptionalAuthorizationDependency,
    service: ReporteEjecucionGastosServiceDependency,
    params: Annotated[ReporteEjecucionGastosSyncParams, Depends()],
):
    if auth.is_admin:
        params.siif_username = settings.SIIF_USERNAME
        params.siif_password = settings.SIIF_PASSWORD

    return await service.sync_ejecucion_gastos_from_source(params=params)


# -------------------------------------------------
@reporte_ejecucion_gastos_router.get(
    "/export",
    summary="Descarga todos los reportes como archivo .xlsx y exporta a Google Sheets",
    response_description="Archivo Excel con los registros solicitados",
)
async def export_all_from_db(
    service: ReporteEjecucionGastosServiceDependency,
    params: Annotated[ReporteEjecucionGastosParams, Depends()],
    upload_to_google_sheets: bool = Query(True, alias="uploadToGoogleSheets"),
):
    return await service.export_all_from_db(
        upload_to_google_sheets=upload_to_google_sheets, params=params
    )
