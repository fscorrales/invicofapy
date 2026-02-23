from typing import Annotated, List

from fastapi import APIRouter, Depends, Query

from ...auth.services import OptionalAuthorizationDependency
from ...config import settings
from ...utils import RouteReturnSchema
from ..schemas.reporte_listado_obras import (
    ReporteListadoObrasParams,
    ReporteListadoObrasSyncParams,
)
from ..services.reporte_listado_obras import (
    ReporteListadoObrasServiceDependency,
)

reporte_listado_obras_router = APIRouter(prefix="/listado_obras")


# -------------------------------------------------
@reporte_listado_obras_router.post(
    "/sync_from_source", response_model=List[RouteReturnSchema]
)
async def sync_listado_obras_from_source(
    auth: OptionalAuthorizationDependency,
    service: ReporteListadoObrasServiceDependency,
    params: Annotated[ReporteListadoObrasSyncParams, Depends()],
):
    if auth.is_admin:
        params.siif_username = settings.SIIF_USERNAME
        params.siif_password = settings.SIIF_PASSWORD

    return await service.sync_listado_obras_from_source(params=params)


# -------------------------------------------------
@reporte_listado_obras_router.get(
    "/export",
    summary="Descarga todos los reportes como archivo .xlsx y exporta a Google Sheets",
    response_description="Archivo Excel con los registros solicitados",
)
async def export_all_from_db(
    service: ReporteListadoObrasServiceDependency,
    params: Annotated[ReporteListadoObrasParams, Depends()],
    upload_to_google_sheets: bool = Query(True, alias="uploadToGoogleSheets"),
):
    return await service.export_all_from_db(
        upload_to_google_sheets=upload_to_google_sheets, params=params
    )
