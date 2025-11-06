from typing import Annotated, List

from fastapi import APIRouter, Depends, Query

from ...auth.services import OptionalAuthorizationDependency
from ...config import settings
from ...utils import RouteReturnSchema
from ..schemas.reporte_libro_diario import (
    ReporteLibroDiarioParams,
    ReporteLibroDiarioSyncParams,
)
from ..services.reporte_libro_diario import (
    ReporteLibroDiarioServiceDependency,
)

reporte_libro_diario_router = APIRouter(prefix="/libro_diario")


# -------------------------------------------------
@reporte_libro_diario_router.post(
    "/sync_from_source", response_model=List[RouteReturnSchema]
)
async def sync_libro_diario_from_source(
    auth: OptionalAuthorizationDependency,
    service: ReporteLibroDiarioServiceDependency,
    params: Annotated[ReporteLibroDiarioSyncParams, Depends()],
):
    if auth.is_admin:
        params.siif_username = settings.SIIF_USERNAME
        params.siif_password = settings.SIIF_PASSWORD

    return await service.sync_libro_diario_from_source(
        params=params
    )


# -------------------------------------------------
@reporte_libro_diario_router.get(
    "/export_libro_diario",
    summary="Descarga el Libro Diario como archivo .xlsx y exporta a Google Sheets",
    response_description="Archivo Excel con los registros solicitados",
)
async def export_planillometro_contabilidad(
    service: ReporteLibroDiarioServiceDependency,
    params: Annotated[ReporteLibroDiarioParams, Depends()],
    upload_to_google_sheets: bool = Query(True, alias="uploadToGoogleSheets"),
):
    return await service.export_libro_diario_from_db(
        upload_to_google_sheets=upload_to_google_sheets, params=params
    )
