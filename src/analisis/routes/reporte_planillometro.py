from typing import Annotated, List

from fastapi import APIRouter, Depends, Query

from ...auth.services import OptionalAuthorizationDependency
from ...config import settings
from ...utils import RouteReturnSchema
from ..schemas.reporte_planillometro import (
    ReportePlanillometroParams,
    ReportePlanillometroSyncParams,
)
from ..services.reporte_planillometro import (
    ReportePlanillometroServiceDependency,
)

reporte_planillometro_router = APIRouter(prefix="/planillometro")


# -------------------------------------------------
@reporte_planillometro_router.post(
    "/sync_from_source", response_model=List[RouteReturnSchema]
)
async def sync_planillometro_from_source(
    auth: OptionalAuthorizationDependency,
    service: ReportePlanillometroServiceDependency,
    params: Annotated[ReportePlanillometroSyncParams, Depends()],
):
    if auth.is_admin:
        params.siif_username = settings.SIIF_USERNAME
        params.siif_password = settings.SIIF_PASSWORD
        params.sgv_username = settings.SGV_USERNAME
        params.sgv_password = settings.SGV_PASSWORD

    return await service.sync_planillometro_from_source(params=params)


# -------------------------------------------------
@reporte_planillometro_router.get(
    "/export",
    summary="Descarga todos los reportes como archivo .xlsx y exporta a Google Sheets",
    response_description="Archivo Excel con los registros solicitados",
)
async def export_all_from_db(
    service: ReportePlanillometroServiceDependency,
    params: Annotated[ReportePlanillometroParams, Depends()],
    upload_to_google_sheets: bool = Query(True, alias="uploadToGoogleSheets"),
):
    return await service.export_all_from_db(
        upload_to_google_sheets=upload_to_google_sheets, params=params
    )
