from typing import Annotated, List

from fastapi import APIRouter, Depends, Query

from ...auth.services import OptionalAuthorizationDependency
from ...config import settings
from ...utils import RouteReturnSchema
from ..schemas.reporte_formulacion_presupuesto import (
    ReporteFormulacionPresupuestoParams,
    ReporteFormulacionPresupuestoSyncParams,
)
from ..services.reporte_formulacion_presupuesto import (
    ReporteFormulacionPresupuestoServiceDependency,
)

reporte_formulacion_presupuesto_router = APIRouter(prefix="/formulacion_presupuesto")


# -------------------------------------------------
@reporte_formulacion_presupuesto_router.post(
    "/sync_from_source", response_model=List[RouteReturnSchema]
)
async def sync_formulacion_presupuesto_from_source(
    auth: OptionalAuthorizationDependency,
    service: ReporteFormulacionPresupuestoServiceDependency,
    params: Annotated[ReporteFormulacionPresupuestoSyncParams, Depends()],
):
    if auth.is_admin:
        params.siif_username = settings.SIIF_USERNAME
        params.siif_password = settings.SIIF_PASSWORD

    return await service.sync_formulacion_presupuesto_from_source(
        username=params.siif_username, password=params.siif_password, params=params
    )


# -------------------------------------------------
@reporte_formulacion_presupuesto_router.get(
    "/export",
    summary="Descarga todos los reportes como archivo .xlsx y exporta a Google Sheets",
    response_description="Archivo Excel con los registros solicitados",
)
async def export_all_from_db(
    service: ReporteFormulacionPresupuestoServiceDependency,
    params: Annotated[ReporteFormulacionPresupuestoParams, Depends()],
    upload_to_google_sheets: bool = Query(True, alias="uploadToGoogleSheets"),
):
    return await service.export_all_from_db(
        upload_to_google_sheets=upload_to_google_sheets,
        params=params
    )

# -------------------------------------------------
@reporte_formulacion_presupuesto_router.get(
    "/export_planillometro_contabilidad",
    summary="Descarga el Reporte Contabilidad como archivo .xlsx y exporta a Google Sheets",
    response_description="Archivo Excel con los registros solicitados",
)
async def export_planillometro_contabilidad(
    service: ReporteFormulacionPresupuestoServiceDependency,
    params: Annotated[ReporteFormulacionPresupuestoParams, Depends()],
    upload_to_google_sheets: bool = Query(True, alias="uploadToGoogleSheets"),
):
    return await service.export_planillometro_contabilidad(
        upload_to_google_sheets=upload_to_google_sheets,
        params=params
    )
