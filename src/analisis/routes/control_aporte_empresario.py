from typing import Annotated, List

from fastapi import APIRouter, Depends, Query

from ...auth.services import OptionalAuthorizationDependency
from ...config import settings
from ...utils import RouteReturnSchema
from ..schemas.control_aporte_empresario import (
    ControlAporteEmpresarioParams,
    ControlAporteEmpresarioSyncParams,
)
from ..services import ControlAporteEmpresarioServiceDependency

control_aporte_empresario_router = APIRouter(prefix="/aporte_empresario")


# -------------------------------------------------
@control_aporte_empresario_router.post(
    "/sync_from_source", response_model=List[RouteReturnSchema]
)
async def sync_control_aporte_empresario_from_source(
    auth: OptionalAuthorizationDependency,
    service: ControlAporteEmpresarioServiceDependency,
    params: Annotated[ControlAporteEmpresarioSyncParams, Depends()],
):
    if auth.is_admin:
        params.siif_username = settings.SIIF_USERNAME
        params.siif_password = settings.SIIF_PASSWORD

    return await service.sync_control_aporte_empresario_from_source(params=params)


# -------------------------------------------------
@control_aporte_empresario_router.post(
    "/compute", response_model=List[RouteReturnSchema]
)
async def compute_all(
    service: ControlAporteEmpresarioServiceDependency,
    params: Annotated[ControlAporteEmpresarioParams, Depends()],
):
    return await service.compute_all(params=params)


# -------------------------------------------------
@control_aporte_empresario_router.get(
    "/export",
    summary="Descarga todos los controles como archivo .xlsx y exporta a Google Sheets",
    response_description="Archivo Excel con los registros solicitados",
)
async def export_all_from_db(
    service: ControlAporteEmpresarioServiceDependency,
    params: Annotated[ControlAporteEmpresarioParams, Depends()],
    upload_to_google_sheets: bool = Query(True, alias="uploadToGoogleSheets"),
):
    return await service.export_all_from_db(
        upload_to_google_sheets=upload_to_google_sheets, params=params
    )
