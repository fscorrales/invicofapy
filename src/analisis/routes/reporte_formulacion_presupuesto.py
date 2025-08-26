from typing import Annotated, List

from fastapi import APIRouter, Depends, Query

from ...auth.services import OptionalAuthorizationDependency
from ...config import settings
from ...utils import RouteReturnSchema
from ..schemas.reporte_formulacion_presupuesto import (
    ReporteFormulacionPresupuestoParams,
)
from ..services.reporte_formulacion_presupuesto import ReporteFormulacionPresupuestoServiceDependency

reporte_formulacion_presupuesto_router = APIRouter(prefix="/formulacion_presupuesto")


# -------------------------------------------------
@reporte_formulacion_presupuesto_router.post(
    "/sync_from_source", response_model=List[RouteReturnSchema]
)
async def sync_formulacion_presupuesto_from_source(
    auth: OptionalAuthorizationDependency,
    service: ReporteFormulacionPresupuestoServiceDependency,
    params: Annotated[ReporteFormulacionPresupuestoParams, Depends()],
    username: str = None,
    password: str = None,
):
    if auth.is_admin:
        username = settings.SIIF_USERNAME
        password = settings.SIIF_PASSWORD

    return await service.sync_formulacion_presupuesto_from_source(
        username=username, password=password, params=params
    )


# # -------------------------------------------------
# @reporte_modulos_basicos_router.post(
#     "/generate", response_model=List[RouteReturnSchema]
# )
# async def generate_all(
#     service: ReporteModulosBasicosServiceDependency,
#     params: Annotated[ReporteModulosBasicosIcaroParams, Depends()],
# ):
#     return await service.generate_all(params=params)


# # -------------------------------------------------
# @reporte_modulos_basicos_router.get(
#     "/export",
#     summary="Descarga todos los controles como archivo .xlsx y exporta a Google Sheets",
#     response_description="Archivo Excel con los registros solicitados",
# )
# async def export_all_from_db(
#     service: ReporteModulosBasicosServiceDependency,
#     upload_to_google_sheets: bool = Query(True, alias="uploadToGoogleSheets"),
# ):
#     return await service.export_all_from_db(
#         upload_to_google_sheets=upload_to_google_sheets
#     )


# # -------------------------------------------------
# @icaro_vs_siif_router.post("/control_anual/compute", response_model=RouteReturnSchema)
# async def compute_control_anual(
#     service: IcaroVsSIIFServiceDependency,
#     params: Annotated[ControlCompletoParams, Depends()],
# ):
#     return await service.compute_control_anual(params=params)


# # -------------------------------------------------
# @icaro_vs_siif_router.get(
#     "/control_anual/get_from_db", response_model=List[ControlAnualDocument]
# )
# async def get_control_anual_from_db(
#     service: IcaroVsSIIFServiceDependency,
#     params: Annotated[ControlAnualFilter, Depends()],
# ):
#     apply_auto_filter(params=params)
#     return await service.get_control_anual_from_db(params=params)


# # -------------------------------------------------
# @icaro_vs_siif_router.get(
#     "/control_anual/export",
#     summary="Descarga el Control Anual como archivo .xlsx y exporta a Google Sheets",
#     response_description="Archivo Excel con los registros solicitados",
# )
# async def export_control_anual_from_db(
#     service: IcaroVsSIIFServiceDependency,
#     upload_to_google_sheets: bool = Query(True, alias="uploadToGoogleSheets"),
# ):
#     return await service.export_control_anual_from_db(
#         upload_to_google_sheets=upload_to_google_sheets
#     )


# # -------------------------------------------------
# @icaro_vs_siif_router.post(
#     "/control_comprobantes/compute", response_model=RouteReturnSchema
# )
# async def compute_control_comprobantes(
#     service: IcaroVsSIIFServiceDependency,
#     params: Annotated[ControlCompletoParams, Depends()],
# ):
#     return await service.compute_control_comprobantes(params=params)


# # -------------------------------------------------
# @icaro_vs_siif_router.get(
#     "/control_comprobantes/get_from_db",
#     response_model=List[ControlComprobantesDocument],
# )
# async def get_control_comprobantes_from_db(
#     service: IcaroVsSIIFServiceDependency,
#     params: Annotated[ControlComprobantesFilter, Depends()],
# ):
#     apply_auto_filter(params=params)
#     return await service.get_control_comprobantes_from_db(params=params)


# # -------------------------------------------------
# @icaro_vs_siif_router.post("/control_pa6/compute", response_model=RouteReturnSchema)
# async def compute_control_pa6(
#     service: IcaroVsSIIFServiceDependency,
#     params: Annotated[ControlCompletoParams, Depends()],
# ):
#     return await service.compute_control_pa6(params=params)


# # -------------------------------------------------
# @icaro_vs_siif_router.get(
#     "/control_pa6/get_from_db",
#     response_model=List[ControlPa6Document],
# )
# async def get_control_pa6_from_db(
#     service: IcaroVsSIIFServiceDependency,
#     params: Annotated[ControlPa6Filter, Depends()],
# ):
#     apply_auto_filter(params=params)
#     return await service.get_control_pa6_from_db(params=params)


# # -------------------------------------------------
# @icaro_vs_siif_router.get(
#     "/control_pa6/export",
#     summary="Descarga el Control PA6 como archivo .xlsx y exporta a Google Sheets",
#     response_description="Archivo Excel con los registros solicitados",
# )
# async def export_control_pa6_from_db(
#     service: IcaroVsSIIFServiceDependency,
#     upload_to_google_sheets: bool = Query(True, alias="uploadToGoogleSheets"),
# ):
#     return await service.export_control_pa6_from_db(
#         upload_to_google_sheets=upload_to_google_sheets
#     )
