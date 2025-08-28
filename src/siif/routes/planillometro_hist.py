import os
from typing import Annotated, List

from fastapi import APIRouter, Depends, Query

from ...utils import (
    RouteReturnSchema,
    apply_auto_filter,
    get_siif_planillometro_hist_path,
)
from ..schemas import PlanillometroHistDocument, PlanillometroHistFilter
from ..services import PlanillometroHistServiceDependency

planillometro_hist_router = APIRouter(prefix="/planillometro_hist")


# -------------------------------------------------
@planillometro_hist_router.post("/sync_from_excel", response_model=RouteReturnSchema)
async def sync_planillometro_hist_from_excel(
    service: PlanillometroHistServiceDependency,
    excel_path: str = Query(
        default=os.path.join(
            get_siif_planillometro_hist_path(), "planillometro_hist.xlsx"
        ),
        description="Ruta al archivo Planillometro Historico EXCEL",
        alias="path",
    ),
):
    return await service.sync_planillometro_hist_from_excel(excel_path)


@planillometro_hist_router.get(
    "/get_from_db", response_model=List[PlanillometroHistDocument]
)
async def get_planillometro_hist_from_db(
    service: PlanillometroHistServiceDependency,
    params: Annotated[PlanillometroHistFilter, Depends()],
):
    apply_auto_filter(params=params)
    return await service.get_planillometro_hist_from_db(params=params)


# -------------------------------------------------
@planillometro_hist_router.get(
    "/export",
    summary="Descarga los registros de Planillometro Historico como archivo .xlsx",
    response_description="Archivo Excel con los registros solicitados",
)
async def export_planillometro_hist_from_db(
    service: PlanillometroHistServiceDependency,
):
    return await service.export_planillometro_hist_from_db()
