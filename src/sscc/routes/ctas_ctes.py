import os
from typing import Annotated, List

from fastapi import APIRouter, Depends, Query

from ...utils import RouteReturnSchema, apply_auto_filter, get_sscc_cta_cte_path
from ..schemas import CtasCtesDocument, CtasCtesFilter
from ..services import CtasCtesServiceDependency

ctas_ctes_router = APIRouter(prefix="/ctas_ctes")


# -------------------------------------------------
@ctas_ctes_router.post("/sync_from_excel", response_model=RouteReturnSchema)
async def sync_ctas_ctes_from_excel(
    service: CtasCtesServiceDependency,
    excel_path: str = Query(
        default=os.path.join(get_sscc_cta_cte_path(), "cta_cte.xlsx"),
        description="Ruta al archivo Ctas Ctes EXCEL",
        alias="path",
    ),
):
    return await service.sync_ctas_ctes_from_excel(excel_path)


@ctas_ctes_router.get("/get_from_db", response_model=List[CtasCtesDocument])
async def get_ctas_ctes_from_db(
    service: CtasCtesServiceDependency,
    params: Annotated[CtasCtesFilter, Depends()],
):
    apply_auto_filter(params=params)
    return await service.get_ctas_ctes_from_db(params=params)


# -------------------------------------------------
@ctas_ctes_router.get(
    "/export",
    summary="Descarga los registros de Cuentas Corrientes como archivo .xlsx",
    response_description="Archivo Excel con los registros solicitados",
)
async def export_ctas_ctes_from_db(service: CtasCtesServiceDependency):
    return await service.export_ctas_ctes_from_db()
