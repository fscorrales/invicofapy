import os
from typing import Annotated, List

from fastapi import APIRouter, Depends, Query

from ...utils import (
    RouteReturnSchema,
    apply_auto_filter,
    get_siif_rdeu012b2_c_path,
)
from ..schemas import Rdeu012b2CDocument, Rdeu012b2CFilter
from ..services import Rdeu012b2CServiceDependency

rdeu012b2_c_router = APIRouter(prefix="/rdeu012b2_c")


# -------------------------------------------------
@rdeu012b2_c_router.post("/sync_from_excel", response_model=RouteReturnSchema)
async def sync_rdeu012b2_c_from_excel(
    service: Rdeu012b2CServiceDependency,
    excel_path: str = Query(
        default=os.path.join(get_siif_rdeu012b2_c_path(), "rdeu012b2_c.xlsx"),
        description="Ruta al archivo Rdeu012b2C EXCEL",
        alias="path",
    ),
):
    return await service.sync_rdeu012b2_c_from_excel(excel_path)


@rdeu012b2_c_router.get("/get_from_db", response_model=List[Rdeu012b2CDocument])
async def get_rdeu012b2_c_from_db(
    service: Rdeu012b2CServiceDependency,
    params: Annotated[Rdeu012b2CFilter, Depends()],
):
    apply_auto_filter(params=params)
    return await service.get_rdeu012b2_c_from_db(params=params)


# -------------------------------------------------
@rdeu012b2_c_router.get(
    "/export",
    summary="Descarga los registros de Rdeu012b2C como archivo .xlsx",
    response_description="Archivo Excel con los registros solicitados",
)
async def export_rdeu012b2_c_from_db(
    service: Rdeu012b2CServiceDependency,
):
    return await service.export_rdeu012b2_c_from_db()
