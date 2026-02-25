import os
from typing import Annotated, List

from fastapi import APIRouter, Depends, Query

from ...utils import (
    RouteReturnSchema,
    apply_auto_filter,
    get_siif_rdeu012b2_cuit_path,
    get_sqlite_path,
)
from ..schemas import Rdeu012b2CuitDocument, Rdeu012b2CuitFilter
from ..services import Rdeu012b2CuitServiceDependency

rdeu012b2_cuit_router = APIRouter(prefix="/rdeu012b2_cuit")


# -------------------------------------------------
@rdeu012b2_cuit_router.post("/sync_from_pdf", response_model=RouteReturnSchema)
async def sync_rdeu012b2_cuit_from_pdf(
    service: Rdeu012b2CuitServiceDependency,
    pdf_path: str = Query(
        default=os.path.join(get_siif_rdeu012b2_cuit_path(), "rdeu012b2_cuit.pdf"),
        description="Ruta al archivo Rdeu012b2Cuit PDF",
        alias="path",
    ),
):
    return await service.sync_rdeu012b2_cuit_from_pdf(pdf_path)


# -------------------------------------------------
@rdeu012b2_cuit_router.post("/sync_from_sqlite", response_model=RouteReturnSchema)
async def sync_rdeu012b2_cuit_from_sqlite(
    service: Rdeu012b2CuitServiceDependency,
    sqlite_path: str = Query(
        default=os.path.join(get_sqlite_path(), "siif.sqlite"),
        description="Ruta al archivo SIIF SQLite",
        alias="path",
    ),
):
    return await service.sync_rdeu012b2_cuit_from_sqlite(sqlite_path)


# -------------------------------------------------
@rdeu012b2_cuit_router.get("/get_from_db", response_model=List[Rdeu012b2CuitDocument])
async def get_rdeu012b2_cuit_from_db(
    service: Rdeu012b2CuitServiceDependency,
    params: Annotated[Rdeu012b2CuitFilter, Depends()],
):
    apply_auto_filter(params=params)
    return await service.get_rdeu012b2_cuit_from_db(params=params)


# -------------------------------------------------
@rdeu012b2_cuit_router.get(
    "/export",
    summary="Descarga los registros de Rdeu012b2Cuit como archivo .xlsx",
    response_description="Archivo Excel con los registros solicitados",
)
async def export_rdeu012b2_cuit_from_db(
    service: Rdeu012b2CuitServiceDependency,
):
    return await service.export_rdeu012b2_cuit_from_db()
