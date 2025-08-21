import os
from typing import Annotated, List

from fastapi import APIRouter, Depends, Query

from ...auth.services import OptionalAuthorizationDependency
from ...config import settings
from ...utils import RouteReturnSchema, apply_auto_filter, get_sqlite_path
from ..schemas import (
    ResumenRendProvDocument,
    ResumenRendProvFilter,
    ResumenRendProvParams,
)
from ..services import ResumenRendProvServiceDependency

resumen_rend_prov_router = APIRouter(
    prefix="/resumen_rend_prov", tags=["SGF - Resumen Rend. Prov."]
)


# -------------------------------------------------
@resumen_rend_prov_router.post("/sync_from_sgf", response_model=List[RouteReturnSchema])
async def sync_resumen_rend_prov_from_sgf(
    auth: OptionalAuthorizationDependency,
    service: ResumenRendProvServiceDependency,
    params: Annotated[ResumenRendProvParams, Depends()],
    username: str = None,
    password: str = None,
):
    if auth.is_admin:
        username = settings.SGF_USERNAME
        password = settings.SGF_PASSWORD

    return await service.sync_resumen_rend_prov_from_sgf(
        username=username, password=password, params=params
    )


# -------------------------------------------------
@resumen_rend_prov_router.post("/sync_from_sqlite", response_model=RouteReturnSchema)
async def sync_resumen_rend_prov_from_sqlite(
    service: ResumenRendProvServiceDependency,
    sqlite_path: str = Query(
        default=os.path.join(get_sqlite_path(), "sgf.sqlite"),
        description="Ruta al archivo SGF SQLite",
        alias="path",
    ),
):
    return await service.sync_resumen_rend_prov_from_sqlite(sqlite_path)


# -------------------------------------------------
@resumen_rend_prov_router.get(
    "/get_from_db", response_model=List[ResumenRendProvDocument]
)
async def get_resumen_rend_prov_from_db(
    service: ResumenRendProvServiceDependency,
    params: Annotated[ResumenRendProvFilter, Depends()],
):
    apply_auto_filter(params=params)
    return await service.get_resumen_rend_prov_from_db(params=params)


# -------------------------------------------------
@resumen_rend_prov_router.get(
    "/export",
    summary="Descarga los registros Resumen Rend. Prov. como archivo .xlsx",
    response_description="Archivo Excel con los registros solicitados",
)
async def export_resumen_rend_prov_from_db(
    service: ResumenRendProvServiceDependency,
    ejercicio: int = None,
):
    return await service.export_resumen_rend_prov_from_db(ejercicio)
