import os
from typing import Annotated, List

from fastapi import APIRouter, Depends, Query

from ...auth.services import OptionalAuthorizationDependency
from ...config import settings
from ...utils import RouteReturnSchema, apply_auto_filter, get_sqlite_path
from ..schemas import Rpa03gDocument, Rpa03gFilter, Rpa03gParams
from ..services import Rpa03gServiceDependency

rpa03g_router = APIRouter(prefix="/rpa03g")


# -------------------------------------------------
@rpa03g_router.post("/sync_from_siif", response_model=List[RouteReturnSchema])
async def sync_rpa03g_from_siif(
    auth: OptionalAuthorizationDependency,
    service: Rpa03gServiceDependency,
    params: Annotated[Rpa03gParams, Depends()],
    username: str = None,
    password: str = None,
):
    if auth.is_admin:
        username = settings.SIIF_USERNAME
        password = settings.SIIF_PASSWORD

    return await service.sync_rpa03g_from_siif(
        username=username, password=password, params=params
    )


# -------------------------------------------------
@rpa03g_router.post("/sync_from_sqlite", response_model=RouteReturnSchema)
async def sync_rpa03g_from_sqlite(
    service: Rpa03gServiceDependency,
    sqlite_path: str = Query(
        default=os.path.join(get_sqlite_path(), "siif.sqlite"),
        description="Ruta al archivo SIIF SQLite",
        alias="path",
    ),
):
    return await service.sync_rpa03g_from_sqlite(sqlite_path)


# -------------------------------------------------
@rpa03g_router.get("/get_from_db", response_model=List[Rpa03gDocument])
async def get_rpa03g_from_db(
    service: Rpa03gServiceDependency,
    params: Annotated[Rpa03gFilter, Depends()],
):
    apply_auto_filter(params=params)
    return await service.get_rpa03g_from_db(params=params)


# -------------------------------------------------
@rpa03g_router.get(
    "/export",
    summary="Descarga los registros rpa03g como archivo .xlsx",
    response_description="Archivo Excel con los registros solicitados",
)
async def export_rpa03g_from_db(
    service: Rpa03gServiceDependency, ejercicio: int = None
):
    return await service.export_rpa03g_from_db(ejercicio)
