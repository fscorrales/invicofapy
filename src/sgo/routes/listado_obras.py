import os
from typing import Annotated, List

from fastapi import APIRouter, Depends, Query

from ...auth.services import OptionalAuthorizationDependency
from ...config import settings
from ...utils import RouteReturnSchema, apply_auto_filter, get_sqlite_path
from ..schemas import (
    ListadoObrasDocument,
    ListadoObrasFilter,
    ListadoObrasParams,
)
from ..services import ListadoObrasServiceDependency

listado_obras_router = APIRouter(prefix="/listado_obras")


@listado_obras_router.post("/sync_from_sgo", response_model=List[RouteReturnSchema])
async def sync_listado_obras_from_sgo(
    auth: OptionalAuthorizationDependency,
    service: ListadoObrasServiceDependency,
    params: Annotated[ListadoObrasParams, Depends()],
    username: str = None,
    password: str = None,
):
    if auth.is_admin:
        username = settings.SGO_USERNAME
        password = settings.SGO_PASSWORD

    return await service.sync_listado_obras_from_sgo(
        username=username, password=password, params=params
    )


# -------------------------------------------------
@listado_obras_router.post("/sync_from_sqlite", response_model=RouteReturnSchema)
async def sync_listado_obras_from_sqlite(
    service: ListadoObrasServiceDependency,
    sqlite_path: str = Query(
        default=os.path.join(get_sqlite_path(), "sgo.sqlite"),
        description="Ruta al archivo SGO SQLite",
        alias="path",
    ),
):
    return await service.sync_listado_obras_from_sqlite(sqlite_path)


# -------------------------------------------------
@listado_obras_router.get("/get_from_db", response_model=List[ListadoObrasDocument])
async def get_listado_obras_from_db(
    service: ListadoObrasServiceDependency,
    params: Annotated[ListadoObrasFilter, Depends()],
):
    apply_auto_filter(params=params)
    return await service.get_listado_obras_from_db(params=params)


# -------------------------------------------------
@listado_obras_router.get(
    "/export",
    summary="Descarga los registros Listado Obras como archivo .xlsx",
    response_description="Archivo Excel con los registros solicitados",
)
async def export_listado_obras_from_db(
    service: ListadoObrasServiceDependency, ejercicio: int = None
):
    return await service.export_listado_obras_from_db(ejercicio)
