import os
from typing import Annotated, List

from fastapi import APIRouter, Depends, Query

from ...auth.services import OptionalAuthorizationDependency
from ...config import settings
from ...utils import RouteReturnSchema, apply_auto_filter, get_sqlite_path
from ..schemas import Rcocc31Document, Rcocc31Filter, Rcocc31Params
from ..services import Rcocc31ServiceDependency

rcocc31_router = APIRouter(prefix="/rcocc31")


@rcocc31_router.post("/sync_from_siif", response_model=RouteReturnSchema)
async def sync_rcocc31_from_siif(
    auth: OptionalAuthorizationDependency,
    service: Rcocc31ServiceDependency,
    params: Annotated[Rcocc31Params, Depends()],
    username: str = None,
    password: str = None,
):
    if auth.is_admin:
        username = settings.SIIF_USERNAME
        password = settings.SIIF_PASSWORD

    return await service.sync_rcocc31_from_siif(
        username=username, password=password, params=params
    )


# -------------------------------------------------
@rcocc31_router.post("/sync_from_sqlite", response_model=RouteReturnSchema)
async def sync_rcocc31_from_sqlite(
    service: Rcocc31ServiceDependency,
    sqlite_path: str = Query(
        default=os.path.join(get_sqlite_path(), "SIIF.sqlite"),
        description="Ruta al archivo SIIF SQLite",
        alias="path",
    ),
):
    return await service.sync_rcocc31_from_sqlite(sqlite_path)


@rcocc31_router.get("/get_from_db", response_model=List[Rcocc31Document])
async def get_rcocc31_from_db(
    service: Rcocc31ServiceDependency,
    params: Annotated[Rcocc31Filter, Depends()],
):
    apply_auto_filter(params=params)
    return await service.get_rcocc31_from_db(params=params)


# -------------------------------------------------
@rcocc31_router.get(
    "/export",
    summary="Descarga los registros rcocc31 como archivo .xlsx",
    response_description="Archivo Excel con los registros solicitados",
)
async def export_rcocc31_from_db(
    service: Rcocc31ServiceDependency, ejercicio: int = None
):
    return await service.export_rcocc31_from_db(ejercicio)
