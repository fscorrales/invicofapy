import os
from typing import Annotated, List

from fastapi import APIRouter, Depends, Query

from ...auth.services import OptionalAuthorizationDependency
from ...config import settings
from ...utils import RouteReturnSchema, apply_auto_filter, get_sqlite_path
from ..schemas import Rci02Document, Rci02Filter, Rci02Params
from ..services import Rci02ServiceDependency

rci02_router = APIRouter(prefix="/rci02")


@rci02_router.post("/sync_from_siif", response_model=RouteReturnSchema)
async def sync_rci02_from_siif(
    auth: OptionalAuthorizationDependency,
    service: Rci02ServiceDependency,
    params: Annotated[Rci02Params, Depends()],
    username: str = None,
    password: str = None,
):
    if auth.is_admin:
        username = settings.SIIF_USERNAME
        password = settings.SIIF_PASSWORD

    return await service.sync_ri102_from_siif(
        username=username, password=password, params=params
    )


# -------------------------------------------------
@rci02_router.post("/sync_from_sqlite", response_model=RouteReturnSchema)
async def sync_rci02_from_sqlite(
    service: Rci02ServiceDependency,
    sqlite_path: str = Query(
        default=os.path.join(get_sqlite_path(), "siif.sqlite"),
        description="Ruta al archivo SIIF SQLite",
        alias="path",
    ),
):
    return await service.sync_rci02_from_sqlite(sqlite_path)


@rci02_router.get("/get_from_db", response_model=List[Rci02Document])
async def get_rci02_from_db(
    service: Rci02ServiceDependency,
    params: Annotated[Rci02Filter, Depends()],
):
    apply_auto_filter(params=params)
    return await service.get_rci02_from_db(params=params)


# -------------------------------------------------
@rci02_router.get(
    "/export",
    summary="Descarga los registros rci02 como archivo .xlsx",
    response_description="Archivo Excel con los registros solicitados",
)
async def export_rci02_from_db(service: Rci02ServiceDependency, ejercicio: int = None):
    return await service.export_rci02_from_db(ejercicio)
