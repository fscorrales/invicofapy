import os
from typing import Annotated, List

from fastapi import APIRouter, Depends, Query

from ...auth.services import OptionalAuthorizationDependency
from ...config import settings
from ...utils import RouteReturnSchema, apply_auto_filter, get_sqlite_path
from ..schemas import Rdeu012Document, Rdeu012Filter, Rdeu012Params
from ..services import Rdeu012ServiceDependency

rdeu012_router = APIRouter(prefix="/rdeu012")


@rdeu012_router.post("/sync_from_siif", response_model=List[RouteReturnSchema])
async def sync_rdeu012_from_siif(
    auth: OptionalAuthorizationDependency,
    service: Rdeu012ServiceDependency,
    params: Annotated[Rdeu012Params, Depends()],
    username: str = None,
    password: str = None,
):
    if auth.is_admin:
        username = settings.SIIF_USERNAME
        password = settings.SIIF_PASSWORD

    return await service.sync_rdeu012_from_siif(
        username=username, password=password, params=params
    )


# -------------------------------------------------
@rdeu012_router.post("/sync_from_sqlite", response_model=RouteReturnSchema)
async def sync_ri102_from_sqlite(
    service: Rdeu012ServiceDependency,
    sqlite_path: str = Query(
        default=os.path.join(get_sqlite_path(), "siif.sqlite"),
        description="Ruta al archivo SIIF SQLite",
        alias="path",
    ),
):
    return await service.sync_rdeu012_from_sqlite(sqlite_path)


@rdeu012_router.get("/get_from_db", response_model=List[Rdeu012Document])
async def get_rdeu012_from_db(
    service: Rdeu012ServiceDependency,
    params: Annotated[Rdeu012Filter, Depends()],
):
    apply_auto_filter(params=params)
    return await service.get_rdeu012_from_db(params=params)


# -------------------------------------------------
@rdeu012_router.get(
    "/export",
    summary="Descarga los registros rdeu012 como archivo .xlsx",
    response_description="Archivo Excel con los registros solicitados",
)
async def export_rdeu012_from_db(
    service: Rdeu012ServiceDependency, ejercicio: int = None
):
    return await service.export_rdeu012_from_db(ejercicio)
