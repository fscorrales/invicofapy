import os
from typing import Annotated, List

from fastapi import APIRouter, Depends, Query

from ...auth.services import OptionalAuthorizationDependency
from ...config import settings
from ...utils import RouteReturnSchema, apply_auto_filter, get_sqlite_path
from ..schemas import Rvicon03Document, Rvicon03Filter, Rvicon03Params
from ..services import Rvicon03ServiceDependency

rvicon03_router = APIRouter(prefix="/rvicon03")


@rvicon03_router.post("/sync_from_siif", response_model=RouteReturnSchema)
async def sync_rvicon03_from_siif(
    auth: OptionalAuthorizationDependency,
    service: Rvicon03ServiceDependency,
    params: Annotated[Rvicon03Params, Depends()],
    username: str = None,
    password: str = None,
):
    if auth.is_admin:
        username = settings.SIIF_USERNAME
        password = settings.SIIF_PASSWORD

    return await service.sync_rvicon03_from_siif(
        username=username, password=password, params=params
    )


# -------------------------------------------------
@rvicon03_router.post("/sync_from_sqlite", response_model=RouteReturnSchema)
async def sync_rvicon03_from_sqlite(
    service: Rvicon03ServiceDependency,
    sqlite_path: str = Query(
        default=os.path.join(get_sqlite_path(), "siif.sqlite"),
        description="Ruta al archivo SIIF SQLite",
        alias="path",
    ),
):
    return await service.sync_rvicon03_from_sqlite(sqlite_path)


@rvicon03_router.get("/get_from_db", response_model=List[Rvicon03Document])
async def get_rvicon03_from_db(
    service: Rvicon03ServiceDependency,
    params: Annotated[Rvicon03Filter, Depends()],
):
    apply_auto_filter(params=params)
    return await service.get_rvicon03_from_db(params=params)


# -------------------------------------------------
@rvicon03_router.get(
    "/export",
    summary="Descarga los registros rvicon03 como archivo .xlsx",
    response_description="Archivo Excel con los registros solicitados",
)
async def export_rvicon03_from_db(
    service: Rvicon03ServiceDependency, ejercicio: int = None
):
    return await service.export_rvicon03_from_db(ejercicio)
