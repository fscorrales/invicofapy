import os
from typing import Annotated, List

from fastapi import APIRouter, Depends, Query

from ...auth.services import OptionalAuthorizationDependency
from ...config import settings
from ...utils import RouteReturnSchema, apply_auto_filter, get_sqlite_path
from ..schemas import Rf610Document, Rf610Filter, Rf610Params
from ..services import Rf610ServiceDependency

rf610_router = APIRouter(prefix="/rf610")


@rf610_router.post("/sync_from_siif", response_model=List[RouteReturnSchema])
async def sync_rf610_from_siif(
    auth: OptionalAuthorizationDependency,
    service: Rf610ServiceDependency,
    params: Annotated[Rf610Params, Depends()],
    username: str = None,
    password: str = None,
):
    if auth.is_admin:
        username = settings.SIIF_USERNAME
        password = settings.SIIF_PASSWORD

    return await service.sync_rf610_from_siif(
        username=username, password=password, params=params
    )


# -------------------------------------------------
@rf610_router.post("/sync_from_sqlite", response_model=RouteReturnSchema)
async def sync_rf602_from_sqlite(
    service: Rf610ServiceDependency,
    sqlite_path: str = Query(
        default=os.path.join(get_sqlite_path(), "siif.sqlite"),
        description="Ruta al archivo SIIF SQLite",
        alias="path",
    ),
):
    return await service.sync_rf602_from_sqlite(sqlite_path)


@rf610_router.get("/get_from_db", response_model=List[Rf610Document])
async def get_rf610_from_db(
    service: Rf610ServiceDependency,
    params: Annotated[Rf610Filter, Depends()],
):
    apply_auto_filter(params=params)
    return await service.get_rf610_from_db(params=params)


# -------------------------------------------------
@rf610_router.get(
    "/export",
    summary="Descarga los registros rf610 como archivo .xlsx",
    response_description="Archivo Excel con los registros solicitados",
)
async def export_rf610_from_db(service: Rf610ServiceDependency, ejercicio: int = None):
    return await service.export_rf610_from_db(ejercicio)
