import os
from typing import Annotated, List

from fastapi import APIRouter, Depends, Query

from ...auth.services import OptionalAuthorizationDependency
from ...config import settings
from ...utils import RouteReturnSchema, apply_auto_filter, get_sqlite_path
from ..schemas import Ri102Document, Ri102Filter, Ri102Params
from ..services import Ri102ServiceDependency

ri102_router = APIRouter(prefix="/ri102")


@ri102_router.post("/sync_from_siif", response_model=List[RouteReturnSchema])
async def sync_ri102_from_siif(
    auth: OptionalAuthorizationDependency,
    service: Ri102ServiceDependency,
    params: Annotated[Ri102Params, Depends()],
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
@ri102_router.post("/sync_from_sqlite", response_model=RouteReturnSchema)
async def sync_ri102_from_sqlite(
    service: Ri102ServiceDependency,
    sqlite_path: str = Query(
        default=os.path.join(get_sqlite_path(), "siif.sqlite"),
        description="Ruta al archivo SIIF SQLite",
        alias="path",
    ),
):
    return await service.sync_ri102_from_sqlite(sqlite_path)


# -------------------------------------------------
@ri102_router.get("/get_from_db", response_model=List[Ri102Document])
async def get_ri102_from_db(
    service: Ri102ServiceDependency,
    params: Annotated[Ri102Filter, Depends()],
):
    apply_auto_filter(params=params)
    return await service.get_ri102_from_db(params=params)


# -------------------------------------------------
@ri102_router.get(
    "/export",
    summary="Descarga los registros ri102 como archivo .xlsx",
    response_description="Archivo Excel con los registros solicitados",
)
async def export_ri102_from_db(service: Ri102ServiceDependency, ejercicio: int = None):
    return await service.export_ri102_from_db(ejercicio)
