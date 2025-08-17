import os
from typing import Annotated, List

from fastapi import APIRouter, Depends, Query

from ...auth.services import OptionalAuthorizationDependency
from ...config import settings
from ...utils import RouteReturnSchema, apply_auto_filter, get_sqlite_path
from ..schemas import RfpP605bDocument, RfpP605bFilter, RfpP605bParams
from ..services import RfpP605bServiceDependency

rfp_p605b_router = APIRouter(prefix="/rfp_p605b")


@rfp_p605b_router.post("/sync_from_siif", response_model=List[RouteReturnSchema])
async def sync_rfp_p605b_from_siif(
    auth: OptionalAuthorizationDependency,
    service: RfpP605bServiceDependency,
    params: Annotated[RfpP605bParams, Depends()],
    username: str = None,
    password: str = None,
):
    if auth.is_admin:
        username = settings.SIIF_USERNAME
        password = settings.SIIF_PASSWORD

    return await service.sync_rfp_p605b_from_siif(
        username=username, password=password, params=params
    )


# -------------------------------------------------
@rfp_p605b_router.post("/sync_from_sqlite", response_model=RouteReturnSchema)
async def sync_rfp_p605b_from_sqlite(
    service: RfpP605bServiceDependency,
    sqlite_path: str = Query(
        default=os.path.join(get_sqlite_path(), "siif.sqlite"),
        description="Ruta al archivo SIIF SQLite",
        alias="path",
    ),
):
    return await service.sync_rfp_p605b_from_sqlite(sqlite_path)


@rfp_p605b_router.get("/get_from_db", response_model=List[RfpP605bDocument])
async def get_rfp_p605b_from_db(
    service: RfpP605bServiceDependency,
    params: Annotated[RfpP605bFilter, Depends()],
):
    apply_auto_filter(params=params)
    return await service.get_rfp_p605b_from_db(params=params)


# -------------------------------------------------
@rfp_p605b_router.get(
    "/export",
    summary="Descarga los registros rfp_p605b como archivo .xlsx",
    response_description="Archivo Excel con los registros solicitados",
)
async def export_rfp_p605b_from_db(
    service: RfpP605bServiceDependency, ejercicio: int = None
):
    return await service.export_rfp_p605b_from_db(ejercicio)
