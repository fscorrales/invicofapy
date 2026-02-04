import os
from typing import Annotated, List

from fastapi import APIRouter, Depends, Query

from ...auth.services import OptionalAuthorizationDependency
from ...config import settings
from ...utils import RouteReturnSchema, apply_auto_filter, get_sqlite_path
from ..schemas import (
    SaldosBarriosEvolucionDocument,
    SaldosBarriosEvolucionFilter,
    SaldosBarriosEvolucionParams,
)
from ..services import SaldosBarriosEvolucionServiceDependency

saldos_barrios_evolucion_router = APIRouter(prefix="/saldos_barrios_evolucion")


@saldos_barrios_evolucion_router.post(
    "/sync_from_sgv", response_model=List[RouteReturnSchema]
)
async def sync_saldos_barrios_evolucion_from_sgv(
    auth: OptionalAuthorizationDependency,
    service: SaldosBarriosEvolucionServiceDependency,
    params: Annotated[SaldosBarriosEvolucionParams, Depends()],
    username: str = None,
    password: str = None,
):
    if auth.is_admin:
        username = settings.SGV_USERNAME
        password = settings.SGV_PASSWORD

    return await service.sync_saldos_barrios_evolucion_from_sgv(
        username=username, password=password, params=params
    )


# -------------------------------------------------
@saldos_barrios_evolucion_router.post(
    "/sync_from_sqlite", response_model=RouteReturnSchema
)
async def sync_saldos_barrios_evolucion_from_sqlite(
    service: SaldosBarriosEvolucionServiceDependency,
    sqlite_path: str = Query(
        default=os.path.join(get_sqlite_path(), "sgv.sqlite"),
        description="Ruta al archivo SGV SQLite",
        alias="path",
    ),
):
    return await service.sync_saldos_barrios_evolucion_from_sqlite(sqlite_path)


# -------------------------------------------------
@saldos_barrios_evolucion_router.get(
    "/get_from_db", response_model=List[SaldosBarriosEvolucionDocument]
)
async def get_saldos_barrios_evolucion_from_db(
    service: SaldosBarriosEvolucionServiceDependency,
    params: Annotated[SaldosBarriosEvolucionFilter, Depends()],
):
    apply_auto_filter(params=params)
    return await service.get_saldos_barrios_evolucion_from_db(params=params)


# -------------------------------------------------
@saldos_barrios_evolucion_router.get(
    "/export",
    summary="Descarga los registros Saldos Barrios Evolución como archivo .xlsx",
    response_description="Archivo Excel con los registros solicitados",
)
async def export_saldos_barrios_evolucion_from_db(
    service: SaldosBarriosEvolucionServiceDependency, ejercicio: int = None
):
    return await service.export_saldos_barrios_evolucion_from_db(ejercicio)
