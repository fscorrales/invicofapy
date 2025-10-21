__all__ = ["slave_router"]

import os
from typing import Annotated, List

from fastapi import APIRouter, Depends, Query

from ...utils import RouteReturnSchema, apply_auto_filter, get_slave_path
from ..schemas import (
    FacturerosDocument,
    FacturerosFilter,
    HonorariosDocument,
    HonorariosFilter,
)
from ..services import SlaveServiceDependency

slave_router = APIRouter(prefix="/slave", tags=["SLAVE"])


# -------------------------------------------------
@slave_router.post("/sync_from_access", response_model=List[RouteReturnSchema])
async def sync_all_from_access(
    service: SlaveServiceDependency,
    access_path: str = Query(
        default=os.path.join(get_slave_path(), "Slave.accdb"),
        description="Ruta al archivo Slave ACCESS",
        alias="path",
    ),
):
    return await service.sync_all_from_access(access_path)


# -------------------------------------------------
@slave_router.get("/get_factureros_from_db", response_model=List[FacturerosDocument])
async def get_factureros_from_db(
    service: SlaveServiceDependency,
    params: Annotated[FacturerosFilter, Depends()],
):
    apply_auto_filter(params=params)
    return await service.get_factureros_from_db(params=params)


# -------------------------------------------------
@slave_router.get("/get_honorarios_from_db", response_model=List[HonorariosDocument])
async def get_honorarios_from_db(
    service: SlaveServiceDependency,
    params: Annotated[HonorariosFilter, Depends()],
):
    apply_auto_filter(params=params)
    return await service.get_honorarios_from_db(params=params)


# -------------------------------------------------
@slave_router.get(
    "/export",
    summary="Descarga los registros de las tablas Slave como archivo .xlsx",
    response_description="Archivo Excel con los registros solicitados",
)
async def export_all_from_db(service: SlaveServiceDependency):
    return await service.export_all_from_db()
