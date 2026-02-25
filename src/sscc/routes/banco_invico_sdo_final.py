import os
from typing import Annotated, List

from fastapi import APIRouter, Depends, Query

from ...utils import (
    RouteReturnSchema,
    apply_auto_filter,
    get_sqlite_path,
    get_sscc_saldos_path,
)
from ..schemas import BancoINVICOSdoFinalDocument, BancoINVICOSdoFinalFilter
from ..services import BancoINVICOSdoFinalServiceDependency

banco_invico_sdo_final_cuit_router = APIRouter(prefix="/banco_invico_sdo_final")


# -------------------------------------------------
@banco_invico_sdo_final_cuit_router.post(
    "/sync_from_csv", response_model=RouteReturnSchema
)
async def sync_banco_invico_sdo_final_from_csv(
    service: BancoINVICOSdoFinalServiceDependency,
    csv_path: str = Query(
        default=os.path.join(get_sscc_saldos_path(), "saldos_sscc.csv"),
        description="Ruta al archivo Saldos SSCC CSV",
        alias="path",
    ),
):
    return await service.sync_banco_invico_sdo_final_from_csv(csv_path)


# -------------------------------------------------
@banco_invico_sdo_final_cuit_router.post(
    "/sync_from_sqlite", response_model=RouteReturnSchema
)
async def sync_banco_invico_sdo_final_from_sqlite(
    service: BancoINVICOSdoFinalServiceDependency,
    sqlite_path: str = Query(
        default=os.path.join(get_sqlite_path(), "sscc.sqlite"),
        description="Ruta al archivo SSCC SQLite",
        alias="path",
    ),
):
    return await service.sync_banco_invico_sdo_final_from_sqlite(sqlite_path)


# -------------------------------------------------
@banco_invico_sdo_final_cuit_router.get(
    "/get_from_db", response_model=List[BancoINVICOSdoFinalDocument]
)
async def get_banco_invico_sdo_final_from_db(
    service: BancoINVICOSdoFinalServiceDependency,
    params: Annotated[BancoINVICOSdoFinalFilter, Depends()],
):
    apply_auto_filter(params=params)
    return await service.get_banco_invico_sdo_final_from_db(params=params)


# -------------------------------------------------
@banco_invico_sdo_final_cuit_router.get(
    "/export",
    summary="Descarga los registros de BancoINVICOSdoFinal como archivo .xlsx",
    response_description="Archivo Excel con los registros solicitados",
)
async def export_banco_invico_sdo_final_from_db(
    service: BancoINVICOSdoFinalServiceDependency,
):
    return await service.export_banco_invico_sdo_final_from_db()
