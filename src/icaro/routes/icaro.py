__all__ = ["icaro_router"]

import os
from typing import Annotated, List

from fastapi import APIRouter, Depends, HTTPException, Query

# from ...auth.services import OptionalAuthorizationDependency
# from ...config import settings
# from ...utils import RouteReturnSchema, apply_auto_filter, get_r_icaro_path
# from ..schemas import Rf602Document, Rf602Filter, Rf602Params
# from ..services import Rf602ServiceDependency
from ...utils import RouteReturnSchema, apply_auto_filter, get_r_icaro_path
from ..schemas import (
    CargaDocument,
    CargaFilter,
    EstructurasDocument,
    EstructurasFilter,
    ObrasDocument,
    ObrasFilter,
    ProveedoresDocument,
    ProveedoresFilter,
)
from ..services import IcaroServiceDependency

icaro_router = APIRouter(prefix="/icaro", tags=["ICARO"])


# @icaro_router.post("/migrate")
# async def migrate_sqlite_path(
#     # path: str = Body(..., example=get_r_icaro_path())
# ):
#     path = os.path.join(get_r_icaro_path(), "ICARO.sqlite")
#     # Validar que existe
#     if not os.path.isfile(path):
#         raise HTTPException(
#             status_code=404, detail=f"La ruta {path} del archivo no existe"
#         )

#     migrator = IcaroMongoMigrator(sqlite_path=path)

#     await migrator.migrate_all()
#     return {"detail": f"Archivo migrado exitosamente desde {path}"}


# -------------------------------------------------
@icaro_router.post("/sync_from_sqlite", response_model=List[RouteReturnSchema])
async def sync_all_from_sqlite(
    service: IcaroServiceDependency,
    sqlite_path: str = Query(
        default=os.path.join(get_r_icaro_path(), "ICARO.sqlite"),
        description="Ruta al archivo ICARO SQLite",
        alias="path",
    ),
):
    return await service.sync_all_from_sqlite(sqlite_path)


# -------------------------------------------------
@icaro_router.get("/get_carga_from_db", response_model=List[CargaDocument])
async def get_carga_from_db(
    service: IcaroServiceDependency,
    params: Annotated[CargaFilter, Depends()],
):
    apply_auto_filter(params=params)
    return await service.get_carga_from_db(params=params)


# -------------------------------------------------
@icaro_router.get("/get_estructuras_from_db", response_model=List[EstructurasDocument])
async def get_estructuras_from_db(
    service: IcaroServiceDependency,
    params: Annotated[EstructurasFilter, Depends()],
):
    apply_auto_filter(params=params)
    return await service.get_estructuras_from_db(params=params)


# -------------------------------------------------
@icaro_router.get("/get_obras_from_db", response_model=List[ObrasDocument])
async def get_obras_from_db(
    service: IcaroServiceDependency,
    params: Annotated[ObrasFilter, Depends()],
):
    apply_auto_filter(params=params)
    return await service.get_obras_from_db(params=params)


# -------------------------------------------------
@icaro_router.get("/get_proveedores_from_db", response_model=List[ProveedoresDocument])
async def get_proveedores_from_db(
    service: IcaroServiceDependency,
    params: Annotated[ProveedoresFilter, Depends()],
):
    apply_auto_filter(params=params)
    return await service.get_proveedores_from_db(params=params)


# -------------------------------------------------
@icaro_router.get(
    "/export",
    summary="Descarga los registros de las tablas Icaro como archivo .xlsx",
    response_description="Archivo Excel con los registros solicitados",
)
async def export_all_from_db(service: IcaroServiceDependency):
    return await service.export_all_from_db()
