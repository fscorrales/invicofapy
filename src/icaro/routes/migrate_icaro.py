__all__ = ["icaro_router"]

import os
from typing import Annotated, List

from fastapi import APIRouter, Depends, Body, HTTPException

# from ...auth.services import OptionalAuthorizationDependency
# from ...config import settings
# from ...utils import RouteReturnSchema, apply_auto_filter, get_r_icaro_path
# from ..schemas import Rf602Document, Rf602Filter, Rf602Params
# from ..services import Rf602ServiceDependency

from ...utils import get_r_icaro_path
from ..handlers import IcaroMongoMigrator

icaro_router = APIRouter(prefix="/icaro", tags=["ICARO"])


@icaro_router.post("/migrate")
async def migrate_sqlite_path(
    # path: str = Body(..., example=get_r_icaro_path())
):
    path = os.path.join(get_r_icaro_path(), "ICARO.sqlite")
    # Validar que existe
    if not os.path.isfile(path):
        raise HTTPException(status_code=404, detail=f"La ruta {path} del archivo no existe")

    migrator = IcaroMongoMigrator(
        sqlite_path=path
    )

    await migrator.migrate_all()
    return {"detail": f"Archivo migrado exitosamente desde {path}"}


# async def sync_rf602_from_siif(
#     auth: OptionalAuthorizationDependency,
#     service: Rf602ServiceDependency,
#     params: Annotated[Rf602Params, Depends()],
#     username: str = None,
#     password: str = None,
# ):
#     if auth.is_admin:
#         username = settings.SIIF_USERNAME
#         password = settings.SIIF_PASSWORD

#     return await service.sync_rf602_from_siif(
#         username=username, password=password, params=params
#     )


