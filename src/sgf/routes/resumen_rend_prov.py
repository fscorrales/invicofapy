from typing import Annotated, List

from fastapi import APIRouter, Depends

from ...auth.services import OptionalAuthorizationDependency
from ...config import settings
from ...utils import RouteReturnSchema, apply_auto_filter
from ..schemas import (
    ResumenRendProvDocument,
    ResumenRendProvFilter,
    ResumenRendProvParams,
)
from ..services import ResumenRendProvServiceDependency

resumen_rend_prov_router = APIRouter(
    prefix="/resumen_rend_prov", tags=["SGF - Resumen Rend. Prov."]
)


@resumen_rend_prov_router.post("/sync_from_sgf", response_model=RouteReturnSchema)
async def sync_resumen_rend_prov_from_sgf(
    auth: OptionalAuthorizationDependency,
    service: ResumenRendProvServiceDependency,
    params: Annotated[ResumenRendProvParams, Depends()],
    username: str = None,
    password: str = None,
):
    if auth.is_admin:
        username = settings.SGF_USERNAME
        password = settings.SGF_PASSWORD

    return await service.sync_resumen_rend_prov_from_sgf(
        username=username, password=password, params=params
    )


@resumen_rend_prov_router.get(
    "/get_from_db", response_model=List[ResumenRendProvDocument]
)
async def get_resumen_rend_prov_from_db(
    service: ResumenRendProvServiceDependency,
    params: Annotated[ResumenRendProvFilter, Depends()],
):
    apply_auto_filter(params=params)
    return await service.get_resumen_rend_prov_from_db(params=params)


# @rf602_router.post("/download_and_update/")
# async def siif_download(
#     ejercicio: str,
#     service: Rf602ServiceDependency,
# ) -> Rf602ValidationOutput:
#     return await service.download_and_update(ejercicio=ejercicio)
