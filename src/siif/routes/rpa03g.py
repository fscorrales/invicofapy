from typing import Annotated, List

from fastapi import APIRouter, Depends

from ...auth.services import OptionalAuthorizationDependency
from ...config import settings
from ...utils import RouteReturnSchema, apply_auto_filter
from ..schemas import Rpa03gDocument, Rpa03gFilter, Rpa03gParams
from ..services import Rpa03gServiceDependency

rpa03g_router = APIRouter(prefix="/rpa03g")


@rpa03g_router.post("/sync_from_siif", response_model=RouteReturnSchema)
async def sync_rpa03_from_siif(
    auth: OptionalAuthorizationDependency,
    service: Rpa03gServiceDependency,
    params: Annotated[Rpa03gParams, Depends()],
    username: str = None,
    password: str = None,
):
    if auth.is_admin:
        username = settings.SIIF_USERNAME
        password = settings.SIIF_PASSWORD

    return await service.sync_rpa03_from_siif(
        username=username, password=password, params=params
    )


@rpa03g_router.get("/get_from_db", response_model=List[Rpa03gDocument])
async def get_rpa03_from_db(
    service: Rpa03gServiceDependency,
    params: Annotated[Rpa03gFilter, Depends()],
):
    apply_auto_filter(params=params)
    return await service.get_rpa03_from_db(params=params)
