from typing import Annotated, List

from fastapi import APIRouter, Depends

from ...auth.services import OptionalAuthorizationDependency
from ...config import settings
from ...utils import RouteReturnSchema, apply_auto_filter
from ..schemas import Rfondo07tpDocument, Rfondo07tpFilter, Rfondo07tpParams
from ..services import Rfondo07tpServiceDependency

rfondo07tp_router = APIRouter(prefix="/rfondo07tp")


@rfondo07tp_router.post("/sync_from_siif", response_model=RouteReturnSchema)
async def sync_rfondo07tp_from_siif(
    auth: OptionalAuthorizationDependency,
    service: Rfondo07tpServiceDependency,
    params: Annotated[Rfondo07tpParams, Depends()],
    username: str = None,
    password: str = None,
):
    if auth.is_admin:
        username = settings.SIIF_USERNAME
        password = settings.SIIF_PASSWORD

    return await service.sync_rfondo07tp_from_siif(
        username=username, password=password, params=params
    )


@rfondo07tp_router.get("/get_from_db", response_model=List[Rfondo07tpDocument])
async def get_rfondo07tp_from_db(
    service: Rfondo07tpServiceDependency,
    params: Annotated[Rfondo07tpFilter, Depends()],
):
    apply_auto_filter(params=params)
    return await service.get_rfondo07tp_from_db(params=params)
