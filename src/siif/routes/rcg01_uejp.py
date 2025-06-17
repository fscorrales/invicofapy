from typing import Annotated, List

from fastapi import APIRouter, Depends

from ...auth.services import OptionalAuthorizationDependency
from ...config import settings
from ...utils import RouteReturnSchema, apply_auto_filter
from ..schemas import Rcg01UejpDocument, Rcg01UejpFilter, Rcg01UejpParams
from ..services import Rcg01UejpServiceDependency

rcg01_uejp_router = APIRouter(prefix="/rcg01_uejp", tags=["SIIF - rcg01_uejp"])


@rcg01_uejp_router.post("/sync_from_siif", response_model=RouteReturnSchema)
async def sync_rcg01_uejp_from_siif(
    auth: OptionalAuthorizationDependency,
    service: Rcg01UejpServiceDependency,
    params: Annotated[Rcg01UejpParams, Depends()],
    username: str = None,
    password: str = None,
):
    if auth.is_admin:
        username = settings.SIIF_USERNAME
        password = settings.SIIF_PASSWORD

    return await service.sync_rcg01_uejp_from_siif(
        username=username, password=password, params=params
    )


@rcg01_uejp_router.get("/get_from_db", response_model=List[Rcg01UejpDocument])
async def get_rcg01_uejp_from_db(
    service: Rcg01UejpServiceDependency,
    params: Annotated[Rcg01UejpFilter, Depends()],
):
    apply_auto_filter(params=params)
    return await service.get_rcg01_uejp_from_db(params=params)
