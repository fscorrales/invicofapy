from typing import Annotated, List

from fastapi import APIRouter, Depends

from ...auth.services import OptionalAuthorizationDependency
from ...config import settings
from ...utils import RouteReturnSchema, apply_auto_filter
from ..schemas import (
    BancoINVICODocument,
    BancoINVICOFilter,
    BancoINVICOParams,
)
from ..services import BancoINVICOServiceDependency

banco_invico_router = APIRouter(prefix="/banco_invico", tags=["SSCC - Banco INVICO"])


@banco_invico_router.post("/sync_from_sscc", response_model=RouteReturnSchema)
async def sync_banco_invico_from_sscc(
    auth: OptionalAuthorizationDependency,
    service: BancoINVICOServiceDependency,
    params: Annotated[BancoINVICOParams, Depends()],
    username: str = None,
    password: str = None,
):
    if auth.is_admin:
        username = settings.SSCC_USERNAME
        password = settings.SSCC_PASSWORD

    return await service.sync_banco_invico_from_sscc(
        username=username, password=password, params=params
    )


@banco_invico_router.get("/get_from_db", response_model=List[BancoINVICODocument])
async def get_banco_invico_from_db(
    service: BancoINVICOServiceDependency,
    params: Annotated[BancoINVICOFilter, Depends()],
):
    apply_auto_filter(params=params)
    return await service.get_banco_invico_from_db(params=params)
