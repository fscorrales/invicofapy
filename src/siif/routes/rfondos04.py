from typing import Annotated, List

from fastapi import APIRouter, Depends

from ...auth.services import OptionalAuthorizationDependency
from ...config import settings
from ...utils import RouteReturnSchema, apply_auto_filter
from ..schemas import Rfondos04Document, Rfondos04Filter, Rfondos04Params
from ..services import Rfondos04ServiceDependency

rfondos04_router = APIRouter(prefix="/rfondos04")


# -------------------------------------------------
@rfondos04_router.post("/sync_from_siif", response_model=List[RouteReturnSchema])
async def sync_rfondo07tp_from_siif(
    auth: OptionalAuthorizationDependency,
    service: Rfondos04ServiceDependency,
    params: Annotated[Rfondos04Params, Depends()],
    username: str = None,
    password: str = None,
):
    if auth.is_admin:
        username = settings.SIIF_USERNAME
        password = settings.SIIF_PASSWORD

    return await service.sync_rfondos04_from_siif(
        username=username, password=password, params=params
    )


# -------------------------------------------------
@rfondos04_router.get("/get_from_db", response_model=List[Rfondos04Document])
async def get_rfondos04_from_db(
    service: Rfondos04ServiceDependency,
    params: Annotated[Rfondos04Filter, Depends()],
):
    apply_auto_filter(params=params)
    return await service.get_rfondos04_from_db(params=params)


# -------------------------------------------------
@rfondos04_router.get(
    "/export",
    summary="Descarga los registros rfondo07tp como archivo .xlsx",
    response_description="Archivo Excel con los registros solicitados",
)
async def export_rfondos04_from_db(
    service: Rfondos04ServiceDependency, ejercicio: int = None
):
    return await service.export_rfondos04_from_db(ejercicio)
