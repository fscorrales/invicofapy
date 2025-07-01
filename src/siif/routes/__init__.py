__all__ = ["siif_router"]

from fastapi import APIRouter

from .rcg01_uejp import rcg01_uejp_router
from .rf602 import rf602_router
from .rf610 import rf610_router
from .rfondo07tp import rfondo07tp_router
from .rpa03g import rpa03g_router

siif_router = APIRouter(prefix="/siif", tags=["SIIF"])

siif_router.include_router(rf602_router)
siif_router.include_router(rf610_router)
siif_router.include_router(rcg01_uejp_router)
siif_router.include_router(rpa03g_router)
siif_router.include_router(rfondo07tp_router)
