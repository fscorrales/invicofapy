__all__ = ["siif_router"]

from fastapi import APIRouter

from .planillometro_hist import planillometro_hist_router
from .rcg01_uejp import rcg01_uejp_router
from .rci02 import rci02_router
from .rcocc31 import rcocc31_router
from .rdeu012 import rdeu012_router
from .rf602 import rf602_router
from .rf610 import rf610_router
from .rfondo07tp import rfondo07tp_router
from .rfp_p605b import rfp_p605b_router
from .ri102 import ri102_router
from .rpa03g import rpa03g_router
from .rvicon03 import rvicon03_router

siif_router = APIRouter(prefix="/siif", tags=["SIIF"])

siif_router.include_router(ri102_router)
siif_router.include_router(rci02_router)
siif_router.include_router(rf602_router)
siif_router.include_router(rf610_router)
siif_router.include_router(rcg01_uejp_router)
siif_router.include_router(rpa03g_router)
siif_router.include_router(rdeu012_router)
siif_router.include_router(rfondo07tp_router)
siif_router.include_router(rfp_p605b_router)
siif_router.include_router(rvicon03_router)
siif_router.include_router(rcocc31_router)
siif_router.include_router(planillometro_hist_router)
