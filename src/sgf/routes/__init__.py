__all__ = ["sgf_router"]

from fastapi import APIRouter

from .resumen_rend_prov import resumen_rend_prov_router

sgf_router = APIRouter(prefix="/sgf")

sgf_router.include_router(resumen_rend_prov_router)
