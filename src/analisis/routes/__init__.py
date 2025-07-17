__all__ = ["control_router"]

from fastapi import APIRouter

from .icaro_vs_siif import icaro_vs_siif_router

control_router = APIRouter(prefix="/control", tags=["Control"])

control_router.include_router(icaro_vs_siif_router)