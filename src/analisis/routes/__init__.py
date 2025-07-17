__all__ = ["control_router"]

from fastapi import APIRouter

from .control_icaro_vs_siif import control_icaro_vs_siif_router

control_router = APIRouter(prefix="/control", tags=["Control"])

control_router.include_router(control_icaro_vs_siif_router)
