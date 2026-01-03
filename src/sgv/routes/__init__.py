__all__ = ["sgv_router"]

from fastapi import APIRouter

from .saldos_barrios_evolucion import saldos_barrios_evolucion_router

sgv_router = APIRouter(prefix="/sgv", tags=["SGV"])

sgv_router.include_router(saldos_barrios_evolucion_router)
