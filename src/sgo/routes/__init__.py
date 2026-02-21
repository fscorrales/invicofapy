__all__ = ["sgo_router"]

from fastapi import APIRouter

from .listado_obras import listado_obras_router

sgo_router = APIRouter(prefix="/sgo", tags=["SGO"])

sgo_router.include_router(listado_obras_router)
