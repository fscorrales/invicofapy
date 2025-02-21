__all__ = ["siif_router"]

from fastapi import APIRouter

from .rf602 import rf602_router

# from .orders import orders_router

siif_router = APIRouter(prefix="/siif")

siif_router.include_router(rf602_router)
# siif_router.include_router(orders_router)
