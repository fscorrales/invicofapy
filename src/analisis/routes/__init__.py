__all__ = ["control_router", "reporte_router"]

from fastapi import APIRouter

from .control_haberes import control_haberes_router
from .control_icaro_vs_siif import control_icaro_vs_siif_router
from .control_obras import control_obras_router
from .control_recursos import control_recursos_router
from .reporte_formulacion_presupuesto import reporte_formulacion_presupuesto_router
from .reporte_modulos_basicos import reporte_modulos_basicos_router

control_router = APIRouter(prefix="/control", tags=["Controles"])
control_router.include_router(control_recursos_router)
control_router.include_router(control_icaro_vs_siif_router)
control_router.include_router(control_obras_router)
control_router.include_router(control_haberes_router)

reporte_router = APIRouter(prefix="/reporte", tags=["Reportes"])
reporte_router.include_router(reporte_formulacion_presupuesto_router)
reporte_router.include_router(reporte_modulos_basicos_router)
