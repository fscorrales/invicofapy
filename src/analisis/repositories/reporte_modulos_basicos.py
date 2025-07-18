__all__ = [
    "ReporteModulosBasicosIcaroRepositoryDependency",
]

from typing import Annotated

from fastapi import Depends

from ...config import BaseRepository
from ..schemas.reporte_modulos_basicos import ReporteModulosBasicosIcaroReport


# -------------------------------------------------
class ReporteModulosBasicosIcaroRepository(
    BaseRepository[ReporteModulosBasicosIcaroReport]
):
    collection_name = "reporte_modulos_basicos_icaro"
    model = ReporteModulosBasicosIcaroReport


ReporteModulosBasicosIcaroRepositoryDependency = Annotated[
    ReporteModulosBasicosIcaroRepository, Depends()
]
