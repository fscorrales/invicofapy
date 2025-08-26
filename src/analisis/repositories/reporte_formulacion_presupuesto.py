__all__ = [
    "ReporteFormulacionPresupuestoRepositoryDependency",
]

from typing import Annotated

from fastapi import Depends

from ...config import BaseRepository
from ..schemas.reporte_formulacion_presupuesto import ReporteFormulacionPresupuestoReport


# -------------------------------------------------
class ReporteFormulacionPresupuestoRepository(BaseRepository[ReporteFormulacionPresupuestoReport]):
    collection_name = "reporte_formulacion_presupuesto"
    model = ReporteFormulacionPresupuestoReport


ReporteFormulacionPresupuestoRepositoryDependency = Annotated[
    ReporteFormulacionPresupuestoRepository, Depends()
]
