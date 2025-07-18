__all__ = [
    "ReporteSIIFPresWithDescRepositoryDependency",
]

from typing import Annotated

from fastapi import Depends

from ...config import BaseRepository
from ..schemas.reporte_formulacion_presupuesto import ReporteSIIFPresWithDescReport


# -------------------------------------------------
class ReporteSIIFPresWithDescRepository(BaseRepository[ReporteSIIFPresWithDescReport]):
    collection_name = "reporte_form_pres_siif_with_desc"
    model = ReporteSIIFPresWithDescReport


ReporteSIIFPresWithDescRepositoryDependency = Annotated[
    ReporteSIIFPresWithDescRepository, Depends()
]
