__all__ = [
    "ControlAporteEmpresarioRepositoryDependency",
]

from typing import Annotated

from fastapi import Depends

from ...config import BaseRepository
from ..schemas.control_aporte_empresario import (
    ControlAporteEmpresarioReport,
)


# -------------------------------------------------
class ControlAporteEmpresarioRepository(BaseRepository[ControlAporteEmpresarioReport]):
    collection_name = "control_aporte_empresario"
    model = ControlAporteEmpresarioReport


ControlAporteEmpresarioRepositoryDependency = Annotated[
    ControlAporteEmpresarioRepository, Depends()
]
