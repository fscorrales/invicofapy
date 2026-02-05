__all__ = [
    "ControlDeudaFlotanteRepositoryDependency",
]

from typing import Annotated

from fastapi import Depends

from ...config import BaseRepository
from ..schemas.control_deuda_flotante import (
    ControlDeudaFlotanteReport,
)


# -------------------------------------------------
class ControlDeudaFlotanteRepository(BaseRepository[ControlDeudaFlotanteReport]):
    collection_name = "control_deuda_flotante"
    model = ControlDeudaFlotanteReport


ControlDeudaFlotanteRepositoryDependency = Annotated[
    ControlDeudaFlotanteRepository, Depends()
]
