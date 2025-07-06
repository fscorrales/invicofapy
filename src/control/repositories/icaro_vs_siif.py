__all__ = [
    "ControlAnualRepositoryDependency",
    "ControlComprobantesRepositoryDependency",
]

from typing import Annotated

from fastapi import Depends

from ...config import BaseRepository
from ..schemas.icaro_vs_siif import ControlAnualReport, ControlComprobantesReport


# -------------------------------------------------
class ControlAnualRepository(BaseRepository[ControlAnualReport]):
    collection_name = "icaro_vs_siif_control_anual"
    model = ControlAnualReport


ControlAnualRepositoryDependency = Annotated[ControlAnualRepository, Depends()]


# -------------------------------------------------
class ControlComprobantesRepository(BaseRepository[ControlComprobantesReport]):
    collection_name = "icaro_vs_siif_control_comprobantes"
    model = ControlComprobantesReport


ControlComprobantesRepositoryDependency = Annotated[
    ControlComprobantesRepository, Depends()
]
