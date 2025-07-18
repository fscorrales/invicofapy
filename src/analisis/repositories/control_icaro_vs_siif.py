__all__ = [
    "ControlAnualRepositoryDependency",
    "ControlComprobantesRepositoryDependency",
    "ControlPa6RepositoryDependency",
]

from typing import Annotated

from fastapi import Depends

from ...config import BaseRepository
from ..schemas.control_icaro_vs_siif import (
    ControlAnualReport,
    ControlComprobantesReport,
    ControlPa6Report,
)


# -------------------------------------------------
class ControlAnualRepository(BaseRepository[ControlAnualReport]):
    collection_name = "control_icaro_vs_siif_anual"
    model = ControlAnualReport


ControlAnualRepositoryDependency = Annotated[ControlAnualRepository, Depends()]


# -------------------------------------------------
class ControlComprobantesRepository(BaseRepository[ControlComprobantesReport]):
    collection_name = "control_icaro_vs_siif_comprobantes"
    model = ControlComprobantesReport


ControlComprobantesRepositoryDependency = Annotated[
    ControlComprobantesRepository, Depends()
]


# -------------------------------------------------
class ControlPa6Repository(BaseRepository[ControlPa6Report]):
    collection_name = "control_icaro_vs_siif_pa6"
    model = ControlPa6Report


ControlPa6RepositoryDependency = Annotated[ControlPa6Repository, Depends()]
