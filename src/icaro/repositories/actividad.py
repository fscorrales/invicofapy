__all__ = ["ActividadRepositoryDependency", "ActividadRepository"]

from typing import Annotated

from fastapi import Depends

from ...config import BaseRepository
from ..schemas import ActividadReport


class ActividadRepository(BaseRepository[ActividadReport]):
    collection_name = "icaro_actividad"
    model = ActividadReport


ActividadRepositoryDependency = Annotated[ActividadRepository, Depends()]
