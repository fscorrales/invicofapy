__all__ = ["ProyectoRepositoryDependency", "ProyectoRepository"]

from typing import Annotated

from fastapi import Depends

from ...config import BaseRepository
from ..schemas import ProyectoReport


class ProyectoRepository(BaseRepository[ProyectoReport]):
    collection_name = "icaro_proyectos"
    model = ProyectoReport


ProyectoRepositoryDependency = Annotated[ProyectoRepository, Depends()]
