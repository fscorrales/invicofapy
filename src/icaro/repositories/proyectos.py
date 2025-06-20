__all__ = ["ProyectosRepositoryDependency", "ProyectosRepository"]

from typing import Annotated

from fastapi import Depends

from ...config import BaseRepository
from ..schemas import ProyectoReport


class ProyectosRepository(BaseRepository[ProyectoReport]):
    collection_name = "icaro_proyectos"
    model = ProyectoReport


ProyectosRepositoryDependency = Annotated[ProyectosRepository, Depends()]
