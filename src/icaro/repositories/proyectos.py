__all__ = ["ProyectosRepositoryDependency", "ProyectosRepository"]

from typing import Annotated

from fastapi import Depends

from ...config import BaseRepository
from ..schemas import ProyectosReport


class ProyectosRepository(BaseRepository[ProyectosReport]):
    collection_name = "icaro_proyectos"
    model = ProyectosReport


ProyectosRepositoryDependency = Annotated[ProyectosRepository, Depends()]
