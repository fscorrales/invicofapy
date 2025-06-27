__all__ = ["PartidasRepositoryDependency", "PartidasRepository"]

from typing import Annotated

from fastapi import Depends

from ...config import BaseRepository
from ..schemas import PartidasReport


class PartidasRepository(BaseRepository[PartidasReport]):
    collection_name = "icaro_partidas"
    model = PartidasReport


PartidasRepositoryDependency = Annotated[PartidasRepository, Depends()]
