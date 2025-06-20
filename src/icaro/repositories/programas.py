__all__ = ["ProgramaRepositoryDependency", "ProgramaRepository"]

from typing import Annotated

from fastapi import Depends

from ...config import BaseRepository
from ..schemas import ProgramaReport


class ProgramaRepository(BaseRepository[ProgramaReport]):
    collection_name = "icaro_programas"
    model = ProgramaReport


ProgramaRepositoryDependency = Annotated[ProgramaRepository, Depends()]
