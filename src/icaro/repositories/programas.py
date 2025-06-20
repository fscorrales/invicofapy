__all__ = ["ProgramasRepositoryDependency", "ProgramasRepository"]

from typing import Annotated

from fastapi import Depends

from ...config import BaseRepository
from ..schemas import ProgramaReport


class ProgramasRepository(BaseRepository[ProgramaReport]):
    collection_name = "icaro_programas"
    model = ProgramaReport


ProgramasRepositoryDependency = Annotated[ProgramasRepository, Depends()]
