__all__ = ["ProgramasRepositoryDependency", "ProgramasRepository"]

from typing import Annotated

from fastapi import Depends

from ...config import BaseRepository
from ..schemas import ProgramasReport


class ProgramasRepository(BaseRepository[ProgramasReport]):
    collection_name = "icaro_programas"
    model = ProgramasReport


ProgramasRepositoryDependency = Annotated[ProgramasRepository, Depends()]
