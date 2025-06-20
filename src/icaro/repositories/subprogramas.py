__all__ = ["SubprogramaRepositoryDependency", "SubprogramaRepository"]

from typing import Annotated

from fastapi import Depends

from ...config import BaseRepository
from ..schemas import SubprogramaReport


class SubprogramaRepository(BaseRepository[SubprogramaReport]):
    collection_name = "icaro_subprogramas"
    model = SubprogramaReport


SubprogramaRepositoryDependency = Annotated[SubprogramaRepository, Depends()]
