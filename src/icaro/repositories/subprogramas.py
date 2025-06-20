__all__ = ["SubprogramasRepositoryDependency", "SubprogramasRepository"]

from typing import Annotated

from fastapi import Depends

from ...config import BaseRepository
from ..schemas import SubprogramaReport


class SubprogramasRepository(BaseRepository[SubprogramaReport]):
    collection_name = "icaro_subprogramas"
    model = SubprogramaReport


SubprogramasRepositoryDependency = Annotated[SubprogramasRepository, Depends()]
