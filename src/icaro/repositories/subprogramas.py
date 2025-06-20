__all__ = ["SubprogramasRepositoryDependency", "SubprogramasRepository"]

from typing import Annotated

from fastapi import Depends

from ...config import BaseRepository
from ..schemas import SubprogramasReport


class SubprogramasRepository(BaseRepository[SubprogramasReport]):
    collection_name = "icaro_subprogramas"
    model = SubprogramasReport


SubprogramasRepositoryDependency = Annotated[SubprogramasRepository, Depends()]
