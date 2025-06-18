__all__ = ["Rpa03gRepositoryDependency"]

from typing import Annotated

from fastapi import Depends

from ...config import BaseRepository
from ..schemas import Rpa03gReport


class Rpa03gRepository(BaseRepository[Rpa03gReport]):
    collection_name = "siif_rpa03g"
    model = Rpa03gReport


Rpa03gRepositoryDependency = Annotated[Rpa03gRepository, Depends()]
