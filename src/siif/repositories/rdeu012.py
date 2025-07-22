__all__ = ["Rdeu012RepositoryDependency", "Rdeu012Repository"]

from typing import Annotated

from fastapi import Depends

from ...config import BaseRepository
from ..schemas import Rdeu012Report


class Rdeu012Repository(BaseRepository[Rdeu012Report]):
    collection_name = "siif_rdeu012"
    model = Rdeu012Report


Rdeu012RepositoryDependency = Annotated[Rdeu012Repository, Depends()]
