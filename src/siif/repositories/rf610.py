__all__ = ["Rf610RepositoryDependency", "Rf610Repository"]

from typing import Annotated

from fastapi import Depends

from ...config import BaseRepository
from ..schemas import Rf610Report


class Rf610Repository(BaseRepository[Rf610Report]):
    collection_name = "siif_rf610"
    model = Rf610Report


Rf610RepositoryDependency = Annotated[Rf610Repository, Depends()]
