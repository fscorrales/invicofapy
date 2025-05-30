__all__ = ["Rf602RepositoryDependency"]

from typing import Annotated

from fastapi import Depends

from ...config import BaseRepository
from ..schemas import Rf602Report


class Rf602Repository(BaseRepository[Rf602Report]):
    collection_name = "siif_rf602"
    model = Rf602Report


Rf602RepositoryDependency = Annotated[Rf602Repository, Depends()]
