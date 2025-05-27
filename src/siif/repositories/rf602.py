__all__ = ["TitulosFCIsRepositoryDependency"]

from typing import Annotated

from fastapi import Depends

from ...config import BaseRepository
from ..schemas import Rf602


class Rf602Repository(BaseRepository[Rf602]):
    collection_name = "siif_rf602"
    model = Rf602


Rf602RepositoryDependency = Annotated[Rf602Repository, Depends()]